#pragma warning (disable:4996)

#include "IOCP_server.h"
#include <iostream>

IOCP_server::IOCP_server(int bufSize, std::function<void(SOCKET)> connectCall, std::function<void(SOCKET)> disconnectCall)
{
	m_open = false;

	m_hIOCP = NULL;
	m_lpfnAcceptEx = NULL;
	m_listenSock = make_unique<SOCKET_INFO>();

	this->m_connectCall = connectCall;
	this->m_disconnectCall = disconnectCall;

	WSADATA	wsaData;
	if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
	{
		OutputDebugStringA("WSA init fail\n");
	}
}

IOCP_server::~IOCP_server()
{
	CloseServerSocket();
}

bool IOCP_server::OpenServer(int port)
{
	if (m_open)
	{
		OutputDebugStringA("server is already open\n");
		return false;
	}
	else
	{
		m_listenSock->socket = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);

		if (m_listenSock->socket == (SOCKET)SOCKET_ERROR)
		{
			return false;
		}
		else
		{
			m_listenSock->socketAddr.sin_family = AF_INET;
			m_listenSock->socketAddr.sin_port = htons(port);
			m_listenSock->socketAddr.sin_addr.s_addr = htonl(INADDR_ANY);

			if (::bind(m_listenSock->socket, (SOCKADDR*)&(m_listenSock->socketAddr), sizeof(m_listenSock->socketAddr)) == SOCKET_ERROR)
			{
				CloseServerSocket();
				return false;
			}
			else
			{
				// 1. 수신 시작
				if (listen(m_listenSock->socket, SOMAXCONN) == SOCKET_ERROR)
				{
					return false;
				}

				// 2. AcceptEx 함수 포인터 얻기
				GUID guidAcceptEx = WSAID_ACCEPTEX;
				DWORD bytes = 0;
				WSAIoctl(m_listenSock->socket, SIO_GET_EXTENSION_FUNCTION_POINTER,
					&guidAcceptEx, sizeof(guidAcceptEx),
					&m_lpfnAcceptEx, sizeof(m_lpfnAcceptEx),		// BOOL AccpetEx 함수 포인터 받아옴
					&bytes, NULL, NULL);

				// 3. IOCP 생성
				m_hIOCP = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);

				// 4. listen socket IOCP 연결
				// Completion key 0 설정 (== 사용안함)
				CreateIoCompletionPort((HANDLE)m_listenSock->socket, m_hIOCP, 0, 0);

				// 5. IOCP 이벤트를 처리할 Worker 스레드 생성
				for (int i = 0; i < IO_WORKER_COUNT; i++)
				{
					if (!m_worker[i].joinable())
					{
						m_open = true;
						m_worker[i] = std::jthread([this](std::stop_token token) { AccpetProc(token); });
					}
					else
					{
						auto text = format("{} worker creat fail..\n", i);
						OutputDebugStringA(text.c_str());
					}
				}

				// 6. AcceptEx 등록
				for (int i = 0; i < ACCEPT_POST_COUNT; i++)
					PostAccept();

				return true;
			}
		}
	}
}

void IOCP_server::PostAccept()
{
	// AcceptEx에서 비동기 연결 요청만 등록
	// 완료통지에서 연결된 소켓의 초기화 작업을 수행한다.
	ACCEPT_CONTEXT* accCtx = new ACCEPT_CONTEXT;
	ZeroMemory(&accCtx->overlapped, sizeof(OVERLAPPED));

	accCtx->type = IO_ACCEPT;
	accCtx->acceptSocket = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);

	DWORD recvBytes = 0;

	// ReceiveDataLength = 0:
	// 연결 데이터는 받지 않고 연결 완료만 비동기로 통지
	bool ret;
	ret = m_lpfnAcceptEx(m_listenSock->socket, accCtx->acceptSocket,
		accCtx->addrBuf, 0,
		sizeof(sockaddr_in) + 16,
		sizeof(sockaddr_in) + 16,
		&recvBytes,
		&accCtx->overlapped);

	if (!ret)
	{
		int err = WSAGetLastError();
		if (err != WSA_IO_PENDING)
		{
			printf("AcceptEx failed: %d\n", err);
		}
	}
}

void IOCP_server::AccpetProc(std::stop_token token)
{
	DWORD bytes;
	SOCKET_CONTEXT* sockCtx;
	BASE_IO_CONTEXT* ctx;

	DataPacket packet;

	while (!token.stop_requested())
	{
		// OVERLAPPED 기반 완료된 I/O 작업 구분
		BOOL result = GetQueuedCompletionStatus(
			m_hIOCP,
			&bytes,
			(PULONG_PTR)&sockCtx,
			(OVERLAPPED**)&ctx,
			INFINITE);

		// nullptr OVERLAPPED는 서버 종료용 완료 통지
		if (ctx == nullptr)
			break;

		// 실제 I/O 실패
		if (!result)
		{
			DWORD error = GetLastError();

			uint64_t socketKey = sockCtx->socket;

			if (m_sessionManager.find(socketKey) != m_sessionManager.end())
			{
				printf("[Disconnect] client %lld, error: %lu\n", socketKey, error);
				m_sessionManager[socketKey]->Disconnect();
			}

			if (socketKey != INVALID_SOCKET)
				closesocket(socketKey);

			continue;
		}

		switch (ctx->type)
		{
		case IO_ACCEPT:
		{
			// AcceptEx 전용 컨텍스트.
			// OVERLAPPED를 첫 멤버로 두어 완료 이벤트에서 ACCEPT_CONTEXT로 복원
			auto* accCtx = (ACCEPT_CONTEXT*)ctx;      // 연결을 받기 위한 임시 소켓
			SOCKET_INFO* acceptSock = new SOCKET_INFO;
			acceptSock->socket = accCtx->acceptSocket;

			// accept context 업데이트
			// AcceptEx로 생성된 소켓을 listen socket의 accept context와 연결
			// 이후 getpeername, SO_KEEPALIVE 등의 소켓 옵션 사용
			setsockopt(acceptSock->socket, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, (char*)&(m_listenSock->socket), sizeof(m_listenSock->socket));

			int len = sizeof(acceptSock->socketAddr);
			getpeername(acceptSock->socket, (SOCKADDR*)&acceptSock->socketAddr, &len);

			// 접속 client socket IOCP 등록
			// 이후 클라이언트 I/O 완료 통지를 IOCP에서 처리
			// CompletionKey에는 연결별 SOCKET_INFO 전달
			CreateIoCompletionPort((HANDLE)acceptSock->socket, m_hIOCP, (ULONG_PTR)acceptSock, 0);

			// 걸어둔 AcceptEx는 1회성. 새로운 연결을 받기 위해 다시 호출
			PostAccept();

			// Session에서 발생한 패킷/연결 종료 이벤트를 서버로 전달
			unique_ptr<Session> client = make_unique<Session>([this](DataPacket* data) { InsertPacket(data); }, [this](SOCKET_INFO& sockInfo) { ConnectLost(sockInfo); });
			// 완료통지에서 할당한 소켓 주소 key 삽입
			client->session.socket = accCtx->acceptSocket;
			client->session.socketAddr = acceptSock->socketAddr;
			client->ioCtx.type = IO_RECV;

			DWORD flags = 0;
			DWORD recvBytes = 0;

			if (client->PostRecv())
			{
				uint64_t socketKey = accCtx->acceptSocket;

				// 연결완료 통지
				if (m_connectCall)
					m_connectCall(socketKey);

				// 클라이언트 연결 완료. 초기 연결 정보 전송
				ZeroMemory(&packet, sizeof(DataPacket));
				packet.header.id = socketKey;
				packet.header.len = 4;

				client->OnSend(&packet, sizeof(DataPacket));
				printf("[Accept] new client: %s(%lld)\n", inet_ntoa(acceptSock->socketAddr.sin_addr), socketKey);

				// 연결된 클라이언트를 관리 컨테이너에 등록
				m_sessionManager.insert(make_pair(socketKey, move(client)));
			}
			else
			{
				delete ctx;
			}
			//

			// accept 완료 후 더 이상 쓰이지 않음 -> 삭제
			delete accCtx;
		}
		break;
		case IO_RECV:
		{
			// 정상적인 연결 종료
			if (bytes == 0)
			{
				std::cout << "[Disconnect]" << std::endl;
				m_sessionManager.erase(sockCtx->socket);
				continue;
			}

			// 등록된 컨테이너에서 socket 값으로 Session 식별 후 수신 버퍼로 전달
			auto iter = m_sessionManager.find(sockCtx->socket);
			if (iter != m_sessionManager.end())
			{
				iter->second->RecvData((int8_t*)(((IO_CONTEXT*)ctx)->data), bytes);
			}
		}
		break;
		case IO_SEND:
		{
			auto* ioCtx = static_cast<SEND_IO_CONTEXT*>(ctx);
			auto iter = m_sessionManager.find(sockCtx->socket);
			if (iter != m_sessionManager.end())
			{
				// Send 완료 처리 Session 전달
				iter->second->SendComplete(*ioCtx);
			}
		}
		break;

		default:
			break;
		}
	}
}

void IOCP_server::CloseServerSocket()
{
	m_open = false;
	m_listenSock->CloseSocket();

	// INFINITE 대기 중인 Worker를 꺠움
	for (int i = 0; i < IO_WORKER_COUNT; i++)
	{
		PostQueuedCompletionStatus(m_hIOCP, 0, 0, nullptr);
	}

	for (int i = 0; i < IO_WORKER_COUNT; i++)
	{
		if (m_worker[i].joinable())
		{
			m_worker[i].request_stop();
			m_worker[i].join();
		}
	}
}

void IOCP_server::ConnectLost(SOCKET_INFO& sockInfo)
{
	m_sessionManager.erase(sockInfo.socket);

	if (m_disconnectCall)
		m_disconnectCall(sockInfo.socket);
}

void IOCP_server::InsertPacket(DataPacket* packet)
{
	std::lock_guard<std::mutex> lock(m_dataMtx);
	m_batch.push(packet);
	printf("Make Packet Cap: %lld\n", m_batch.size());
}

int IOCP_server::GetRecvData(std::queue<DataPacket*>* container)
{
	std::lock_guard<std::mutex> lock(m_dataMtx);

	int swapSize = m_batch.size();
	container->swap(m_batch);

	return swapSize;
}


