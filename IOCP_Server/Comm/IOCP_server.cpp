#pragma warning (disable:4996)

#include "IOCP_server.h"
#include <chrono>
#include <cstdio>

IOCP_server::IOCP_server(SessionCallback connectCall, SessionCallback disconnectCall)
{
	m_open = false;

	m_hIOCP = NULL;
	m_lpfnAcceptEx = NULL;
	m_listenSock = std::make_unique<SOCKET_INFO>();

	m_connectCall = std::move(connectCall);
	m_disconnectCall = std::move(disconnectCall);

	WSADATA	wsaData;
	if (WSAStartup(MAKEWORD(2, 2), &wsaData) == 0)
		m_wsaStarted = true;
	else
		OutputDebugStringA("WSA init fail\n");
}

IOCP_server::~IOCP_server()
{
	CloseServerSocket();

	if (m_wsaStarted)
		WSACleanup();
}

bool IOCP_server::OpenServer(int port)
{
	if (m_open)
	{
		OutputDebugStringA("server is already open\n");
		return false;
	}

	if (!m_wsaStarted)
		return false;

	// 1. listen 소켓 생성
	m_listenSock->socket = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);
	if (m_listenSock->socket == INVALID_SOCKET)
		return false;

	m_listenSock->socketAddr.sin_family = AF_INET;
	m_listenSock->socketAddr.sin_port = htons(port);
	m_listenSock->socketAddr.sin_addr.s_addr = htonl(INADDR_ANY);

	if (::bind(m_listenSock->socket, (SOCKADDR*)&(m_listenSock->socketAddr), sizeof(m_listenSock->socketAddr)) == SOCKET_ERROR)
	{
		CloseServerSocket();
		return false;
	}

	// 2. 수신 시작
	if (listen(m_listenSock->socket, SOMAXCONN) == SOCKET_ERROR)
	{
		CloseServerSocket();
		return false;
	}

	// 3. AcceptEx 함수 포인터 얻기
	GUID guidAcceptEx = WSAID_ACCEPTEX;
	DWORD bytes = 0;
	if (WSAIoctl(m_listenSock->socket, SIO_GET_EXTENSION_FUNCTION_POINTER,
		&guidAcceptEx, sizeof(guidAcceptEx),
		&m_lpfnAcceptEx, sizeof(m_lpfnAcceptEx),		// BOOL AccpetEx 함수 포인터 받아옴
		&bytes, NULL, NULL) == SOCKET_ERROR || m_lpfnAcceptEx == NULL)
	{
		CloseServerSocket();
		return false;
	}

	// 4. IOCP 생성
	m_hIOCP = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
	if (m_hIOCP == NULL)
	{
		CloseServerSocket();
		return false;
	}

	// 5. listen socket IOCP 연결
	if (CreateIoCompletionPort((HANDLE)m_listenSock->socket, m_hIOCP, 0, 0) == NULL)
	{
		CloseServerSocket();
		return false;
	}

	// 6. IOCP 이벤트를 처리할 Worker 스레드 생성
	m_open = true;
	for (int i = 0; i < IO_WORKER_COUNT; i++)
		m_worker[i] = std::jthread([this](std::stop_token token) { WorkProc(token); });

	// 7. AcceptEx 등록
	for (int i = 0; i < ACCEPT_POST_COUNT; i++)
		PostAccept();

	return true;
}

bool IOCP_server::PostAccept()
{
	// AcceptEx에서 비동기 연결 요청만 등록
	// 완료통지에서 연결된 소켓의 초기화 작업을 수행한다.
	auto accCtx = std::make_unique<ACCEPT_CONTEXT>();
	accCtx->type = IO_ACCEPT;
	accCtx->socket = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, nullptr, 0, WSA_FLAG_OVERLAPPED);

	if (accCtx->socket == INVALID_SOCKET)
	{
		printf("accept socket create failed: %d\n", WSAGetLastError());
		return false;
	}

	DWORD recvBytes = 0;

	// 연결 데이터는 받지 않고 연결 완료만 비동기로 통지
	BOOL rst = m_lpfnAcceptEx(m_listenSock->socket, accCtx->socket,
		accCtx->addrBuf, 0,
		sizeof(sockaddr_in) + 16,
		sizeof(sockaddr_in) + 16,
		&recvBytes,
		&accCtx->overlapped);

	if (!rst)
	{
		int err = WSAGetLastError();
		if (err != WSA_IO_PENDING)
		{
			printf("AcceptEx failed: %d\n", err);
			closesocket(accCtx->socket);
			return false;
		}
	}

	// 완료 통지(OnAccept)에서 추적 및 관리하므로 소유권 포기
	accCtx.release();
	return true;
}

void IOCP_server::WorkProc(std::stop_token token)
{
	while (!token.stop_requested())
	{
		DWORD bytes;
		SOCKET socketKey = 0;
		BASE_IO_CONTEXT* ctx = nullptr;

		// OVERLAPPED 기반 완료된 I/O 작업 구분
		BOOL result = GetQueuedCompletionStatus(m_hIOCP, &bytes, &socketKey, (OVERLAPPED**)&ctx, INFINITE);

		// OVERLAPPED가 nullptr인 경우는 서버 종료용 완료 통지
		if (ctx == nullptr)
			break;

		switch (ctx->type)
		{
		case IO_ACCEPT:
		{
			OnAccept(static_cast<ACCEPT_CONTEXT*>(ctx), result);
		}
		break;

		case IO_RECV:
		{
			auto ioCtx = static_cast<IO_CONTEXT*>(ctx);
			// 컨텍스트가 쥐고 있던 참조를 회수. 이 블록이 끝나면 참조 반납 (맵 조회/전역 락 불필요)
			std::shared_ptr<Session> session = std::move(ioCtx->owner);
			if (!session)
				break;

			if (!result || bytes == 0)
				session->Close();				// 오류 또는 상대의 정상 종료
			else
				session->RecvComplete(bytes);
		}
		break;
		case IO_SEND:
		{
			auto* sendCtx = static_cast<SEND_IO_CONTEXT*>(ctx);
			std::shared_ptr<Session> session = std::move(sendCtx->owner);
			if (!session)
				break;

			session->SendComplete(*sendCtx, result);
		}
		break;

		default:
			break;
		}
	}
}

void IOCP_server::OnAccept(ACCEPT_CONTEXT* rawCtx, bool ok)
{
	// AcceptEx 전용 컨텍스트.
	// OVERLAPPED를 첫 멤버로 두어 완료 이벤트에서 ACCEPT_CONTEXT로 복원
	// PostAccept에서 포기한 소유권 획득 -> 이곳에서 소멸
	std::unique_ptr<ACCEPT_CONTEXT> accCtx(rawCtx);
	SOCKET accSocket = accCtx->socket;

	// 실패한 accept(서버 종료로 listen 소켓이 닫힌 경우 포함) 또는 종료 중
	if (!ok || !m_open)
	{
		closesocket(accSocket);
		if (m_open)
			PostAccept();		// 서버가 열려 있는데 실패한 경우에만 다음 접속 대비
		return;
	}

	// accept context 업데이트
	// AcceptEx로 생성된 소켓을 listen socket의 accept context와 연결
	// 이후 getpeername, SO_KEEPALIVE 등의 소켓 옵션 사용
	setsockopt(accSocket, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, (char*)&(m_listenSock->socket), sizeof(m_listenSock->socket));

	char optval = 1;
	setsockopt(accSocket, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval));		// keep alive 설정

	tcp_keepalive keepAlive;
	keepAlive.onoff = 1;
	keepAlive.keepalivetime = 5000;      // 5 sec
	keepAlive.keepaliveinterval = 1000;   // 1 sec

	DWORD bytesReturned;
	if (WSAIoctl(accSocket, SIO_KEEPALIVE_VALS, &keepAlive, sizeof(keepAlive), NULL, 0, &bytesReturned, NULL, NULL) == SOCKET_ERROR)
	{
		printf("client socket set SIO_KEEPALIVE_VALS failed: %d\n", WSAGetLastError());
		closesocket(accSocket);
		PostAccept();
		return;
	}

	SOCKADDR_IN socketAddr;
	int len = sizeof(socketAddr);
	getpeername(accSocket, (SOCKADDR*)&socketAddr, &len);

	// 걸어둔 AcceptEx는 1회성. 새로운 연결을 받기 위해 다시 호출
	PostAccept();

	// Session에서 발생한 패킷/연결 종료 이벤트를 서버로 전달
	auto session = std::make_shared<Session>([this](DataPacket* data) { InsertPacket(data); }, [this](SOCKET socket) { CloseSession(socket); });
	// 완료통지에서 할당한 소켓 주소 key 삽입
	session->SetSessionInfo(accSocket, socketAddr);

	// 연결된 클라이언트를 관리 컨테이너에 등록 및 연결완료 통지
	if (m_connectCall)
		m_connectCall(accSocket);

	{
		std::lock_guard lock(m_sessionMtx);
		m_sessionManager.emplace(accSocket, session);
	}

	// 서버 종료와의 경합: CloseServerSocket 의 스냅샷 이후에 등록된 세션은 스스로 닫는다
	if (!m_open)
	{
		session->Close();
		return;
	}

	// 접속 client socket IOCP 등록 (CompletionKey = SOCKET id)
	if (CreateIoCompletionPort((HANDLE)accSocket, m_hIOCP, accSocket, 0) == NULL)
	{
		printf("[Accept] CreateIoCompletionPort failed: %d\n", WSAGetLastError());
		session->Close();
		return;
	}

	if (session->PostRecv())
	{
		DataPacket packet{};

		// 클라이언트 연결 완료. 초기 연결 정보 전송
		printf("[Accept] new client: %s(%lld)\n", inet_ntoa(socketAddr.sin_addr), accSocket);
		packet.header.id = accSocket;
		packet.header.len = sizeof(DataPacket);
		session->OnSend(&packet, sizeof(DataPacket));
	}
}

void IOCP_server::CloseSession(uint64_t id)
{
	RemoveSession(id);

	printf("[Disconnect] session %llu\n", (unsigned long long)id);

	if (m_disconnectCall)
		m_disconnectCall(id);
}

void IOCP_server::CloseServerSocket()
{
	m_open = false;
	m_listenSock->CloseSocket();

	if (m_hIOCP != NULL)
	{
		// 연결된 session 정리
		std::vector<std::shared_ptr<Session>> snapshot;
		snapshot.reserve(m_sessionManager.size());
		for (auto& kv : m_sessionManager)
			snapshot.push_back(kv.second);
		for (auto& s : snapshot)
			s->Close();

		snapshot.clear();

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

		CloseHandle(m_hIOCP);
		m_hIOCP = NULL;
	}

	m_lpfnAcceptEx = NULL;
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

std::shared_ptr<Session> IOCP_server::FindSession(uint64_t id)
{
	std::lock_guard lock(m_sessionMtx);
	auto it = m_sessionManager.find(id);
	return it == m_sessionManager.end() ? nullptr : it->second;
}

std::shared_ptr<Session> IOCP_server::RemoveSession(uint64_t id)
{
	std::lock_guard lock(m_sessionMtx);

	auto it = m_sessionManager.find(id);
	if (it == m_sessionManager.end())
		return nullptr;

	auto sp = std::move(it->second);
	m_sessionManager.erase(it);

	return sp;      // 호출자가 놓으면 소멸
}


