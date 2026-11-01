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
		&m_lpfnAcceptEx, sizeof(m_lpfnAcceptEx),		// BOOL AcceptEx 함수 포인터 받아옴
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

	// 7. AcceptEx 등록 (하나라도 실패하면 서버를 열지 않는다)
	for (int i = 0; i < ACCEPT_POST_COUNT; i++)
	{
		if (!PostAccept())
		{
			printf("[Server] initial AcceptEx post failed\n");
			CloseServerSocket();
			return false;
		}
	}

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

// accept 완료 후 다음 접속을 받기 위한 재등록. 일시적 실패로 accept 슬롯이 제거되는 것을 방지
void IOCP_server::RepostAccept()
{
	for (int retry = 0; retry < ACCEPT_REPOST_RETRY; retry++)
	{
		if (!m_open)		// 서버 종료 감지
			return;

		if (PostAccept())
			return;

		Sleep(ACCEPT_REPOST_DELAY_MS);
	}

	printf("[Accept] re-post failed. accept slot lost\n");
}

void IOCP_server::WorkProc(std::stop_token token)
{
	while (true)
	{
		DWORD bytes = 0;
		SOCKET socketKey = 0;
		BASE_IO_CONTEXT* ctx = nullptr;

		// OVERLAPPED 기반 완료된 I/O 작업 구분
		BOOL result = GetQueuedCompletionStatus(m_hIOCP, &bytes, &socketKey, (OVERLAPPED**)&ctx, WORKER_POLL_MS);

		// ctx == nullptr: WORKER_POLL_MS 동안 통지가 없었음(타임아웃) 또는 IOCP 핸들 오류
		if (ctx == nullptr)
		{
			if (token.stop_requested() || (!result && GetLastError() != WAIT_TIMEOUT))
				break;	// 종료 요청 후 조용해졌거나 복구 불가 오류
			continue;
		}

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
	// PostAccept에서 포기한 소유권 획득 -> 이곳에서 소멸
	std::unique_ptr<ACCEPT_CONTEXT> accCtx(rawCtx);
	SOCKET accSocket = accCtx->socket;

	// 실패한 accept(서버 종료로 listen 소켓이 닫힌 경우 포함) 또는 종료 중
	if (!ok || !m_open)
	{
		closesocket(accSocket);
		RepostAccept();		// 종료 중이면 내부에서 무시된다
		return;
	}

	// 소켓 옵션 설정 + 접속자 주소 조회. 실패하면 이 연결은 버리고 다음 접속을 받는다
	SOCKADDR_IN peerAddr{};
	if (!ConfigureAcceptedSocket(accSocket, peerAddr))
	{
		closesocket(accSocket);
		RepostAccept();
		return;
	}

	// 걸어둔 AcceptEx는 1회성. 새로운 연결을 받기 위해 다시 호출
	RepostAccept();

	// 세션 생성 → 관리 컨테이너 등록 → IOCP 연결
	std::shared_ptr<Session> session = CreateSession(accSocket, peerAddr);
	if (!session)
		return;

	// 첫 recv 등록. 실패하면 Start() 안에서 Close() 되어 정상적으로 정리된다.
	if (!session->Start())
		return;

	// 클라이언트 연결 완료. 초기 연결 정보 전송
	printf("[Accept] new client: %s(%llu)\n", inet_ntoa(peerAddr.sin_addr), accSocket);

	DataPacket packet{};
	packet.header.id = accSocket;
	packet.header.len = sizeof(DataPacket);
	session->OnSend(&packet, sizeof(DataPacket));
}

// accept 직후 소켓 설정. 하나라도 실패하면 false (호출한 쪽에서 소켓을 닫는다)
bool IOCP_server::ConfigureAcceptedSocket(SOCKET accSocket, SOCKADDR_IN& peerAddr)
{
	// accept context 업데이트
	// AcceptEx로 생성된 소켓을 listen socket의 accept context와 연결
	// 이후 getpeername, SO_KEEPALIVE 등의 소켓 옵션 사용
	if (setsockopt(accSocket, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT, (char*)&(m_listenSock->socket), sizeof(m_listenSock->socket)) == SOCKET_ERROR)
	{
		printf("[Accept] SO_UPDATE_ACCEPT_CONTEXT failed: %d\n", WSAGetLastError());
		return false;
	}

	char optval = 1;
	if (setsockopt(accSocket, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval)) == SOCKET_ERROR)		// keep alive 설정
	{
		printf("[Accept] SO_KEEPALIVE failed: %d\n", WSAGetLastError());
		return false;
	}

	tcp_keepalive keepAlive{};
	keepAlive.onoff = 1;
	keepAlive.keepalivetime = KEEPALIVE_TIME_MS;
	keepAlive.keepaliveinterval = KEEPALIVE_INTERVAL_MS;

	DWORD bytesReturned = 0;
	if (WSAIoctl(accSocket, SIO_KEEPALIVE_VALS, &keepAlive, sizeof(keepAlive), NULL, 0, &bytesReturned, NULL, NULL) == SOCKET_ERROR)
	{
		printf("client socket set SIO_KEEPALIVE_VALS failed: %d\n", WSAGetLastError());
		return false;
	}

	int len = sizeof(peerAddr);
	if (getpeername(accSocket, (SOCKADDR*)&peerAddr, &len) == SOCKET_ERROR)
	{
		printf("[Accept] getpeername failed: %d\n", WSAGetLastError());
		return false;
	}

	return true;
}

// Session 을 만들어 관리 컨테이너에 등록하고 IOCP 연결
std::shared_ptr<Session> IOCP_server::CreateSession(SOCKET accSocket, const SOCKADDR_IN& peerAddr)
{
	// Session 에서 발생한 패킷/종료 이벤트를 서버로 전달
	auto session = std::make_shared<Session>(accSocket, peerAddr, [this](DataPacket* data) { InsertPacket(data); }, [this](SessionId id) { CloseSession(id); });

	// [순서 중요] connect 통지 → 맵 등록.
	// 맵에 등록되는 순간부터 다른 스레드(서버 종료 등)가 이 세션을 Close() 할 수 있고, Close() 는 disconnect 통지를 보낸다.
	// 등록 뒤에 connect 통지를 하면 disconnect 통지가 connect 통지보다 먼저 나갈 수 있다.
	if (m_connectCall)
		m_connectCall(accSocket);

	{
		std::lock_guard<std::mutex> lock(m_sessionMtx);
		m_sessionManager.emplace(accSocket, session);
	}

	// 서버 종료 후 연결된 세션 닫기
	if (!m_open)
	{
		session->Close();
		return nullptr;
	}

	// 접속 client socket IOCP 등록 (CompletionKey = SOCKET id)
	if (CreateIoCompletionPort((HANDLE)accSocket, m_hIOCP, accSocket, 0) == NULL)
	{
		printf("[Accept] CreateIoCompletionPort failed: %d\n", WSAGetLastError());
		session->Close();
		return nullptr;
	}

	return session;
}

void IOCP_server::CloseSession(SessionId id)
{
	RemoveSession(id);

	printf("[Disconnect] session %llu\n", id);

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
		{
			std::lock_guard<std::mutex> lock(m_sessionMtx);	// 워커가 맵을 수정 중일 수 있으므로 락 필요
			snapshot.reserve(m_sessionManager.size());
			for (auto& kv : m_sessionManager)
				snapshot.push_back(kv.second);
		}
		for (auto& s : snapshot)
			s->Close();

		snapshot.clear();

		for (int i = 0; i < IO_WORKER_COUNT; i++)
			m_worker[i].request_stop();

		for (int i = 0; i < IO_WORKER_COUNT; i++)
		{
			if (m_worker[i].joinable())
				m_worker[i].join();
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

std::shared_ptr<Session> IOCP_server::FindSession(SessionId id)
{
	std::lock_guard lock(m_sessionMtx);
	auto it = m_sessionManager.find(id);
	return it == m_sessionManager.end() ? nullptr : it->second;
}

std::shared_ptr<Session> IOCP_server::RemoveSession(SessionId id)
{
	std::lock_guard lock(m_sessionMtx);

	auto it = m_sessionManager.find(id);
	if (it == m_sessionManager.end())
		return nullptr;

	auto sp = std::move(it->second);
	m_sessionManager.erase(it);

	return sp;      // 호출자가 놓으면 소멸
}


