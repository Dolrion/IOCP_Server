#pragma once

#include "../UTIL/CircularBuf.h"
#include "IOCP_Define.h"
#include "Session.h"
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>

class IOCP_server
{
public:
	using SessionCallback = std::function<void(SessionId)>;

private:
	HANDLE m_hIOCP;
	LPFN_ACCEPTEX m_lpfnAcceptEx;			// AcceptEx 포인터
	std::unique_ptr<SOCKET_INFO> m_listenSock;
	std::jthread m_worker[IO_WORKER_COUNT];				// IO 워커 스레드

	std::atomic<bool> m_open{ false };
	bool m_wsaStarted = false;

	std::mutex m_sessionMtx;
	std::unordered_map <SessionId, std::shared_ptr<Session>> m_sessionManager;		// 세션 리스트

	std::queue<DataPacket*> m_batch;
	std::mutex m_dataMtx;

	SessionCallback m_connectCall;
	SessionCallback m_disconnectCall;

private:
	bool PostAccept();
	void RepostAccept();
	void WorkProc(std::stop_token token);

	// accept 완료 처리 = 소켓 설정 → 세션 생성/등록 → 첫 recv
	void OnAccept(ACCEPT_CONTEXT* rawCtx, bool ok);
	bool ConfigureAcceptedSocket(SOCKET accSocket, SOCKADDR_IN& peerAddr);
	std::shared_ptr<Session> CreateSession(SOCKET accSocket, const SOCKADDR_IN& peerAddr);

	void CloseSession(SessionId id);

public:
	IOCP_server(SessionCallback connectCall = nullptr, SessionCallback disconnectCall = nullptr);
	~IOCP_server();

	bool IsOpen() const { return m_open.load(); }

	bool OpenServer(int port);
	void CloseServerSocket();

	void InsertPacket(DataPacket* packet);
	int GetRecvData(std::queue<DataPacket*>* container);

	std::shared_ptr<Session> FindSession(SessionId id);
	std::shared_ptr<Session> RemoveSession(SessionId id);
};

