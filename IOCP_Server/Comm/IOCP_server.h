#pragma once

#define _WINSOCK_DEPRECATED_NO_WARNINGS

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
	using SessionCallback = std::function<void(uint64_t)>;

private:
	HANDLE m_hIOCP;
	LPFN_ACCEPTEX m_lpfnAcceptEx;			// AcceptEx 포인터
	std::unique_ptr<SOCKET_INFO> m_listenSock;
	std::jthread m_worker[IO_WORKER_COUNT];				// 수신 스레드

	std::atomic<bool> m_open{ false };
	bool m_wsaStarted = false;

	std::mutex m_sessionMtx;
	std::unordered_map <SOCKET, std::shared_ptr<Session>> m_sessionManager;		// 클라이언트 리스트

	std::queue<DataPacket*> m_batch;
	std::mutex m_dataMtx;

	SessionCallback m_connectCall;
	SessionCallback m_disconnectCall;

private:
	bool PostAccept();
	void WorkProc(std::stop_token token);
	void OnAccept(ACCEPT_CONTEXT* rawCtx, bool ok);
	void CloseSession(uint64_t id);

public:
	IOCP_server(SessionCallback connectCall = nullptr, SessionCallback disconnectCall = nullptr);
	~IOCP_server();

	bool IsOpen() const { return m_open.load(); }

	bool OpenServer(int port);
	void CloseServerSocket();

	void InsertPacket(DataPacket* packet);
	int GetRecvData(std::queue<DataPacket*>* container);

	std::shared_ptr<Session> FindSession(uint64_t id);
	std::shared_ptr<Session> RemoveSession(uint64_t id);
};

