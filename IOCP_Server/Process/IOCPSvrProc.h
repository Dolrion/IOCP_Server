#pragma once

#include "../Comm/IOCP_server.h"
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>

class IOCPSvrProc
{
private:
	std::queue<DataPacket*> m_procBatch;

	std::mutex m_mtx;
	std::unordered_map <SOCKET, int> m_sockList;

	std::jthread m_worker;
	std::unique_ptr<IOCP_server> m_server;

private:
	void GetRecvData(std::stop_token token);

public:
	IOCPSvrProc();
	~IOCPSvrProc();

	bool OpenServer(int port);

	void RegisterClient(SOCKET socket);
	void ReleaseClient(SOCKET socket);
};