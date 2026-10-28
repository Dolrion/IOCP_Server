#include "IOCPSvrProc.h"
#include "../Pool/MemoryPool.h"
#include <chrono>

IOCPSvrProc::IOCPSvrProc()
{
	m_server = std::make_unique<IOCP_server>([this](SOCKET s) { RegisterClient(s); }, [this](SOCKET s) { ReleaseClient(s); });
}

IOCPSvrProc::~IOCPSvrProc()
{
	m_worker.request_stop();
	if (m_worker.joinable())
		m_worker.join();
}

bool IOCPSvrProc::OpenServer(int port)
{
	bool res = false;

	if (m_server->OpenServer(port))
	{
		m_worker = std::jthread([this](std::stop_token st) { GetRecvData(st); });
		res = true;
	}

	return res;
}

void IOCPSvrProc::RegisterClient(SOCKET socket)
{
	std::lock_guard<std::mutex> lock(m_mtx);
	m_sockList.emplace(socket, 0);
}

void IOCPSvrProc::ReleaseClient(SOCKET socket)
{
	std::lock_guard<std::mutex> lock(m_mtx);

	if (m_sockList.find(socket) != m_sockList.end())
	{
		m_sockList.erase(socket);
	}
}

void IOCPSvrProc::GetRecvData(std::stop_token token)
{
	MemoryPool& mp = MemoryPool::GetInstance();

	int cnt = 0;
	while (!token.stop_requested())
	{
		if (m_server->GetRecvData(&m_procBatch) > 0)
		{
			while (m_procBatch.size() > 0)
			{
				auto iter = m_procBatch.front();
				printf("Recv Data: %lld - count: %d\n", iter->header.len, ++cnt);
				mp.Deallocate(iter);		// 사용 완료된 청크 반환

				m_procBatch.pop();
			}
		}
		else
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
}
