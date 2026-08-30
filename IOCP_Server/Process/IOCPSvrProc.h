#include "../UTIL/CircularBuf.h"
#include "../Comm/IOCP_server.h"
#include <memory>
#include <pplwin.h>


class IOCPSvrProc
{
private:
	std::stop_token m_token;
	std::jthread m_worker;
	std::queue<DataPacket*> m_procBatch;

	std::unique_ptr<IOCP_server> m_server;
	std::unordered_map <SOCKET, int> m_sockList;

	std::mutex m_mtx;

private:
	void GetRecvData(std::stop_token token);

public:
	IOCPSvrProc();
	~IOCPSvrProc();

	bool OpenServer(int port);

	void RegisterClient(SOCKET socket);
	void ReleaseClient(SOCKET socket);
};