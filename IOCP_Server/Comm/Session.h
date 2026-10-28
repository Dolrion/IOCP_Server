#pragma once

#include "IOCP_Define.h"
#include "../UTIL/CircularBuf.h"
#include "../Pool/InstancePool.h"
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>

/*
* @@@ Session 수명 규칙 @@@
* 1. Session 은 반드시 std::shared_ptr 로 생성한다. (enable_shared_from_this 사용)
* 2. WSARecv/WSASend 를 걸 때마다 해당 IO 컨텍스트의 owner 가 shared_ptr 을 하나 쥔다.
*    → 걸려 있는 I/O 가 하나라도 있으면 Session(과 그 안의 OVERLAPPED/버퍼)은 파괴되지 않는다.
* 3. 소켓 핸들은 ~Session 에서만 closesocket 한다.
*    → I/O 가 모두 끝난 뒤에만 핸들이 닫히므로 핸들 값 재사용으로 인한 오동작이 구조적으로 없다.
* 4. Close() 는 어느 스레드에서 몇 번 불려도 안전(멱등)하며, 걸려 있는 I/O 를 취소해 깨우기만 한다.
*    닫힘 통지(CloseCallback)는 Session 당 정확히 1회 호출된다.
* 5. Close()/Send() 를 호출하는 쪽은 Session 의 shared_ptr 을 쥐고 있어야 한다.
*/

class Session : public std::enable_shared_from_this<Session>
{
public:
	using PacketCallback = std::function<void(DataPacket*)>;
	using CloseCallback = std::function<void(SOCKET)>;

private:
	SOCKET_INFO m_session;
	std::atomic<bool> m_closed{ false };

	IO_CONTEXT m_recvCtx;									// 고유 recv context
	CircularBuf m_recvBuf;

	std::mutex m_mtxSend;
	InstancePool<SEND_IO_CONTEXT> m_sendCtxPool;		// send 데이터 완료 통지

	PacketCallback m_msgComplete;
	CloseCallback m_onClosed;

public:
	Session(PacketCallback pushMessage, CloseCallback closeEvent);
	~Session();

	Session(const Session&) = delete;
	Session& operator=(const Session&) = delete;
	
	void SetSessionInfo(SOCKET socket, SOCKADDR_IN socketAddr);

	bool PostRecv();
	void RecvComplete(uint32_t writeSize);
	bool ProcessPackets();

	bool OnSend(void* packet, size_t len);

	void Close();
	void SendComplete(SEND_IO_CONTEXT& ioCtx, bool success);
};

