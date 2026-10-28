#pragma warning (disable:4996)

#include "Session.h"
#include "../Pool/MemoryPool.h"
#include <cstdio>

Session::Session(PacketCallback pushMessage, CloseCallback closeEvent) : m_recvBuf(RECV_RING_SIZE), m_msgComplete(std::move(pushMessage)), m_onClosed(std::move(closeEvent))
{
	m_recvCtx.type = IO_RECV;
}

Session::~Session()
{
	closesocket(m_session.socket);
}

void Session::SetSessionInfo(SOCKET socket, SOCKADDR_IN socketAddr)
{
	m_session.socket = socket;
	m_session.socketAddr = socketAddr;
}

bool Session::PostRecv()
{
	if (m_closed.load())
		return false;

	// 초기화 필수
	ZeroMemory(&m_recvCtx.overlapped, sizeof(OVERLAPPED));

	// I/O 를 걸기 '전에' 참조를 쥔다. (완료 통지는 걸자마자 다른 워커에 도착할 수 있음)
	m_recvCtx.owner = shared_from_this();

	DWORD flags = 0;
	int ret = WSARecv(m_session.socket, &m_recvCtx.buffer, 1, nullptr, &flags, &m_recvCtx.overlapped, NULL);

	if (ret == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING)
	{
		// 등록 실패 → 완료 통지가 오지 않으므로 참조를 직접 반납
		printf("[Session %llu] WSARecv failed: %d\n", (unsigned long long)m_session.socket, WSAGetLastError());
		m_recvCtx.owner.reset();
		Close();
		return false;
	}

	// 등록하는 사이 Close() 가 먼저 실행됐을 수 있으므로 한 번 더 확인해서 취소
	if (m_closed.load())
		CancelIoEx(reinterpret_cast<HANDLE>(m_session.socket), nullptr);

	return true;
}

void Session::RecvComplete(uint32_t size)
{
	if (size == 0 || m_closed.load())
	{
		Close();
		return;
	}

	if (m_recvBuf.Write(reinterpret_cast<const int8_t*>(m_recvCtx.data), size) == 0)
	{
		// 링버퍼 가득 찬 경우 소비가 밀렸거나 비정상 클라이언트 가정 -> 종료
		printf("[Session %llu] recv buffer overflow\n", (unsigned long long)m_session.socket);
		Close();
		return;
	}

	if (!ProcessPackets())
		return;

	// IOCP 수신 재등록 (recv context 재사용)
	PostRecv();
}

bool Session::ProcessPackets()
{
	// recv data processing
	MemoryPool& mp = MemoryPool::GetInstance();
	while (!m_closed.load() && m_recvBuf.DataSize() >= sizeof(Header))
	{
		Header header;
		m_recvBuf.Peek((int8_t*)(&header), sizeof(Header));

		// 수신 페이로드 크기 검사 (청크 허용 범위 초과 및 비정상 패킷 크기 처리 방지)
		if (header.len < sizeof(Header) || header.len > PACKET_CHUNK_SIZE)
		{
			printf("[Session %llu] invalid packet length: %llu\n", (unsigned long long)m_session.socket, (unsigned long long)header.len);
			Close();
			return false;
		}

		// 헤더에 명시된 크기만큼 아직 안 쌓였으면 다음 recv 를 기다림
		if (m_recvBuf.DataSize() < header.len)
			break;

		DataPacket* packet = static_cast<DataPacket*>(mp.Allocate());		// 메모리 청크에서 할당
		if (packet == nullptr)		// 풀 고갈 - 데이터 유지 후 탈출
		{
			break;
		}

		m_recvBuf.Read((int8_t*)(packet), header.len);
		OnSend(packet, packet->header.len);

		// 완성된 패킷을 등록한 callback 전달
		if (m_msgComplete)
			m_msgComplete(packet);
		else
			mp.Deallocate(packet);
	}

	return !m_closed.load();
}

bool Session::OnSend(void* sData, size_t len)
{
	// send data 오염 방지
	std::lock_guard<std::mutex> lock(m_mtxSend);

	if (m_closed.load())
		return false;

	auto ioCtx = m_sendCtxPool.Acquire();
	printf("socket %lld - acquire send io addr: 0x%p\n", m_session.socket, ioCtx);

	ioCtx->owner = shared_from_this();		// 전송 완료 때까지 Session 생존 보장
	ioCtx->Prepare(sData, static_cast<DWORD>(len));		// send 데이터 복사

	// Send 요청 등록
	DWORD sent = 0;
	int rst = WSASend(m_session.socket, &ioCtx->buffer, 1, &sent, 0, &ioCtx->overlapped, nullptr);
	if (rst == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING)
	{
		ioCtx->owner.reset();
		m_sendCtxPool.Release(ioCtx);

		Close();
		return false;
	}

	if (m_closed.load())
		CancelIoEx(reinterpret_cast<HANDLE>(m_session.socket), nullptr);

	return true;
}

// IOCP 완료 통지로부터 수신 받은 send 완료 context
void Session::SendComplete(SEND_IO_CONTEXT& ioCtx, bool success)
{
	// SEND_IO_CONTEXT 자원 반환 (재사용)
	ZeroMemory(&ioCtx.overlapped, sizeof(OVERLAPPED));
	m_sendCtxPool.Release(&ioCtx);
	printf("socket %lld - send io complete addr: 0x%p (pool size: %d)\n", m_session.socket, &ioCtx, m_sendCtxPool.GetSize());

	// 실패 -> 연결 종료
	if (!success)
		Close();
}

void Session::Close()
{
	if (m_closed.exchange(true))
		return;

	// 이 함수가 도는 동안 Session 이 파괴되지 않도록 자기 참조를 쥔다
	// (맵에서 제거되며 마지막 참조가 사라지는 경우 방어)
	std::shared_ptr<Session> self = shared_from_this();

	// 걸려 있는 I/O 를 오류로 완료시켜 깨운다. (핸들은 ~Session 에서 닫음)
	// shutdown 을 먼저 하면 이후 새로 거는 I/O 는 동기 실패하고, 이미 걸린 I/O 는 CancelIoEx 가 취소한다.
	shutdown(m_session.socket, SD_BOTH);
	CancelIoEx(reinterpret_cast<HANDLE>(m_session.socket), nullptr);

	// 닫힘 통지 (Session 당 1회) → 서버: 맵에서 제거 + disconnect 통지
	if (m_onClosed)
		m_onClosed(m_session.socket);
}




