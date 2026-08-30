#pragma warning (disable:4996)

#include "Session.h"
#include "../Pool/MemoryPool.h"
#include <iostream>

Session::Session(std::function<void(DataPacket*)> pushMessage, std::function<void(SOCKET_INFO&)> disconnectEvent, int bufSize)
{
	recvBuf = make_unique<CircularBuf>(bufSize);
	this->m_msgComplete = pushMessage;
	this->m_disconnected = disconnectEvent;

	packetPool = std::make_unique<InstancePool<SendPakcet>>();
	m_sendCtxPool = std::make_unique<InstancePool<SEND_IO_CONTEXT>>();
}

Session::~Session()
{
	Disconnect();
}

bool Session::PostRecv()
{
	// 초기화 필수
	ZeroMemory(&ioCtx.overlapped, sizeof(OVERLAPPED));

	DWORD flags = 0;
	DWORD recvBytes = 0;

	int ret = WSARecv(session.socket, &ioCtx.buffer, 1, nullptr, &flags, &ioCtx.overlapped, NULL);

	if (ret == SOCKET_ERROR)
	{
		int err = WSAGetLastError();
		if (err != WSA_IO_PENDING)
		{
			std::cout << "WSARecv failed: " << err << std::endl;
			Disconnect();
			return false;
		}
	}

	return true;
}

void Session::RecvData(int8_t* data, uint32_t size)
{
	// recv data 오염 방지
	std::unique_lock lock(m_mtxRecv);
	recvBuf->Write(data, size);

	// recv data processing
	MemoryPool& mp = MemoryPool::GetInstance();
	while (true)
	{
		if (recvBuf->DataSize() < sizeof(Header))
			break;

		Header header;
		recvBuf->Peek((int8_t*)(&header), sizeof(Header));

		// 헤더에 명시된 크기만큼 데이터가 쌓인 경우
		if (recvBuf->DataSize() >= header.len)
		{
			DataPacket* completePacket = static_cast<DataPacket*>(mp.allocate());		// 메모리 청크에서 할당

			recvBuf->Read((int8_t*)(&completePacket->header), sizeof(Header));

			uint32_t payloadSize = completePacket->header.len - sizeof(Header);
			// 수신 페이로드 크기 검사 (청크 허용 범위 초과 방지)
			if (payloadSize > DataPacket::MaxPayloadSize())
			{
				mp.deallocate(completePacket);
				return;
			}

			memcpy(completePacket->payload, &completePacket->header, sizeof(Header));

			if (payloadSize > 0)
			{
				recvBuf->Read(completePacket->payload + sizeof(Header), payloadSize);
			}

			OnSend(completePacket->payload, completePacket->header.len);

			// 완성된 패킷을 등록한 callback에 전달
			m_msgComplete(std::move(completePacket));
		}
		else
			break;
	}
	lock.unlock();

	// IOCP 수신 재등록 (recv context 재사용)
	PostRecv();
}

void Session::OnSend(void* sData, size_t len)
{
	// send data 오염 방지
	std::unique_lock lock(m_mtxSend);

	auto ioCtx = m_sendCtxPool->Acquire();
	printf("acquire io addr: 0x%p \n", ioCtx);

	ioCtx->type = IO_SEND;
	ioCtx->Prepare(sData, len);		// send 데이터 복사

	// Send 요청 등록
	DWORD sent = 0;
	int ret = WSASend(session.socket, &ioCtx->buffer, 1, &sent, 0, &ioCtx->overlapped, nullptr);

	if (ret == SOCKET_ERROR)
	{
		int err = WSAGetLastError();
		if (err != WSA_IO_PENDING)
		{
			// 실패 시 자원 반환
			ZeroMemory(&ioCtx->overlapped, sizeof(OVERLAPPED));
			m_sendCtxPool->Release(ioCtx);
		}
	}

	lock.unlock();
}

// IOCP 완료 통지로부터 수신 받은 send 완료 context
void Session::SendComplete(SEND_IO_CONTEXT& sioctx)
{
	// SEND_IO_CONTEXT 자원 반환 (재사용)
	ZeroMemory(&sioctx.overlapped, sizeof(OVERLAPPED));
	printf("%lld socket io addr: 0x%p count: %d\n", session.socket, &sioctx, m_sendCtxPool->GetSize());
	m_sendCtxPool->Release(&sioctx);
}

void Session::Disconnect()
{
	session.CloseSocket();

	// 소켓 연결 끊김 이벤트 전달
	m_disconnected(*&session);
}




