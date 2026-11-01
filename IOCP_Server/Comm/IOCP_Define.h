#pragma once

#ifndef _WINSOCK_DEPRECATED_NO_WARNINGS
#define _WINSOCK_DEPRECATED_NO_WARNINGS
#endif

#include <winsock2.h>
#include <mswsock.h>
#include <ws2tcpip.h>
#include <mstcpip.h>		// SIO_KEEPALIVE_VALS 정의

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>

#include "../Pool/MemoryPool.h"

#pragma comment(lib, "ws2_32.lib")

inline constexpr int IO_WORKER_COUNT = 4;			// IO 스레드 개수
inline constexpr int BUFFER_SIZE = 8 * 1024;		// IO 관련 buffer 사이즈는 통일 (8kb)
inline constexpr int ACCEPT_POST_COUNT = 8;			// 미리 등록할 AcceptEx 개수

// 세션당 수신 링버퍼: 미완성 패킷(최대 PACKET_CHUNK_SIZE-1) + 새로 받은 recv 1회분(BUFFER_SIZE) 이상이어야 함
inline constexpr size_t RECV_RING_SIZE = 32 * 1024;
inline constexpr size_t MAX_PENDING_SEND_BYTES = 1024 * 1024;	// 세션당 전송 대기 상한 (초과 시 느린 수신자로 보고 종료)

// 소켓/스레드 관련 설정값
inline constexpr DWORD KEEPALIVE_TIME_MS = 5000;		// 유휴 상태가 이 시간 지속되면 keepalive 전송 시작
inline constexpr DWORD KEEPALIVE_INTERVAL_MS = 1000;	// keepalive 재전송 간격
inline constexpr DWORD WORKER_POLL_MS = 100;			// 워커의 완료 통지 대기 시간 (= 종료 요청을 감지하는 주기)
inline constexpr int CONSUMER_POLL_MS = 100;			// 소비 스레드 폴링 주기
inline constexpr int ACCEPT_REPOST_RETRY = 3;			// accept 재등록 재시도 횟수
inline constexpr int ACCEPT_REPOST_DELAY_MS = 10;		// 재시도 간격

// 세션 식별자 - SOCKET 값을 그대로 사용
using SessionId = SOCKET;

class Session;

enum IO_TYPE { IO_ACCEPT, IO_RECV, IO_SEND };

// IO 컨텍스트만 OVERLAPPED 필요
struct BASE_IO_CONTEXT
{
	OVERLAPPED overlapped;
	IO_TYPE type;
	BASE_IO_CONTEXT() { ZeroMemory(&overlapped, sizeof(OVERLAPPED)); }
};

struct ACCEPT_CONTEXT : BASE_IO_CONTEXT
{
	SOCKET socket;
	// 각 주소마다 최소 sizeof(sockaddr) + 16 바이트 필요
	char addrBuf[(sizeof(sockaddr_in) + 16) * 2];
};

struct IO_CONTEXT : BASE_IO_CONTEXT
{
	std::shared_ptr<Session> owner;        // I/O가 걸려 있는 동안 참조 유지용
	WSABUF buffer;
	char data[BUFFER_SIZE];  // 버퍼

	IO_CONTEXT()
	{
		ZeroMemory(data, BUFFER_SIZE);
		buffer.buf = data;
		buffer.len = BUFFER_SIZE;			// 주의 data[] 크기와 BUFFER_SIZE 일치해야함
		ZeroMemory(&overlapped, sizeof(OVERLAPPED));
	}
};

struct SEND_IO_CONTEXT : BASE_IO_CONTEXT
{
	std::shared_ptr<Session> owner;        // I/O가 걸려 있는 동안 참조 유지용
	WSABUF buffer;
	std::vector<char> data;

	SEND_IO_CONTEXT()
	{
		buffer.buf = nullptr;
		buffer.len = 0;
		ZeroMemory(&overlapped, sizeof(OVERLAPPED));
	}

	void Prepare(const void* src, DWORD size)
	{
		ZeroMemory(&overlapped, sizeof(OVERLAPPED));
		type = IO_SEND;

		if (data.size() < size)
		{
			data.resize(size);
		}

		memcpy(data.data(), src, size);

		buffer.buf = data.data();
		buffer.len = size;
	}
};

struct SOCKET_INFO
{
	SOCKET socket = INVALID_SOCKET;
	SOCKADDR_IN socketAddr{};

	void CloseSocket()
	{
		if (socket != INVALID_SOCKET)
		{
			closesocket(socket);
			socket = INVALID_SOCKET;
		}
	}
};

// 패킷
#pragma pack(push, 1)
struct Header
{
	uint64_t id = 0;
	uint64_t len = 0;
};

struct DataPacket
{
	Header header;
	int8_t payload[1];	// 가변 길이 버퍼 시작 위치

	// 청크 전체(9216바이트 memory pool 참조) 중 헤더를 제외하고 데이터로 쓸 수 있는 최대 크기 계산
	static constexpr size_t MaxPayloadSize(size_t chunk_size = PACKET_CHUNK_SIZE)
	{
		return chunk_size - sizeof(Header);
	}
};
#pragma pack(pop)