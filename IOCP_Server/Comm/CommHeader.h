#pragma once

//#define _WINSOCKAPI_   // <- windows.h가 winsock.h 포함하지 못하게 막음 (프로젝트 전처리기 정의 필수)

#include <winsock2.h>
#include <mswsock.h>
#include <ws2tcpip.h>
#include <mstcpip.h>		// SIO_KEEPALIVE_VALS 정의

#include <stop_token>
#include <thread>
#include <vector>
#include <string>

#pragma comment(lib, "ws2_32.lib")

struct SOCKET_INFO
{
	SOCKET		socket;
	SOCKADDR_IN socketAddr;
};