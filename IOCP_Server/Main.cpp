
/*
* @@@ 전체 흐름 요약 @@@
* 1. PostAccept() → AcceptEx 등록
* 2. 클라이언트 접속
* 3. IOCP에서 IO_ACCEPT 완료 통지
* 4. acceptSocket을 session 전환
* 5. Session 생성 (shared_ptr) → 맵 등록 → client socket을 IOCP에 등록
* 6. PostRecv() → 수신 대기 (I/O 컨텍스트가 Session 참조)
* 7. PostAccept() → 다음 접속 대비
*/

#include <iostream>
#include "Process/IOCPSvrProc.h"

int main()
{
	std::unique_ptr<IOCPSvrProc> server = std::make_unique<IOCPSvrProc>();

	int port = 0;
	std::cout << "포트 입력: ";
	if (!(std::cin >> port) || port < 1 || port > 65535)
	{
		std::cout << "잘못된 포트입니다.\n";
		return 1;
	}

	if (!server->OpenServer(port))
	{
		std::cout << "서버 Open 실패\n";
		return 1;
	}

	std::cout << "서버 Open (Enter 를 누르면 종료)\n";
	std::cin.ignore(1024, '\n');
	std::cin.get();

	server.reset();     // 세션 정리 → I/O 회수 → 워커 종료 순으로 정상 종료
	return 0;
}