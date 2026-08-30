
/*
* @@@ 전체 흐름 요약 @@@
* 1. PostAccept() → AcceptEx 등록 (코어 기반에 따른 최적 대기 스레드 등록)
* 2. 클라이언트 접속
* 3. IOCP에서 IO_ACCEPT 완료 통지
* 4. acceptSocket을 client socket으로 전환
* 5. client socket을 IOCP에 등록
* 6. PostRecv() → 수신 대기
* 7. PostAccept() → 다음 접속 대비
*/

#include <iostream>
#include "Process/IOCPSvrProc.h"

int main()
{
    std::unique_ptr<IOCPSvrProc> server = make_unique<IOCPSvrProc>();
    server->OpenServer(9000);

    while (true)
    {
        Sleep(1000);
    }

    return 0;
}