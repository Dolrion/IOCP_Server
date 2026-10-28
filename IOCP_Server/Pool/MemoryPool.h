#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>

inline constexpr size_t PACKET_CHUNK_SIZE = 9216;		// 청크 1개 크기 (헤더 + payload)
inline constexpr size_t PACKET_CHUNK_COUNT = 1000;		// 청크 개수

/*
* chunk pool (lock-free)
*
* - 청크는 하나의 연속 블록에서 잘라 쓰고, 빈 청크 목록은 '인덱스 스택'으로 관리한다.
* - next 링크를 청크 '메모리 밖'(m_next 배열)에 둔다.
*   → 사용자가 청크에 쓰는 데이터와 풀의 링크 읽기가 같은 메모리를 건드리지 않으므로 data race 가 없다.
*     (청크 안에 next 포인터를 두는 방식은 pop 중인 스레드가 이미 다른 스레드가 가져가 쓰고 있는 청크의 next 를 읽게 됨)
* - head 에 버전 태그를 함께 넣어(ABA 방지) CAS 한다.
*   head = [ 상위 32bit: 태그(변경마다 +1) | 하위 32bit: 맨 위 청크 인덱스 ]
*/
class MemoryPool
{
private:
	static constexpr uint32_t NIL = 0xFFFFFFFFu;

	size_t m_chunkSize;
	size_t m_chunkCount;
	std::unique_ptr<char[]> m_block;						// 연속 메모리 (청크 i = m_block + i * m_chunkSize)
	std::unique_ptr<std::atomic<uint32_t>[]> m_next;		// 청크별 '다음 빈 청크' 인덱스
	std::atomic<uint64_t> m_head;

private:
	MemoryPool(size_t size, size_t count);
	~MemoryPool() = default;

public:
	MemoryPool(const MemoryPool&) = delete;
	MemoryPool& operator=(const MemoryPool&) = delete;

	static MemoryPool& GetInstance();

	void* Allocate();				// 고갈 시 nullptr
	void Deallocate(void* ptr);		// 풀 범위 밖/정렬 안 맞는 포인터는 무시
};
