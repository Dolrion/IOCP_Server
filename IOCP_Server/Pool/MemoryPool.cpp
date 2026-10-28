#include "MemoryPool.h"

MemoryPool::MemoryPool(size_t size, size_t count) : m_chunkSize(size), m_chunkCount(count)
{
	m_block = std::make_unique_for_overwrite<char[]>(m_chunkSize * m_chunkCount);
	m_next = std::make_unique<std::atomic<uint32_t>[]>(m_chunkCount);

	// 0 → 1 → 2 → ... → (count-1) → NIL 순으로 연결
	for (size_t i = 0; i < m_chunkCount; i++)
		m_next[i].store(i + 1 < m_chunkCount ? static_cast<uint32_t>(i + 1) : NIL, std::memory_order_relaxed);

	m_head.store(m_chunkCount > 0 ? 0 : NIL, std::memory_order_release);		// tag = 0, index = 0
}

MemoryPool& MemoryPool::GetInstance()
{
	static MemoryPool instance(PACKET_CHUNK_SIZE, PACKET_CHUNK_COUNT);
	return instance;
}

void* MemoryPool::Allocate()
{
	uint64_t oldHead = m_head.load(std::memory_order_acquire);

	while (true)
	{
		const uint32_t idx = static_cast<uint32_t>(oldHead);		// 하위 32bit: 인덱스
		if (idx == NIL)
		{
			// chunk expend logic.. (작성하지 않음) → 고갈
			printf("remain space none!\n");
			return nullptr;
		}

		// 다른 스레드가 먼저 pop/push 했더라도 태그가 달라져 CAS 가 실패하므로 안전 (ABA 방지)
		const uint32_t next = m_next[idx].load(std::memory_order_relaxed);
		const uint64_t tag = (oldHead >> 32) + 1;
		const uint64_t newHead = (tag << 32) | next;

		if (m_head.compare_exchange_weak(oldHead, newHead, std::memory_order_acq_rel, std::memory_order_acquire))
			return m_block.get() + static_cast<size_t>(idx) * m_chunkSize;
		// 실패 시 oldHead 가 최신 값으로 갱신되어 재시도
	}
}

void MemoryPool::Deallocate(void* ptr)
{
	if (!ptr) return;

	const char* p = static_cast<const char*>(ptr);
	const ptrdiff_t offset = p - m_block.get();
	if (offset < 0 || static_cast<size_t>(offset) >= m_chunkSize * m_chunkCount || static_cast<size_t>(offset) % m_chunkSize != 0)
	{
		printf("MemoryPool::deallocate - invalid pointer ignored\n");
		return;
	}

	const uint32_t idx = static_cast<uint32_t>(static_cast<size_t>(offset) / m_chunkSize);
	uint64_t oldHead = m_head.load(std::memory_order_relaxed);

	while (true)
	{
		m_next[idx].store(static_cast<uint32_t>(oldHead), std::memory_order_relaxed);
		const uint64_t tag = (oldHead >> 32) + 1;
		const uint64_t newHead = (tag << 32) | idx;

		// release: 이 청크에 쓴 사용자 데이터가 이후 allocate 하는 스레드에게 보이도록
		if (m_head.compare_exchange_weak(oldHead, newHead, std::memory_order_acq_rel, std::memory_order_relaxed))
			return;
	}
}