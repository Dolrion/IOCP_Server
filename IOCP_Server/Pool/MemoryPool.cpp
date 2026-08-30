#include "MemoryPool.h"

MemoryPool::MemoryPool(size_t size, size_t count) : chunk_size(std::max(size, sizeof(Chunk))), chunks_per_block(count)
{
	allocate_block();
}

MemoryPool::~MemoryPool()
{
}

MemoryPool& MemoryPool::GetInstance()
{
	static MemoryPool instance(9216, 1000); // C++11부터 thread-safe
	return instance;
}

void MemoryPool::allocate_block()
{
	// 큰 메모리 덩어리 할당
	size_t total_size = chunk_size * chunks_per_block;

	//auto new_block_ptr = std::unique_ptr<char[]>(new char[total_size]);
	auto new_block_ptr = std::make_unique_for_overwrite<char[]>(total_size);
	char* raw_new_block = new_block_ptr.get();

	// 관리 리스트에 추가 (풀이 파괴될 때 자동으로 해제됨)
	blocks.push_back(std::move(new_block_ptr));

	// 할당된 블록을 청크 단위로 쪼개서 프리 리스트에 연결
	for (size_t i = 0; i < chunks_per_block; ++i)
	{
		// 청크 크기만큼 주소 가리킴
		Chunk* chunk = reinterpret_cast<Chunk*>(raw_new_block + (i * chunk_size));
		chunk->next = free_list;
		free_list = chunk;
	}
}

void* MemoryPool::allocate()
{
	Chunk* old_head = free_list.load(std::memory_order_relaxed);

	// CAS를 이용한 Pop 연산
	while (old_head && !free_list.compare_exchange_weak(old_head, old_head->next, std::memory_order_acquire, std::memory_order_relaxed))
	{
		// 실패 시 old_head가 최신 free_list로 자동 업데이트
	}

	// free_list == null 인경우 청크 끝 도달 => 청크 확장
	// 새로운 청크 할당
	if (!old_head)
	{
		// chunk expend logic.. 
		// 작성하지 않음

		printf("remain space none!\n");
	}

	return old_head;
}

void MemoryPool::deallocate(void* ptr)
{
	if (!ptr) return;
	Chunk* chunk = reinterpret_cast<Chunk*>(ptr);

	// CAS를 이용한 Push 연산
	chunk->next = free_list.load(std::memory_order_relaxed);
	while (!free_list.compare_exchange_weak(chunk->next, chunk, std::memory_order_release, std::memory_order_relaxed))
	{
		// 실패 시 chunk->next가 최신 free_list로 자동 업데이트됨 (weak 특성)
	}
}