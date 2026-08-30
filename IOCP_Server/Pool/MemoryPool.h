#pragma once
#include <vector>
#include <memory>

// chunck pool
class MemoryPool
{
private:
	struct Chunk
	{
		Chunk* next;
	};

	size_t chunk_size;                              // 각 청크의 크기
	size_t chunks_per_block;                        // 한 번에 할당할 청크 개수
	std::atomic<Chunk*> free_list{ nullptr };       // 다음 청크 주소 (lock free)
	std::vector<std::unique_ptr<char[]>> blocks;    // 관리 중인 전체 블록 리스트 (메모리 해제용)

	void allocate_block();

private:
	MemoryPool(size_t size, size_t count = 10);
	~MemoryPool();

public:
	static MemoryPool& GetInstance();

	void* allocate();
	void deallocate(void* ptr);
};
