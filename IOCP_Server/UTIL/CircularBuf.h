#pragma once
#include <cstdint>
#include <vector>

class CircularBuf
{
public:
	explicit CircularBuf(size_t capacity = 64 * 1024)
		: m_buffer(capacity),
		m_capacity(capacity),
		m_readPos(0),
		m_writePos(0),
		m_dataSize(0)
	{
	}

	size_t Capacity() const { return m_capacity; }
	size_t DataSize() const { return m_dataSize; }
	size_t FreeSize() const { return m_capacity - m_dataSize; }

	void Clear() { m_readPos = m_writePos = m_dataSize = 0; }
	size_t Write(const int8_t* src, size_t n);
	bool Peek(int8_t* dst, size_t n);
	size_t Read(int8_t* dst, size_t n);

private:
	void Copy(int8_t* dst, size_t n);

private:
	std::vector<int8_t> m_buffer;
	size_t m_capacity;
	size_t m_readPos;
	size_t m_writePos;
	size_t m_dataSize;
};
