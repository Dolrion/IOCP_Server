#include "CircularBuf.h"

size_t CircularBuf::Write(const int8_t* src, size_t n)
{
	if (!src || n == 0) return 0;

	size_t toWrite = std::min(n, FreeSize());
	if (toWrite < n) return 0;

	size_t first = std::min(toWrite, m_capacity - m_writePos);
	std::memcpy(&m_buffer[m_writePos], src, first);

	size_t second = toWrite - first;
	if (second > 0) std::memcpy(&m_buffer[0], src + first, second);

	m_writePos = (m_writePos + toWrite) % m_capacity;
	m_dataSize += toWrite;

	return toWrite;
}

bool CircularBuf::Peek(int8_t* dst, size_t n)
{
	if (!dst || n == 0) return false;
	if (n > m_dataSize) return false;
	Copy(dst, n);
	return true;
}

size_t CircularBuf::Read(int8_t* dst, size_t n)
{
	if (!dst || n == 0) return false;

	size_t readSize = std::min(n, m_dataSize);
	Copy(dst, readSize);
	m_readPos = (m_readPos + readSize) % m_capacity;
	m_dataSize -= readSize;
	return readSize;
}

void CircularBuf::Copy(int8_t* dst, size_t n)
{
	size_t first = std::min(n, m_capacity - m_readPos);
	std::memcpy(dst, &m_buffer[m_readPos], first);

	size_t second = n - first;
	if (second > 0) std::memcpy(dst + first, &m_buffer[0], second);
}
