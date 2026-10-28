#pragma once
#include <mutex>
#include <stack>

template<typename T>
class InstancePool
{
private:
	std::mutex m_mtx;
	std::stack<T*> m_pool;

public:
	InstancePool() = default;
	~InstancePool()
	{
		std::lock_guard<std::mutex> lock(m_mtx);
		while (!m_pool.empty())
		{
			delete m_pool.top();
			m_pool.pop();
		}
	}

	int GetSize() { return m_pool.size(); }

	T* Acquire()
	{
		std::lock_guard<std::mutex> lock(m_mtx);
		if (!m_pool.empty())
		{
			auto* obj = m_pool.top();
			m_pool.pop();
			return obj;
		}
		return new T{};
	}

	void Release(T* obj)
	{
		std::lock_guard<std::mutex> lock(m_mtx);
		m_pool.push(obj);
	}
};
