
#ifndef H_THREAD_POOL
#define H_THREAD_POOL

#pragma once

#include <deque>
#include <functional>
#include <condition_variable>
#include <thread>
#include <mutex>
#include <vector>

namespace gthreadpoollib
{
	/**
	* @brief Interface for receiving errors raised by the tasks.
	*/
	class IThreadPoolErrorHandler
	{
	public:
		virtual ~IThreadPoolErrorHandler() = default;

		/**
		* @brief Called by a worker to check whether an error is occurred
		* @return true in case an error has occurred, false otherwise
		*/
		virtual bool IsErrorPresent() = 0;
	};

	/**
	* @brief Interface for progress notification and cancellation.
	*/
	class IThreadPoolProgress
	{
	public:
		virtual ~IThreadPoolProgress() = default;

		/**
		* @brief Polled by the pool to check whether the user requested cancellation.
		* @return true if the remaining tasks must be cancelled.
		*/
		virtual bool GetCancellationRequested() = 0;
		/**
		* @brief Increment the progress of one tick
		*/
		virtual void IncrementOneProgressStep() = 0;
	};

	/**
	* @class CGThreadPool
	* @brief A simple but effective thread pool implementation with a shared FIFO task
	* queue
	*
	* This class manages a pool of worker threads that asynchronously execute tasks.
	* Tasks are stored in a thread-safe queue and worker threads continuously fetch
	* and execute them.
	*
	* Internally, tasks are stored as `std::function<void()>`. This has an important
	* implication: **any callable passed to EnqueueTask is copied into the queue**.
	*
	* @warning Copy Semantics of Tasks
	* When a task is enqueued, it is wrapped into a `std::function<void()>`, which
	* **copies the callable object**. This means:
	*
	* - Passing objects directly (e.g. class instances with operator()) will result
	*   in **execution on a copy**, not on the original object.
	* - Any modification to internal state will affect only the copied instance.
	* This, for example, applies when the strategy has for example a vector as a member
	* ant it is expected that its operator fills it: if he strategy is passed by copy
	* to the thread pool, only the vector in the copied class will be filled, but not the
	* original one that will remain empty
	*
	* @par Example of Incorrect Usage
	* @code
	* CLayerStrategy strat;
	* threadPool.EnqueueTask(strat); // strat is COPIED! Its internal status is updated locally only!!
	* // strat's internal state will NOT be modified by the worker thread
	* @endcode
	*
	* @par Correct Usage (capture by reference)
	* @code
	* threadPool.EnqueueTask([&strat]()
	* {
	*     strat(); // this really operates on the original object
	* });
	* @endcode
	*
	* @note
	* The user is responsible for ensuring that referenced objects outlive the
	* execution of the enqueued tasks. Typically, this requires calling
	* WaitUntilAllTasksFinished() before those objects go out of scope.
	*
	* @par Thread Safety
	* - Task queue access is fully synchronized.
	* - Execution of tasks is parallel.
	* - Shared resources used inside tasks must be externally synchronized.
	*
	* Based on ideas from: https://www.youtube.com/watch?v=u7ouCuieBhI
	*/
	class CGThreadPool
	{
	public:
		/**
		* @brief Constructs a thread pool with a specified number of worker threads.
		*
		* @param f_threads			Number of worker threads in the pool.
		* @param f_error_manager	Pointer to an error manager to handle errors.
		* @param f_prog_bar_itfc	Pointer to progress bar (it is updated each time a task is complpeted)
		*/
		CGThreadPool(unsigned int f_threads,
					 IThreadPoolErrorHandler* f_error_manager,
					 IThreadPoolProgress* f_prog_bar_itfc = nullptr):
						m_error_manager(f_error_manager),
						m_prog_bar_itfc(f_prog_bar_itfc),
						m_num_threads(f_threads),
						m_cancelled(false)

		{
			m_busy_threads_count = 0;
			m_stop = false;

			for (unsigned int i = 0; i < f_threads; ++i)
			{

				m_workers.emplace_back(std::bind(&CGThreadPool::thread_proc, this));

			}
		}

		/**
		* @brief Enqueues a task to be executed by the thread pool.
		*
		* The task is added to the internal queue. One of the worker threads
		* will be notified to grab and execute it.
		*
		* This function is thread-safe.
		*
		* @tparam F Callable type of the task.
		* @param f_task The task to enqueue.
		*/
		template<class F>
		inline void EnqueueTask(F&& f_task)
		{
			std::unique_lock<std::mutex> lock(m_queue_mutex);
			m_tasks.emplace_back(std::forward<F>(f_task));

			lock.unlock();

			m_cv_task.notify_one();

		}

		/**
		* @brief Waits until all currently enqueued tasks have been picked up and executed.
		*
		* This function blocks the calling thread (usually the main thread) until
		* all tasks currently in the queue have been processed by the worker threads.
		*
		* Important: If this is not called, the main thread will continue execution immediately,
		* and the tasks will be processed asynchronously.
		* @return 1 in case user requested cancellation, 0 otherwise
		*/
		inline int WaitUntilAllTasksFinished()
		{
			std::unique_lock<std::mutex> lock(m_queue_mutex);
			m_cv_finished.wait(lock,
				[this]()
				{
					if (((m_stop == true) && (m_busy_threads_count == 0)))
					{
						if (m_prog_bar_itfc)
						{
							m_prog_bar_itfc->IncrementOneProgressStep();
						}
						return true;
					}
					else
					{
						if (((m_stop == false) && m_tasks.empty() && (m_busy_threads_count == 0)))
						{
							if (m_prog_bar_itfc)
							{
								m_prog_bar_itfc->IncrementOneProgressStep();
							}

							return true;
						}
						else
						{
							if ((m_busy_threads_count != 0) && (m_stop == true))
							{
								if (m_prog_bar_itfc)
								{
									m_prog_bar_itfc->IncrementOneProgressStep();
								}
							}
							else
							{
								if (m_prog_bar_itfc)
								{
									m_prog_bar_itfc->IncrementOneProgressStep();
								}
							}
						}
					}
					return false;
				}
			);

			ResetCounters();
			lock.unlock();

			if (m_stop == true)
			{
				m_cv_task.notify_all();

				for (auto& t : m_workers)
				{
					if (t.joinable())
					{
						t.join();
					}
				}
			}

			if (m_cancelled)
			{
				m_cancelled = false;
				return 1;
			}

			return 0;
		}

		/**
		* @brief Destructor.
		*
		* Signals all worker threads to stop and joins them.
		*/
		~CGThreadPool()
		{
			std::unique_lock<std::mutex> l_latch(m_queue_mutex);
			m_stop = true;
			m_cv_task.notify_all();
			l_latch.unlock();

			for (auto& t : m_workers)
			{
				if (t.joinable())
				{
					t.join();
				}
			}
		}

	private:
		/**
		* @brief Resets internal counters for reuse.
		*
		* This resets the count of busy threads to zero. Useful if the thread pool
		* is reused multiple times during the program's lifetime.
		*/
		inline void ResetCounters()
		{
			m_busy_threads_count = 0;
		}

		inline void thread_proc()
		{
			while (true)
			{
				std::unique_lock<std::mutex> l_latch(m_queue_mutex);
				m_cv_task.wait(l_latch, [this]() { return m_stop || !m_tasks.empty(); });

				if (m_stop)
				{
					break;
				}
				if (!m_tasks.empty())
				{

					++m_busy_threads_count;

					auto l_task_to_execute = m_tasks.front();
					m_tasks.pop_front();

					l_latch.unlock();

					l_task_to_execute();

					l_latch.lock();
					--m_busy_threads_count;

					const bool l_cancel = (m_prog_bar_itfc != nullptr) && m_prog_bar_itfc->GetCancellationRequested();
					const bool l_error = (m_error_manager != nullptr) && m_error_manager->IsErrorPresent();
					if (l_cancel || l_error)
					{
						if (m_prog_bar_itfc)
						{
							if (m_prog_bar_itfc->GetCancellationRequested())
							{
								m_cancelled = true;
							}
						}
						if (m_stop == false)
						{
							m_stop = true;
						}
						m_cv_finished.notify_one();
					}
					else
					{
						m_cv_finished.notify_one();
					}
				}
			}
		}

		/** @brief The list of threads */ 
		std::vector<std::thread> m_workers;
		/**
		* @brief The list of tasks. We use deque, so we have an object that can be accessed by 
		* both the extremes and we can create a FIFO queue (that means that we push_back
		* but pop_front, that is we grab the first task that has been pushed in the queue
		* each time)
		*/
		std::deque<std::function<void()>> m_tasks;
		/** @brief The mutex to guard the queue */
		std::mutex m_queue_mutex;
		/** @brief Condition variable to notify worker threads about new tasks */
		std::condition_variable m_cv_task;
		/**
		* @brief Condition variables to notify the presence of task/s in the queue (m_cv_task) 
		* and to notify the conclusion of a task (m_cv_finished); he notification in this latter
		* case will go to the main thread to check wheter all the tasks have been completed
		* or not
		*/
		std::condition_variable m_cv_finished;
		/** Variable to count how many threads are busy */
		unsigned int m_busy_threads_count;
		/**
		* Variable to require the stop of the calculation (when the hread pool is created, 
		* this is set to false; if this is set to true (for example by a callback to request 
		* the cancellation of the calculation), then the calculation will be stopped at the
		* next task grabbing (see thread_proc()). This is also set to true in the destructor
		* of the thread pool class to signal that the calculation must top (and all the
		* threads must be joined)
		*/
		bool m_stop;
		/** @brief Signals stop requested */
		bool m_cancelled;
		/** @brief Simply, the number of threads in the thread pool */
		unsigned int m_num_threads;

		/** Pointer to an error manager to handle errors */ 
		IThreadPoolErrorHandler* m_error_manager;

		/** @brief Pointer to progress bar interface(can be called each time a task is completed) */ 
		IThreadPoolProgress* m_prog_bar_itfc;
	};
}

#endif H_THREAD_POOL
