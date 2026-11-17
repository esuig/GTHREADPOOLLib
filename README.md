# GThreadPoolLib

A lightweight, single-header C++ thread pool with a shared FIFO task queue,
optional error-driven stop, cancellation and progress reporting.

No dependencies beyond the standard library.

> **Project status:** this library is at version 0.0.1, an early release. It
> is still being tested and will be further developed, so its API may change
> in future versions. Feedback and bug reports are welcome.
>
> This library has been tested only on Windows and compiled with MSVC 2022 so far

## Features

- **Simple API**: create the pool, enqueue tasks, wait for them to finish.
- **Any callable**: lambdas, functors, function pointers or `std::function`.
- **FIFO order**: tasks are started in the order they were enqueued.
- **Reusable**: after `WaitUntilAllTasksFinished()` returns normally, new
  batches of tasks can be enqueued on the same pool.
- **Error-driven stop and cancellation**: after each task the pool can check
  an error handler and a cancellation flag, and stop processing the remaining
  tasks.
- **Progress reporting**: an optional interface is notified while the pool
  works through the tasks, e.g. to drive a progress bar.
- **Header-only**: copy one file into your project and include it.

## Getting started

Copy `GThreadPoolLib.h` into your project and include it:

```cpp
#include "GThreadPoolLib.h"

using namespace gthreadpoollib;
```

## Example

### Basic usage

```cpp
#include "GThreadPoolLib.h"
#include <atomic>
#include <iostream>
#include <thread>

using namespace gthreadpoollib;

int main()
{
    CGThreadPool pool(std::thread::hardware_concurrency(), nullptr);

    std::atomic<int> sum{0};
    for (int i = 1; i <= 100; ++i)
    {
        pool.EnqueueTask([&sum, i]() { sum += i; });
    }

    pool.WaitUntilAllTasksFinished();
    std::cout << sum << '\n';   // 5050

    // The pool can be reused for another batch
    pool.EnqueueTask([&sum]() { sum += 1; });
    pool.WaitUntilAllTasksFinished();
}
```

### Errors, cancellation and progress

Implement the two interfaces to let the pool stop on errors, honour a cancellation request and report progress.

## Important notes

### Tasks are copied

Tasks are stored as `std::function<void()>`, so **the callable is copied into
the queue**. If you pass an object with an `operator()` directly, the worker
runs a copy, and any state the task modifies is lost. Capture the object by
reference instead:

```cpp
CMyTask task;

pool.EnqueueTask(task);                     // wrong: runs on a copy of task
pool.EnqueueTask([&task]() { task(); });    // right: runs on task itself
```

For the same reason, move-only callables (e.g. lambdas capturing a `std::unique_ptr`) cannot be enqueued.

### Lifetime of captured objects

Objects captured by reference must outlive the tasks that use them. Call
`WaitUntilAllTasksFinished()` before they go out of scope.

### Stopping is final

When processing stops because of an error or a cancellation request, the
tasks still in the queue are not run, and `WaitUntilAllTasksFinished()` joins
all the worker threads. After that, the pool cannot run new tasks: create a
new pool if needed.

### Exceptions

Tasks must not let exceptions escape: an exception thrown out of a task
terminates the program (`std::terminate`), as for any `std::thread`. Catch
exceptions inside the task and report them, e.g. through your error handler.

### Shared data

The pool synchronizes access to its own queue only. Data shared between tasks
must be protected by the tasks themselves (atomics, mutexes, ...).

## License

Released under the [Boost Software License 1.0](LICENSE).

SPDX: `BSL-1.0`

## References

The design is based on ideas from: CppNuts, "Thread pool in C++", YouTube,
https://www.youtube.com/watch?v=u7ouCuieBhI
