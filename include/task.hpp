#pragma once

#include <coroutine>
#include <exception>
#include <future>
#include <memory>

namespace Redis {

template <typename T>
struct Task {
    struct promise_type {
        T value;
        std::exception_ptr exception;
        std::coroutine_handle<> waiter;

        Task get_return_object()
        {
            return Task { std::coroutine_handle<promise_type>::from_promise(*this) };
        }

        std::suspend_never initial_suspend() { return {}; }

        // 关键修改：使用 suspend_always，让协程在结尾保持挂起
        auto final_suspend() noexcept
        {
            struct FinalAwaiter {
                promise_type& promise;

                bool await_ready() noexcept { return false; }

                void await_suspend(std::coroutine_handle<promise_type> h) noexcept
                {
                    // 关键：保存自己的句柄，然后唤醒父协程
                    if (promise.waiter) {
                        promise.waiter.resume(); // 唤醒父协程
                    }
                    // 自己不 resume，保持挂起状态等待父协程来销毁
                }

                void await_resume() noexcept { }
            };
            return FinalAwaiter { *this };
        }

        void unhandled_exception() { exception = std::current_exception(); }

        template <typename U>
        void return_value(U&& val) { value = std::forward<U>(val); }
    };

    std::coroutine_handle<promise_type> handle;

    explicit Task(std::coroutine_handle<promise_type> h)
        : handle(h)
    {
    }

    // 关键修改：不要立即析构，需要显式管理
    ~Task()
    {
        // 不要在这里 destroy！让 Awaiter 来处理
        // 或者使用引用计数
    }

    struct Awaiter {
        std::coroutine_handle<promise_type> handle;

        bool await_ready() const noexcept
        {
            return handle.done();
        }

        void await_suspend(std::coroutine_handle<> awaiting_handle) noexcept
        {
            handle.promise().waiter = awaiting_handle;
            // 关键：如果协程已经完成（在 final_suspend 挂起），需要特殊处理
            if (handle.done()) {
                // 立即恢复父协程
                awaiting_handle.resume();
            }

            // 这个函数返回后就阻塞协程了
            // 控制返回给调用者/调度器
        }

        T await_resume()
        {
            // 先处理异常
            if (handle.promise().exception) {
                handle.destroy(); // 确保销毁
                std::rethrow_exception(handle.promise().exception);
            }

            T result = std::move(handle.promise().value);
            handle.destroy(); // 安全销毁
            return result;
        }
    };

    Awaiter operator co_await()
    {
        return Awaiter { handle };
    }
};

/* 可等待的future的包装 */
template <typename T>
struct AwaitableFuture {
    std::shared_future<T> future;

    /* 判断是否需要等待，如需等待->await_suspend */
    bool await_ready() const { return false; }

    /* await_suspend启动任务，之后协程就会暂停，等待调用resume唤醒 */
    void await_suspend(std::coroutine_handle<> handle)
    {
        auto handle_ptr = std::make_shared<std::coroutine_handle<>>(handle);
        std::thread([handle_ptr, future = future] {
            future.wait();
            if (*handle_ptr && !handle_ptr->done()) {
                handle_ptr->resume();
            }
        }).detach();
    }

    T await_resume()
    {
        return future.get();
    }
};

/*
    Task等待体本身并不支持等待，要想支持等待我们加入了Awaiter,重载了co_await
    而在Task的final_suspend中的FinalAwaiter用来在等待任务体协程完成之后


    Task<int> B() {
        co_return 42;  // 生产者
    }

    Task<void> A() {
        int value = co_await B();  // 消费者等待
        std::cout << value;
    }


    时间线 → 协程A（消费者）                协程B（生产者）
       │
       │  A() 开始执行
       │  ↓
       │  co_await B()
       │  → 调用 B().operator co_await()
       │  → 返回 Awaiter{handle_B}
       │
       │  ↓
       │  Awaiter::await_ready()
       │  → 检查 handle_B.done()
       │  → false（B还没执行完）
       │
       │  ↓
       │  Awaiter::await_suspend(handle_A)
       │  → handle_B.promise().waiter = handle_A
       │  → 协程A挂起（阻塞在这里）
       │
       │  [协程A暂停执行]
       │
       │                                      B() 开始执行（initial_suspend不挂起）
       │                                      ↓
       │                                      co_return 42
       │                                      → 调用 promise_type::return_value(42)
       │
       │                                      ↓
       │                                      执行 final_suspend()
       │                                      → 返回 FinalAwaiter{this->promise}
       │
       │                                      ↓
       │                                      FinalAwaiter::await_ready()
       │                                      → false（总是挂起）
       │
       │                                      ↓
       │                                      FinalAwaiter::await_suspend(handle_B)
       │                                      → 检查 promise.waiter（即 handle_A）
       │                                      → promise.waiter.resume() 恢复协程A
       │                                      → 协程B挂起（在final点等待销毁）
       │
       │  [协程A被恢复]
       │  ← 从 await_suspend 返回后，进入 await_resume
       │
       │  ↓
       │  Awaiter::await_resume()
       │  → 检查异常，获取 value=42
       │  → handle_B.destroy() 销毁协程B
       │  → 返回 value 给 A()
       │
       │  ↓
       │  A() 继续执行，打印42
       │  A() 执行完毕
       │  → 调用 A 的 final_suspend（类似逻辑）
       └─
*/

}