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

    bool await_ready() const { return false; }

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

}