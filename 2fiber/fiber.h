#ifndef _COROUTINE_H_
#define _COROUTINE_H_
#include <iostream>
#include <memory>
#include <atomic>
#include <functional>
#include <cassert>
#include <ucontext.h>
#include <unistd.h>
#include <mutex>
namespace sylar
{
    class Fiber : public std::enable_shared_from_this<Fiber>
    {
    public:
        enum State // 定义协程的状态，属于协程的上下文切换的时候，需要被保存
        {
            READY,
            RUNNING,
            TERM
        };

    private:
        Fiber(); // 细节1 Fiber()是私有的，只能被GetThis()方法调用，用于创建主协程。
    public:
        Fiber(std::function<void()> cb, size_t stacksize = 0, bool run_in_scheduler = true); // 用于创建指定回调函数、栈大小和 run_in_scheduler 本协程是否参与调度器调度，默认为true
        ~Fiber();

    public:
        void reset(std::function<void()> cb);   // 重置协程状态和⼊⼝函数，复⽤栈空间，不重新创建栈
        void resume();                          // 恢复协程执行。
        void yield();                           // 将执行全还给调度协程
        uint64_t getId() const { return m_id; } // 获取唯一标识
        State getState() const { return m_state; }

    public:
        static void SetThis(Fiber *f);           // 设置当前运行的协程。
        static std::shared_ptr<Fiber> GetThis(); // 获取当前运行的协程的shared_ptr实例。
        static void SetSchedulerFiber(Fiber *f); // 设置调度协程，默认主协程
        static uint64_t GetFiberId();            // 获取当前运行的协程的ID。
        static void MainFunc();                  // 协程的主函数，入口点
    private:
        uint64_t m_id = 0;          // 协程唯一标识符
        uint32_t m_stacksize = 0;   // 栈的大小
        State m_state = READY;      // 协程的初始状态是ready
        ucontext_t m_ctx;           // 协程的上下文
        void *m_stack = nullptr;    // 协程栈的指针
        std::function<void()> m_cb; // 协程的回调函数
        bool m_runInScheduler;      // 标志是否将执行器交给调度协程

        public:
        std::mutex m_mutex;
    };
}
#endif
