#ifndef __SYLAR_IOMANAGER_H__
#define __SYLAR_IOMANAGER_H__

#include "scheduler.h"
#include "timer.h"

namespace sylar
{

    // work flow
    // 1 register one event -> 2 wait for it to ready -> 3 schedule the callback -> 4 unregister the event -> 5 run the callback
    // 1 注册事件 -> 2 等待事件 -> 3 事件触发调度回调 -> 4 注销事件回调后从epoll注销 -> 5 执行回调进入调度器中执行调度。
    s IOManager : public Scheduler, public TimerManager
    {
    public:
        enum Event // 内部枚举
        {
            NONE = 0x0, // 表示没有事件
            // READ == EPOLLIN
            READ = 0x1, // 表示读事件，对应于 epoll 的 EPOLLIN 事件。
            // WRITE == EPOLLOUT
            WRITE = 0x4 // 表示写事件，对应于 epoll 的 EPOLLOUT 事件。
        };

    private:
        // 用于描述一个文件描述的事件上下文
        // 每个socket fd都对应一个FdContext，包括fd的值，fd上的事件，以及fd的读写事件上下文
        struct FdContext
        {
            struct EventContext // 描述一个具体事件的上下文，如读事件或写事件。
            {
                // scheduler
                Scheduler *scheduler = nullptr; // 关联的调度器。
                // callback fiber
                std::shared_ptr<Fiber> fiber; // 关联的回调线程（协程）。
                // callback function
                std::function<void()> cb; // 关联的回调函数。(都会注册为协程对象)
            };

            // read event context
            EventContext read; // read 和write表示读和写的上下文
            // write event context
            EventContext write;
            int fd = 0; // 事件关联的fd(句柄)
            // events registered
            Event events = NONE; // 当前注册的事件，可能是 READ、WRITE 或二者的组合。
            std::mutex mutex;
            EventContext &getEventContext(Event event); // 根据事件类型获取相应的事件上下文（如读事件上下文或写事件上下文）。
            void resetEventContext(EventContext &ctx);  // 重置事件上下文。
            void triggerEvent(Event event);             // 触发事件,根据事件类型调用对应上下文结构的调度器去调度协程或函数
        };

    public:
        // threads线程数量，use_caller是否讲主线程或调度线程包含进行，name调度器的名字
        IOManager(size_t threads = 1, bool use_caller = true, const std::string &name = "IOManager"); // 允许设置线程数量、是否使用调用者线程以及名称。
        ~IOManager();
        // 事件管理方法
        //  add one event at a time
        int addEvent(int fd, Event event, std::function<void()> cb = nullptr); // 添加一个事件到文件描述符 fd 上，并关联一个回调函数 cb。
        // delete event
        bool delEvent(int fd, Event event); // 删除文件描述符fd上的某个事件
        // delete the event and trigger its callback
        bool cancelEvent(int fd, Event event); // 取消文件描述符上的某个事件，并触发其回调函数
        // delete all events and trigger its callback
        bool cancelAll(int fd); // 取消文件描述符 fd 上的所有事件，并触发所有回调函数。

        static IOManager *GetThis();

    protected:
        // 通知调度器有任务调度
        // 写pipe让idle协程从epoll_wait退出，待idle协程yield之后Scheduler::run就可以调度其他任务.
        void tickle() override;
        // 判断调度器是否可以停止
        // 判断条件是Scheduler::stopping()外加IOManager的m_pendingEventCount为0，表示没有IO事件可调度
        bool stopping() override;
        // 实际是idle协程只负责收集所有已触发的fd的回调函数并将其加⼊调度器
        // 的任务队列，真正的执⾏时机是idle协程退出后，调度器在下⼀轮调度时执⾏
        void idle() override; // 这里也是scheduler的重写，当没有事件处理时，线程处于空闲状态时的处理逻辑。

        void onTimerInsertedAtFront() override; // 因为Timer类的成员函数重写当有新的定时器插入到前面时的处理逻辑

        void contextResize(size_t size); // 调整文件描述符上下文数组的大小。

    private:
        int m_epfd = 0; // 用于epoll的文件描述符。
        // fd[0] read，fd[1] write
        int m_tickleFds[2];                            // 用于线程间通信的管道文件描述符，fd[0] 是读端，fd[1] 是写端。
        std::atomic<size_t> m_pendingEventCount = {0}; // 原子计数器，用于记录待处理的事件数量。使用atomic的好处是这个变量再进行加或-都是不会被多线程影响
        std::shared_mutex m_mutex;                     // 读写锁
        // store fdcontexts for each fd
        std::vector<FdContext *> m_fdContexts; // 文件描述符上下文数组，用于存储每个文件描述符的 FdContext。
    };

} // end namespace sylar

#endif
