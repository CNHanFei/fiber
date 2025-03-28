#include <unistd.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <cstring>

#include "ioscheduler.h"

static bool debug = true;

namespace sylar
{

    IOManager *IOManager::GetThis()
    {
        return dynamic_cast<IOManager *>(Scheduler::GetThis());
    }

    IOManager::FdContext::EventContext &IOManager::FdContext::getEventContext(Event event)
    {
        assert(event == READ || event == WRITE);
        switch (event)
        {
        case READ:
            return read;
        case WRITE:
            return write;
        }
        throw std::invalid_argument("Unsupported event type");
    }

    void IOManager::FdContext::resetEventContext(EventContext &ctx)
    {
        ctx.scheduler = nullptr;
        ctx.fiber.reset();
        ctx.cb = nullptr;
    }

    // no lock
    IOManager::FdContext::triggerEvent(IOManager::Event event)
    {
        assert(events & event); // 确保event是中有指定的事件，否则程序中断。

        // delete event
        // 清理该事件，表示不再关注，也就是说，注册IO事件是一次性的，
        // 如果想持续关注某个Socket fd的读写事件，那么每次触发事件后都要重新添加
        events = (Event)(events & ~event); // 因为不是使用了十六进制位，所以对标志位取反就是相当于将event从events中删除

        // trigge
        EventContext &ctx = getEventContext(event);
        if (ctx.cb) // 这个过程就相当于scheduler文件中的main.cc测试一样，把真正要执行的函数放入到任务队列中等线程取出后任务后，协程执行，执行完成后返回主协程继续，执行run方法取任务执行任务(不过可能是不同的线程的协程执行了)。
        {
            // call ScheduleTask(std::function<void()>* f, int thr)
            ctx.scheduler->scheduleLock(&ctx.cb);
        }
        else
        {
            // call ScheduleTask(std::shared_ptr<Fiber>* f, int thr)
            ctx.scheduler->scheduleLock(&ctx.fiber);
        }

        // reset event context
        resetEventContext(ctx);
        return;
    }

    IOManager::IOManager(size_t threads, bool use_caller, const std::string &name)
        : Scheduler(threads, use_caller, name), TimerManager()
    {
        // create epoll fd
        m_epfd = epoll_create(5000); // 5000，epoll_create 的参数实际上在现代 Linux 内核中已经被忽略，最早版本的 Linux 中，这个参数用于指定 epoll 内部使用的事件表的大小。
        assert(m_epfd > 0);          // 错误就终止程序

        // create pipe
        // 函数接受一个大小为2的数组类型指针m_tickleFds作为参数。
        // 成功时返回0，并将一对打开的文件描述符值填入m_tickleFds数组。失败时返回-1并设置errno
        1 int rt = pipe(m_tickleFds); // 创建管道的函数规定了m_tickleFds[0]是读端，1是写段
        assert(!rt);                  // 错误就终止程序

        // add read event to epoll
        // 将管道的监听注册到epoll上
        epoll_event event;
        event.events = EPOLLIN | EPOLLET; // Edge Triggered，设置标志位，并且采用边缘触发和读事件。
        event.data.fd = m_tickleFds[0];

        // non-blocked
        // 修改管道文件描述符以非阻塞的方式，配合边缘触发。
        rt = fcntl(m_tickleFds[0], F_SETFL, O_NONBLOCK);
        assert(!rt); // 每次需要判断rt是否成功

        rt = epoll_ctl(m_epfd, EPOLL_CTL_ADD, m_tickleFds[0], &event); // 将 m_tickleFds[0];作为读事件放入到event监听集合中
        assert(!rt);

        contextResize(32); // 初始化了一个包含 32 个文件描述符上下文的数组

        start(); // 启动 Scheduler，开启线程池，准备处理任务。
    }

    // IOManager的析构函数
    IOManager::~IOManager()
    {
        stop();                // 关闭scheduler类中的线程池，让任务全部执行完后线程安全退出
        close(m_epfd);         // 关闭epoll的句柄
        close(m_tickleFds[0]); // 关闭管道读端写端
        close(m_tickleFds[1]);
        // 将fdcontext文件描述符一个个关闭
        for (size_t i = 0; i < m_fdContexts.size(); ++i)
        {
            if (m_fdContexts[i])
            {
                delete m_fdContexts[i];
            }
        }
    }

    // no lock
    void IOManager::contextResize(size_t size)
    {
        m_fdContexts.resize(size); // 调整m_fdContexts的大小
        //  遍历 m_fdContexts 向量，初始化尚未初始化的 FdContext 对象
        for (size_t i = 0; i < m_fdContexts.size(); ++i)
        {
            if (m_fdContexts[i] == nullptr)
            {
                m_fdContexts[i] = new FdContext();
                m_fdContexts[i]->fd = i; // 将文件描述符的编号赋值给 fd
            }
        }
    }

    int IOManager::addEvent(int fd, Event event, std::function<void()> cb)
    {
        // 查找FdContext对象
        // attemp to find FdContext
        FdContext *fd_ctx = nullptr;

        std::shared_lock<std::shared_mutex> read_lock(m_mutex); // 使用读写锁
        if ((int)m_fdContexts.size() > fd)                      // 如果说传入的fd在数组里面则查找然后初始化FdContext的对象
        {
            fd_ctx = m_fdContexts[fd];
            read_lock.unlock();
        }
        else // 不存在则重新分配数组的size来初始化FdContext的对象
        {
            read_lock.unlock();
            std::unique_lock<std::shared_mutex> write_lock(m_mutex);
            contextResize(fd * 1.5);
            136 fd_ctx = m_fdContexts[fd];
        }
        // 一旦找到或者创建Fdcontextt的对象后，加上互斥锁，确保Fdcontext的状态不会被其他线程修改
        std::lock_guard<std::mutex> lock(fd_ctx->mutex);

        // the event has already been added
        if (fd_ctx->events & event) // 判断事件是否存在存在？是就返回-1，
                                    // 因为相同的事件不能重复添加
        {
            return -1;
        }

        // add new event
        // 所以这里就很好判断了如果已经存在就fd_ctx->events本身已经有读或写，就是修改已经有事件，如果不存在就是none事件的情况，就添加事件。
        int op = fd_ctx->events ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
        epoll_event epevent;
        epevent.events = EPOLLET | fd_ctx->events | event;
        epevent.data.ptr = fd_ctx;
        // 函数将事件添加到 epoll 中。如果添加失败，打印错误信息并返回 -1。
        int rt = epoll_ctl(m_epfd, op, fd, &epevent);
        if (rt)
        {
            std::cerr << "addEvent::epoll_ctl failed: " << strerror(errno) << std::endl;
            return -1;
        }

        ++m_pendingEventCount; // 原子计数器，待处理的事件++；

        // update fdcontext
        // 更新 FdContext 的 events 成员，记录当前的所有事件。
        // 注意events可以监听读和写的组合，如果fd_ctx->events为none,
        // 就相当于直接是fd_ctx->events = event
        fd_ctx->events = (Event)(fd_ctx->events | event);
        // update event context
        // 设置事件上下文
        FdContext::EventContext &event_ctx = fd_ctx->getEventContext(event);
        assert(!event_ctx.scheduler && !event_ctx.fiber && !event_ctx.cb); // 确保 EventContext 中没有其他正在执行的调度器、协程或回调函数。
        event_ctx.scheduler = Scheduler::GetThis();                        // 设置调度器为当前的调度器实例（Scheduler::GetThis()）。
        // 如果提供了回调函数 cb，则将其保存到 EventContext 中；否则，将当前正在运行的协程保存到 EventContext 中，并确保协程的状态是正在运行。
        if (cb)
        {
            event_ctx.cb.swap(cb);
        }
        else
        {
            event_ctx.fiber = Fiber::GetThis(); // 需要确保存在主协程
            assert(event_ctx.fiber->getState() == Fiber::RUNNING);
        }
        return 0;
    }

    bool IOManager::delEvent(int fd, Event event)
    {
        // attemp to find FdContext
        FdContext *fd_ctx = nullptr;
        // 这里的步骤和上面的addevent添加事件类似。
        std::shared_lock<std::shared_mutex> read_lock(m_mutex); // 读锁
        if ((int)m_fdContexts.size() > fd)                      // 查找到FdContext如果没查找到代表数组中没这个文件描述符直接，返回false；
        {
            fd_ctx = m_fdContexts[fd];
            read_lock.unlock();
        }
        else
        {
            read_lock.unlock();
            return false;
        }
        // 找到后添加互斥锁
        std::lock_guard<std::mutex> lock(fd_ctx->mutex);

        // the event doesn't exist
        if (!(fd_ctx->events & event)) // 如果事件不相同就返回false，否则就继续
        {
            return false;
        }

        // delete the event
        // 因为这里要删除事件，对原有的事件状态取反就是删除原有的状态比如说传入参数是读事件，我们取反就是删除了这个读事件但可能还要写事件
        Event new_events = (Event)(fd_ctx->events & ~event);
        int op = new_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
        epoll_event epevent;
        epevent.events = EPOLLET | new_events;
        epevent.data.ptr = fd_ctx; // 这一步是为了在 epoll 事件触发时能够快速找到与该事件相关联的 FdContext 对象。

        int rt = epoll_ctl(m_epfd, op, fd, &epevent);
        if (rt)
        {
            std::cerr << "delEvent::epoll_ctl failed: " << strerror(errno) << std::endl;
            return -1;
        }

        --m_pendingEventCount; // 减少了待处理的事件
        // update fdcontext
        fd_ctx->events = new_events; // 所以因为要先将fd_ctx的状态放入epevent.data.ptr所以就没先去更新，这也是为什么需要单独写Event new_events

        // update event context
        // 重置上下文
        FdContext::EventContext &event_ctx = fd_ctx->getEventContext(event);
        fd_ctx->resetEventContext(event_ctx);
        return true;
    }

    bool IOManager::cancelEvent(int fd, Event event)
    {
        // attemp to find FdContext
        FdContext *fd_ctx = nullptr;

        std::shared_lock<std::shared_mutex> read_lock(m_mutex);
        if ((int)m_fdContexts.size() > fd)
        {
            fd_ctx = m_fdContexts[fd];
            read_lock.unlock();
        }
        else
        {
            read_lock.unlock();
            return false;
        }

        std::lock_guard<std::mutex> lock(fd_ctx->mutex);

        // the event doesn't exist
        if (!(fd_ctx->events & event))
        {
            return false;
        }

        // delete the event
        Event new_events = (Event)(fd_ctx->events & ~event);
        int op = new_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
        epoll_event epevent;
        epevent.events = EPOLLET | new_events;
        epevent.data.ptr = fd_ctx;

        int rt = epoll_ctl(m_epfd, op, fd, &epevent);
        if (rt)
        {
            std::cerr << "cancelEvent::epoll_ctl failed: " << strerror(errno) << std::endl;
            return -1;
        }

        --m_pendingEventCount;

        // update fdcontext, event context and trigger
        fd_ctx->triggerEvent(event);
        return true;
    }

    bool IOManager::cancelAll(int fd)
    {
        // attemp to find FdContext
        FdContext *fd_ctx = nullptr;

        std::shared_lock<std::shared_mutex> read_lock(m_mutex);
        if ((int)m_fdContexts.size() > fd)
        {
            fd_ctx = m_fdContexts[fd];
            read_lock.unlock();
        }
        else
        {
            read_lock.unlock();
            return false;
        }

        std::lock_guard<std::mutex> lock(fd_ctx->mutex);

        // none of events exist
        if (!fd_ctx->events)
        {
            return false;
        }

        // delete all events
        int op = EPOLL_CTL_DEL;
        epoll_event epevent;
        epevent.events = 0;
        epevent.data.ptr = fd_ctx;

        int rt = epoll_ctl(m_epfd, op, fd, &epevent);
        if (rt)
        {
            std::cerr << "IOManager::epoll_ctl failed: " << strerror(errno) << std::endl;
            return -1;
        }

        // update fdcontext, event context and trigger
        if (fd_ctx->events & READ)
        {
            fd_ctx->triggerEvent(READ);
            --m_pendingEventCount;
        }

        if (fd_ctx->events & WRITE)
        {
            fd_ctx->triggerEvent(WRITE);
            --m_pendingEventCount;
        }

        assert(fd_ctx->events == 0);
        return true;
    }

    void IOManager::tickle()
    {
        // no idle threads
        if (!hasIdleThreads()) // 这个函数在scheduler检查当前是否有线程处于空闲状态。如果没有空闲线程，函数直接返回，不执行后续操作。
        {
            return;
        }
        // 如果有空闲线程，函数会向管道 m_tickleFds[1] 写入一个字符 "T"。这个写操作的目的是向等待在 m_tickleFds[0]（管道的另一端）的线程发送一个信号，通知它有新任务可以处理了。
        int rt = write(m_tickleFds[1], "T", 1);
        assert(rt == 1);
    }

    bool IOManager::stopping()
    {
        uint64_t timeout = getNextTimer();
        // no timers left and no pending events left with the Scheduler::stopping()
        return timeout == ~0ull && m_pendingEventCount == 0 && Scheduler::stopping();
    }

    void IOManager::idle()
    {
        static const uint64_t MAX_EVNETS = 256;
        std::unique_ptr<epoll_event[]> events(new epoll_event[MAX_EVNETS]);

        while (true)
        {
            if (debug)
                std::cout << "IOManager::idle(),run in thread: " << Thread::GetThreadId() << std::endl;

            if (stopping())
            {
                if (debug)
                    std::cout << "name = " << getName() << " idle exits in thread: " << Thread::GetThreadId() << std::endl;
                break;
            }

            // blocked at epoll_wait
            int rt = 0;
            while (true)
            {
                static const uint64_t MAX_TIMEOUT = 5000;
                uint64_t next_timeout = getNextTimer();
                next_timeout = std::min(next_timeout, MAX_TIMEOUT);

                rt = epoll_wait(m_epfd, events.get(), MAX_EVNETS, (int)next_timeout);
                // EINTR -> retry
                if (rt < 0 && errno == EINTR)
                {
                    continue;
                }
                else
                {
                    break;
                }
            };

            // collect all timers overdue
            std::vector<std::function<void()>> cbs;
            listExpiredCb(cbs);
            if (!cbs.empty())
            {
                for (const auto &cb : cbs)
                {
                    scheduleLock(cb);
                }
                cbs.clear();
            }

            // collect all events ready
            for (int i = 0; i < rt; ++i)
            {
                epoll_event &event = events[i];

                // tickle event
                if (event.data.fd == m_tickleFds[0])
                {
                    uint8_t dummy[256];
                    // edge triggered -> exhaust
                    while (read(m_tickleFds[0], dummy, sizeof(dummy)) > 0)
                        ;
                    continue;
                }

                // other events
                FdContext *fd_ctx = (FdContext *)event.data.ptr;
                std::lock_guard<std::mutex> lock(fd_ctx->mutex);

                // convert EPOLLERR or EPOLLHUP to -> read or write event
                if (event.events & (EPOLLERR | EPOLLHUP))
                {
                    event.events |= (EPOLLIN | EPOLLOUT) & fd_ctx->events;
                }
                // events happening during this turn of epoll_wait
                int real_events = NONE;
                if (event.events & EPOLLIN)
                {
                    real_events |= READ;
                }
                if (event.events & EPOLLOUT)
                {
                    real_events |= WRITE;
                }

                if ((fd_ctx->events & real_events) == NONE)
                {
                    continue;
                }

                // delete the events that have already happened
                int left_events = (fd_ctx->events & ~real_events);
                int op = left_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
                event.events = EPOLLET | left_events;

                int rt2 = epoll_ctl(m_epfd, op, fd_ctx->fd, &event);
                if (rt2)
                {
                    std::cerr << "idle::epoll_ctl failed: " << strerror(errno) << std::endl;
                    continue;
                }

                // schedule callback and update fdcontext and event context
                if (real_events & READ)
                {
                    fd_ctx->triggerEvent(READ);
                    --m_pendingEventCount;
                }
                if (real_events & WRITE)
                {
                    fd_ctx->triggerEvent(WRITE);
                    --m_pendingEventCount;
                }
            } // end for

            Fiber::GetThis()->yield();

        } // end while(true)
    }

    void IOManager::onTimerInsertedAtFront()
    {
        tickle();
    }

} // end namespace sylar