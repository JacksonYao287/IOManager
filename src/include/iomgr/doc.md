IOManager 设计与实现详解

  0. 背景与设计哲学

  IOManager（eBay 开源）要解决的核心矛盾：存储/数据库类应用做异步 IO
  时，传统模式是网络线程收到请求后扔给线程池做阻塞 IO——这样有上下文切换和跨核内存访问的代价。业界有两种
  run-to-completion 模型：

  - 中断式多路复用（epoll
    类）：少量线程阻塞在内核等事件，事件到来被唤醒，线程数可以有弹性，但吞吐/延迟不是极致。
  - 忙轮询（SPDK 式 tight
    loop）：线程永不睡眠，持续轮询用户态队列，单核性能逼近硬件极限，但每个这样的线程要独占一个 CPU core。

  IOManager 的做法是把这两种模型用同一套抽象（Reactor/Fiber/IODevice/消息机制）统一起来，允许二者并存，再
  加上一层磁盘 IO 后端（libaio/io_uring/SPDK bdev）的统一抽象，组成一个三层架构：

  应用层
     │
  IOInterface / DriveInterface  (设备与IO语义抽象，GenericIOInterface / DriveInterface /
  SpdkNvmfInterface)
     │
  IOReactor + IOFiber            (事件循环 + 协程式轻量线程，EPoll / SPDK 两种实现)
     │
  IODevice                        (fd / spdk_bdev_desc / nvmf_qpair 的统一句柄)
     │
  内核 epoll / libaio / io_uring / SPDK(DPDK 用户态驱动)

  贯穿全文的高性能设计母题，先列在这里，后面逐一对应到代码：

  1. 每线程私有状态 + thread_local，几乎不用跨线程锁；
  2. **协作式协程调度（fiber）**代替锁——同一 reactor 内的 fiber 互斥执行，共享数据天然无竞争；
  3. 无锁跨线程队列（folly UMPSCQueue）+ eventfd 唤醒，且做唤醒抑制降低系统调用；
  4. run-to-completion + 批处理：一轮处理完一批事件/消息再让出；
  5. CPU 绑核 + 独占轮询（SPDK）或自适应退避轮询（epoll）二选一地平衡延迟和 CPU 占用；
  6. 零拷贝/DMA 内存（SPDK hugepage）、小对象内联（iovec≤4个免堆分配）、对象池减少内存分配开销；
  7. 批量系统调用（io_submit/io_uring_submit 一次提交多个 IO）；
  8. 运行时能力探测，一次判定、热路径零分支（io_uring 能力探测、设备类型探测均有缓存）。

  ---

  1. IOManager：全局管理器与状态机

  1.1 单例与核心数据结构

  IOManager 是 Meyer's singleton（src/include/iomgr/iomgr.hpp），通过宏 iomanager 全局访问：

  static IOManager& instance() { static IOManager inst; return inst; }
  #define iomanager iomgr::IOManager::instance()

  核心成员：

  - std::unique_ptr<IOManagerImpl> m_impl：策略模式，根据运行模式切换 IOManagerEpollImpl 或
    IOManagerSpdkImpl，接口只有 4 个钩子：pre/post_interface_init、create_reactor_impl、pre/post_interface
    _stop。这一层把"系统线程怎么建、CPU 怎么绑"这种与 reactor 类型强相关的细节，从 IOManager
    主流程中剥离出去。
  - std::vector<std::shared_ptr<IOReactor>> m_worker_reactors + std::vector<sys_thread_id_t>
    m_worker_threads：worker reactor 按固定 slot 下标索引。sys_thread_id_t 是 std::variant<std::thread,
    eal_core_id_t>——epoll 模式下是普通 std::thread，SPDK 模式下是 DPDK 的 lcore id（线程由 DPDK EAL
    自己创建并 pin 到核上）。
  - sisl::ActiveOnlyThreadBuffer<shared_ptr<IOReactor>> m_reactors：既含 worker 也含 user reactor
    的线程本地集合（每个物理线程一个槽位），access_all_threads()
    遍历时不需要全局锁；若要访问"当前线程自己"的 reactor，有快捷路径直接返回 this_reactor() 而不走查找。
  - sisl::atomic_counter<int16_t> m_yet_to_start_nreactors /
    m_yet_to_stop_nreactors：用于无锁地统计"启动中/停止中还剩几个 reactor 没完成"，核心操作是原子的
    decrement_testz()（减并判零），避免"先减后比较"两步操作间的竞态窗口。
  - iomgr_state m_state：状态机当前值，由 std::mutex m_cv_mtx + std::condition_variable m_cv 保护（不是
    atomic）——因为状态切换只发生在 start/stop 这种低频路径，用 mutex+cv 等某个状态出现，比 busy-wait
    更合适；真正的热路径（reactor 计数、消息分发、round-robin 选择）全部用 atomic/atomic_counter。

  1.2 启动流程 start()

  状态机严格单向流转：stopped → interface_init → reactor_init → sys_init → running。关键步骤：

  1. 线程数裁决：优先用调用方传入的 num_threads，否则读取动态配置 thread.num_workers（默认 2），并与
     cgroup CPU quota（读 cpu.cfs_quota_us/cpu.cfs_period_us）取最小——容器化部署下自动收敛线程数，避免超售
     CPU 导致上下文切换开销暴涨。
  2. 内存限额：综合 /proc/meminfo、cgroup memory limit、HUGEPAGE 环境变量算出
     m_mem_size_limit/m_hugepage_limit，并算出 85%/95% 两级内存回收水位线。
  3. 创建 Impl 策略对象并调用 pre_interface_init()。
  4. io_uring 能力探测：syscall(__NR_io_uring_register, ...) + io_uring_get_probe() 判断内核是否支持且
     READV/WRITEV/FSYNC/READ/WRITE 这些操作码齐全，结果缓存，运行期零判断。
  5. 驱动后端自动选型：无自定义配置时，uring 能力 && 非spdk → UringDriveInterface，否则
     AioDriveInterface；SPDK 模式再加 SpdkDriveInterface。
  6. create_worker_reactors()：先把 m_worker_reactors 填满 nullptr 占位（固定好 vector
     大小），再并发地为每个 slot 调 m_impl->create_reactor_impl(...)
     真正创建系统线程——这是"先分配容量、再并发填充"的无锁技巧，避免 vector
     扩容期间其它线程访问引发的竞态。
  7. 每个新线程跑进 IOReactor::run() 后回调 IOManager::reactor_started()：

  void IOManager::reactor_started(shared<IOReactor> reactor) {
      m_yet_to_stop_nreactors.increment();
      if (reactor->is_worker()) {
          m_worker_reactors[reactor->m_worker_slot_num] = reactor;
          reactor->notify_thread_state(true);
          if (m_yet_to_start_nreactors.decrement_testz())
              set_state_and_notify(iomgr_state::sys_init);
      } else {
          reactor->notify_thread_state(true);
      }
  }

  用原子计数器实现了一个无锁的"启动屏障(barrier)"：最后一个完成初始化的 reactor 负责把全局状态推进到
  sys_init。
  8. 启动全局定时器（m_global_user_timer 恒为 timer_epoll；m_global_worker_timer 在 SPDK 下是
  timer_spdk）。
  9. post_interface_init() → 状态置 running → 用 run_on_forget(reactor_regex::all_io, ...) 广播通知所有
  reactor。
  10. 最后创建 IOWatchDog。

  User Reactor 走完全不同的路径：become_user_reactor() 不另开线程，而是在调用者自己的线程里 new 一个
  reactor 对象并调用其 run()（阻塞调用），把调用线程直接"变成"一个 reactor——对应 README
  中"应用自己起线程、后续加入成为 reactor"的描述。

  1.3 停止流程 stop()

  同样是状态机驱动：pre_interface_stop() → stopping → 释放全局 timer 并 wait_for_pending() →
  用同步广播消息 run_on_wait(reactor_regex::all_io, [] { this_reactor()->stop(); }) 通知每个 reactor 退出
  → 用 m_yet_to_stop_nreactors 等所有 reactor 真正退出（reactor_stopped() 回调里 decrement_testz()
  触发状态到 stopped）→ join() 所有 std::thread（SPDK 的 lcore 不需要 join，由 DPDK 管理）→ 清空容器、重置
  impl。

  有个细节：stop() 一开始先手动 m_yet_to_stop_nreactors.increment()
  再发消息，注释说明是为了防止"当前没有任何 reactor 在跑"时计数器提前归零导致 stop()
  永久阻塞——这是计数器型同步原语里常见的"占位 +1 防止假归零"手法。

  1.4 跨线程消息与 Reactor 选择算法

  round_robin_reactor() 用 static std::atomic<size_t> s_idx 以 memory_order_relaxed
  自增取模，因为这里只要求"分布均匀"不要求跨线程的顺序一致性，刻意不用默认的 seq_cst，省掉内存屏障。

  multicast_msg() 给多个 reactor 广播同一条逻辑消息时，不是简单复制 N 份对象，而是"滚动克隆"：发给前 N-1
  个 reactor 用 ctx.cloned_msg = ctx.msg->clone()，最后一个直接复用原始消息对象——省掉一次多余的
  clone/free。

  1.5 IOEnvironment：横切关注点的编排层

  IOEnvironment（ioenvironment 宏）是另一个独立单例，用 builder
  链式调用（with_iomgr(...)->with_http_server(...)->...）组装 IOManager 之外的基础设施：内置 HTTP
  服务器（见第 6 节）、sisl::FileWatcher（配置热更新）、TokenVerifier/TokenClient（鉴权）、sobject_manager
  （可观测对象导出）。它与 IOManager 是组合关系，不管理 reactor，只负责把这些辅助系统和 IOManager 的启动串
  成一条链。动态配置的默认值兜底（IOMgrDynamicConfig::init_settings_default()）也是在这里首次触发的。

  1.6 配置系统（FlatBuffers + 热更新）

  配置用 FlatBuffers schema 定义，通过 sisl::settings 支持运行时热更新（IM_DYNAMIC_CONFIG(xxx)
  宏读取）。与性能强相关的配置项：

  ┌─────────────────────────────────────────────┬──────────┬───────┬─────────────────────────────────┐
  │                    配置                     │  默认值  │ 可热  │              作用               │
  │                                             │          │  更   │                                 │
  ├─────────────────────────────────────────────┼──────────┼───────┼─────────────────────────────────┤
  │ thread.num_workers                          │ 2        │ 否    │ worker reactor（系统线程）数量  │
  ├─────────────────────────────────────────────┼──────────┼───────┼─────────────────────────────────┤
  │ thread.num_fibers                           │ 4        │ 否    │ 每个 reactor 内的 fiber（IO     │
  │                                             │          │       │ Thread）数量                    │
  ├─────────────────────────────────────────────┼──────────┼───────┼─────────────────────────────────┤
  │ drive.num_batch_io_limit                    │ 64       │ 是    │ SPDK 请求批量提交上限           │
  ├─────────────────────────────────────────────┼──────────┼───────┼─────────────────────────────────┤
  │ drive.uring_per_thread_qdepth               │ 256      │ 否    │ io_uring 每线程队列深度         │
  ├─────────────────────────────────────────────┼──────────┼───────┼─────────────────────────────────┤
  │ drive.max_resubmit_cnt                      │ 3        │ 是    │ IO 出错重试上限                 │
  ├─────────────────────────────────────────────┼──────────┼───────┼─────────────────────────────────┤
  │ drive.disable_io_uring                      │ false    │ —     │ 强制禁用 io_uring               │
  ├─────────────────────────────────────────────┼──────────┼───────┼─────────────────────────────────┤
  │ poll.force_wakeup_by_time_ms                │ 10       │ —     │ 空闲轮询/强制唤醒周期           │
  ├─────────────────────────────────────────────┼──────────┼───────┼─────────────────────────────────┤
  │ poll.tight_loop_after_io_max                │ 4        │ —     │ 连续几次有 IO 后切入 tight loop │
  ├─────────────────────────────────────────────┼──────────┼───────┼─────────────────────────────────┤
  │ poll.backoff_delay_min/max_us、increase_fac │ 2 / 500  │ 部分  │ 自适应指数退避参数              │
  │ tor                                         │ / 2.0    │       │                                 │
  ├─────────────────────────────────────────────┼──────────┼───────┼─────────────────────────────────┤
  │ message.max_msgs_before_yield               │ 100      │ 是    │ 单次最多处理多少条跨线程消息再  │
  │                                             │          │       │ 让出                            │
  ├─────────────────────────────────────────────┼──────────┼───────┼─────────────────────────────────┤
  │ iomem.soft/aggressive_mem_release_threshold │ 85% /    │ —     │ 内存回收水位线                  │
  │                                             │ 95%      │       │                                 │
  ├─────────────────────────────────────────────┼──────────┼───────┼─────────────────────────────────┤
  │                                             │ 4096:50% │       │                                 │
  │ iomem.pool_sizes                            │ , 65536: │ —     │ IO buffer 分级内存池            │
  │                                             │ 20%      │       │                                 │
  └─────────────────────────────────────────────┴──────────┴───────┴─────────────────────────────────┘

  poll 表是 README 所说"中断模式与紧循环模式自适应切换"的参数化实现：没有 IO 时按指数退避降低 CPU，持续有
  IO 时转入忙轮询降低延迟。

  1.7 IOWatchDog：IO 卡死检测

  IOWatchDog 默认关闭（drive.io_watchdog_timer_on=false，注释警告开启会影响性能），开启后在每个 worker
  reactor 上注册一个周期性全局定时器（默认 300 秒一次）。核心数据结构是 std::map<uint64_t /*单调递增id*/,
  drive_iocb*> m_outstanding_ios：IO 提交时插入、完成时删除。由于 key 单调递增，map
  的遍历顺序天然等于提交时间顺序，io_timer() 检测超时（默认 60
  秒）时只需从头扫描，遇到第一个未超时的就可以 break，不必扫描全部在途 IO。一旦发现超时，直接
  RELEASE_ASSERT 让进程崩溃（fail-fast：宁可被监控系统重启，也不让 IO 静默挂死造成数据不一致）。

  ---

  2. Reactor：事件循环核心

  2.1 抽象基类 IOReactor

  最关键的设计是 static thread_local IOReactor* this_reactor：每个系统线程只认一个 reactor
  实例，几乎所有框架内代码（am_i_worker_reactor、iofiber_self()、schedule_thread_timer()……）都通过这个 TLS
  指针 O(1) 拿到"当前线程所属的 reactor"，不需要任何锁或查找表，是整个框架调用频率最高的原语之一。

  纯虚接口：listen()（每轮事件循环主体）、is_tight_loop_reactor()、init_impl()/stop_impl()（子类特定资源初
  始化/销毁）、put_msg()（安全地把消息投递到本
  reactor）、add_iodev_impl()/remove_iodev_impl()（注册/反注册 fd 到底层多路复用器）。

  run()：判断是否 USER_CONTROLLED_LOOP/ADAPTIVE_LOOP，设置 this_reactor，调用 init(num_fibers) 创建
  fiber、挂载接口，非用户控制循环时进入 while (listen_once()); 死循环。

  init()：创建一组 IOFiber（第 0 个"main fiber"直接复用 reactor
  所在的系统线程本身，不走协程切换；其余真正拉起协程跑 fiber_loop），调用子类 init_impl()，再遍历全局所有
  IOInterface，按作用域（all_io/all_worker/all_user）匹配后让各接口自行决定要往这个新 reactor
  加哪些设备（on_reactor_start）。

  listen_once()：调用虚函数 listen() 之外，还做三件事：
  1. 触发全局/本 reactor 的 sentinel 回调（每轮循环巡检，例如统计、watchdog 心跳打点）；
  2. 判断 need_backoff，空闲时按指数增长（乘 backoff_delay_increase_factor，封顶
     backoff_delay_max_us）主动 sleep_for 微秒级退避，避免自适应 reactor 空转烧 CPU；
  3. m_fiber_mgr_lib->yield_main()——保证每轮处理完事件后让出 CPU 给已就绪的 fiber，这是 run-to-completion
     模型里"批处理完必须调度协程"的体现。

  stop()：优雅停止——置 m_keep_running=false → yield 一次让其它 fiber 看到标志位 → 通知所有挂载接口
  on_reactor_stop → 给非 main fiber 推空消息把它们从阻塞 pop_msg() 唤醒退出 → 用 sisl::atomic_counter
  m_io_fiber_count 等所有 fiber 退出 → 调子类 stop_impl()（清理 epoll fd / spdk 资源）。

  pick_fiber(fiber_regex)：main_only（固定主 fiber）、syncio_only（随机挑一个专跑同步阻塞 IO 的
  fiber）、random、默认用 thread_local 游标做无锁 round-robin。

  2.2 EPoll Reactor 实现细节

  init_impl()：epoll_create1(0) 创建每线程私有的 epoll
  fd，彼此互不干扰、天然分片、无需跨线程锁；再创建一个 eventfd(EFD_NONBLOCK) 作为本 reactor 的"消息唤醒
  fd"（m_msg_iodev），注册 EPOLLIN；创建本线程的 timer_epoll。

  事件循环 listen()：
  num_fds = epoll_wait(m_epollfd, events, MAX_EVENTS /*20*/, get_poll_interval());

  get_poll_interval() 的返回值同时承担"事件等待超时"和"周期性轮询/心跳"两个职责，超时触发走
  idle_time_wakeup_poller()（处理消息队列 + 调用所有 register_poll_interval_cb
  注册的周期任务），避免额外起一个定时器线程。拿到事件后先按 iodev->priority()
  排序，再逐个分发——epoll_event.data.ptr 直接存 IODevice* 指针，回调直接 iodev->cb(iodev, iodev->cookie,
  event)，没有 fd→对象的 map 查找这一层，是关键的 O(1) 分发优化。

  fd 注册方式（add_iodev_impl）：
  ev.events = EPOLLET | EPOLLEXCLUSIVE | iodev->ev;
  ev.data.ptr = (void*)iodev.get();
  epoll_ctl(m_epollfd, EPOLL_CTL_ADD, iodev->fd(), &ev);
  - EPOLLET（边缘触发）：要求回调一次性把数据读/写到 EAGAIN，减少重复唤醒，是高吞吐网络库的标准做法；
  - EPOLLEXCLUSIVE（Linux 4.5+）：当多个 reactor 线程各自的 epoll 实例都监听同一个 fd（例如多线程 accept
    同一个监听 socket）时，内核只唤醒其中一个，避免惊群——这正是 README 所说"多 reactor
    负载均衡"在代码层面的落地，完全靠内核特性而不是应用层加锁分发。

  批处理陷阱与修复：如果正处于"本轮事件遍历中"（m_event_processing_phase），remove_iodev 不会立即释放
  IODevice，而是先放进 m_removed_iodevs，等整批 events[] 处理完才真正
  clear——因为同一批事件数组里可能还有同一个 fd 的后续事件，提前释放会导致悬空指针（UAF），这是一个典型的
  epoll 批处理正确性修复。

  空闲轮询 idle_time_wakeup_poller()：epoll_wait 超时时处理消息队列 +
  调用所有周期性回调，把"定时轮询"和"事件驱动"统一成一套循环。

  2.3 跨线程消息机制（iomgr_msg）

  消息对象本质是"一个待执行闭包 + 目标 fiber + 可选完成通知"：
  struct iomgr_msg {
      io_fiber_t m_dest_fiber{nullptr};
      run_func_t m_method;              // std::function<void()>
  };
  struct iomgr_waitable_msg : iomgr_msg {
      FiberManagerLib::Promise<bool> m_promise;  // run_on(wait=true,...) 用
  };

  投递快捷路径（deliver_msg）：
  if (iomanager.this_reactor() == this) handle_msg(msg);   // 同线程：直接同步执行
  else put_msg(msg);                                       // 跨线程：无锁队列 + eventfd
  若发送方和接收方恰好同一个线程，直接同步调用，跳过队列和系统调用——用"可能打乱消息严格顺序"换取显著的性能
  提升，是一处典型的正确性/性能取舍。

  跨线程路径 put_msg：
  folly::UMPSCQueue<iomgr_msg*, false> m_msg_q;  // 无锁 多生产者-单消费者 队列
  void put_msg(iomgr_msg* msg) {
      m_msg_q.enqueue(msg);
      if (!m_msg_handler_on.load(acquire))
          write(m_msg_iodev->fd(), &temp, 8);    // eventfd 唤醒
  }
  用 folly 的无锁 MPSC 队列承载跨线程投递；m_msg_handler_on
  原子标志做唤醒抑制——如果目标线程本来就在处理消息，新消息不需要再触发一次 eventfd
  write（系统调用），反正会在当前这轮消费循环里被处理掉。

  消费端 process_messages()：一次性从队列批量 dequeue 并执行，上限 message.max_msgs_before_yield（默认
  100），防止消息风暴饿死 IO 事件；达到上限且队列仍非空，就自己再往 eventfd 写一次，把剩余处理切到下一轮
  epoll_wait，保证公平调度。handle_msg 里如果目标是本 reactor 内某个具体 fiber（而非 reactor
  本身），则转发进该 fiber 的协程内 channel——形成"reactor 级跨线程队列 → fiber 级协程内通道"两级路由。

  2.4 IOFiber：协程式轻量线程

  这是 README 中"IO Thread"概念的落地：基于 stackful 协程（boost::fiber 或
  folly::fibers，编译期二选一），不是手写 ucontext，底层靠 boost.context
  的汇编级寄存器/栈切换，远比线程切换轻（无需陷入内核、无 TLB flush）。

  一个 reactor（系统线程）内可以跑多个 fiber，这些 fiber 共享同一个 OS 线程、协作式调度——同一时刻只有一个
  fiber 在执行，因此同一 reactor 内多个 fiber 访问共享数据结构完全不需要锁。这正是整个框架"无锁"设计的根本
  来源：不是靠精巧的无锁算法，而是靠协作式调度本身提供的互斥保证。

  每个非主 fiber 的主循环（fiber_loop）本质是"阻塞等消息 → 执行 → yield"：
  while (true) {
      if ((msg = fiber->pop_msg()) != nullptr) handle_msg(msg);
      if (!m_keep_running) break;
      m_fiber_mgr_lib->yield();
  }
  这些 fiber 通常被用作"同步阻塞 IO 专用协程"：某些操作必须同步阻塞调用时，可以 run_on 调度到这些 fiber
  上执行，不阻塞主 fiber 的事件循环。

  Boost 实现用 boost::fibers::buffered_channel（容量 1024）做消息通道，满了就先放溢出队列
  m_overflow_msgs，避免发送方阻塞；还自己手写了一个支持 writer-priority 的 fiber 级
  shared_mutex（boost.fiber 本身只有 mutex）。Folly 实现更简单：std::queue +
  folly::fibers::Baton。两套实现都包了一层统一的 Future/Promise API，上层调用方式与底层协程库解耦。

  调度器细节：自定义的 io_fiber_picker（实现 boost::fibers::algo::algorithm）目前在 reactor.cpp
  里实际被注释掉未启用（预留/实验代码），当前跑的是 boost.fiber
  默认调度器。其设计思路值得一提：suspend_until() 并不真正挂起线程等待，而是直接把 main fiber（跑
  epoll_wait 的那个）重新排回调度队列——保证"没有协程可跑时立刻把控制权还给主循环去 poll
  事件"，而不是像标准 round-robin 那样用条件变量真正睡眠。

  2.5 Timer（定时器）

  周期性定时器：每个独占一个 timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK)，直接加入 epoll
  set，复用事件驱动机制，实现简单、精度高（数量少，一个定时器一个 fd 的开销可接受）。

  一次性定时器：为避免"每个 IO 都创建一个新 fd + epoll_ctl"的高昂开销，所有一次性定时器共享一个公共
  timerfd，用**二项堆（boost::heap::binomial_heap）**管理（而非 std::priority_queue，因为需要支持 O(log n)
  的任意节点 erase 来取消定时器）。公共 fd
  的到期时间始终设为堆顶（最近到期）的那个，触发后批量弹出所有已到期的定时器执行回调。

  线程本地 vs 全局：线程本地定时器因为只有本线程访问，跳过锁；全局定时器（可能被多线程同时
  schedule/cancel）才真正加锁——又一次"能用线程局部就不用锁"。

  SPDK 定时器完全独立实现，基于 spdk_poller_register，用原子
  CAS（cur_term_num）保证"同一个全局周期任务广播到所有 worker reactor 时，只有一次真正执行回调"。

  epoll_wait 的超时值与 timer 是互补的两套机制：timer 靠独立 fd 精确触发，get_poll_interval()
  只是一个粗粒度兜底（watchdog心跳、退避轮询等不要求精确时间点的场景）。

  ---

  3. IODevice / IOInterface：设备与接口抽象

  3.1 IODevice

  代表被 reactor 监听的一个"端点"，用 std::variant<int, spdk_bdev_desc*, spdk_nvmf_qpair*> dev 统一表达 fd
  / SPDK bdev / NVMeoF qpair 三种后端类型（is_spdk_dev() 用 std::holds_alternative 判断）。关键字段：

  - ev_callback cb：事件回调；
  - io_fiber_t creator：创建者 fiber；
  - thread_specifier thread_scope：决定设备是"全局"（加到所有 reactor）还是"私有"（只属于某个
    fiber/reactor）；
  - sisl::sparse_vector<unique_ptr<IODeviceThreadContext>> m_iodev_fiber_ctx：每个 fiber
    一份该设备的线程本地上下文（例如 SPDK 的 io_channel），首次初始化用 m_ctx_init_mtx
    保护，之后无锁访问——典型"按需、每线程一份，初始化后无锁"模式；
  - sisl::atomic_counter<int32_t> thread_op_pending_count：跨 reactor 广播 add/remove
    操作时的"扇出-合并(fan-out/join)"计数器，归零触发完成回调。

  生命周期用 std::shared_ptr<IODevice> 管理。

  3.2 IOInterface 与跨线程安全加入 reactor

  一个 IOInterface 管理一类设备集合（一对多），std::unordered_map<backing_dev_t, io_device_ptr> +
  std::shared_mutex 保护。核心流程 add_io_device：

  - 若目标 scope 就是当前线程，直接 reactor->add_iodev(iodev)；
  - 否则不直接操作其他线程的 epoll 实例，而是通过 iomanager.run_on_wait/run_on_forget 把 add_to_reactor
    这个 lambda 作为消息投递到每个目标 reactor，由目标 reactor 自己在其事件循环里执行注册——这正是"每个
    reactor 只由自己的线程操作自身 epoll fd，线程间靠消息传递而非共享锁"的核心体现，彻底避免了跨线程操作
    epoll 实例引发的锁竞争和 cache line 争用。

  on_reactor_start/on_reactor_stop：新 reactor 启动或旧 reactor 停止时，遍历设备表批量加入/移除，支持
  reactor 动态增减。

  反复出现的模式：配置在主线程写一次，广播到各 reactor 落地为 thread_local
  变量，事件循环路径上零锁读取（如 GenericIOInterface 的 listen_sentinel_cb）。

  3.3 其它接口组件

  - HttpServer：基于 3rd_party 的 Pistache 实现，暴露 debug/metrics/管理 REST
    接口，自成一套独立线程池（serveThreaded()），与 IOManager 的 reactor 模型解耦，是 management-plane
    组件而非数据路径。
  - iomgr_flip.hpp：集成 eBay 内部 sisl::flip 故障注入框架（仅 _PRERELEASE 编译），定时器直接复用
    IOManager 自己的定时器子系统，test_and_abort 可在指定故障点直接 SIGKILL 模拟进程崩溃——混沌工程测试用。
  - SpdkNvmfInterface：每个线程维护独立的 spdk_nvmf_poll_group，qpair 加入哪个线程的 poll group
    后只由该线程 poll——与通用 fd "共享一个 epoll 实例"不同，SPDK 路径是每线程独立 poll group，天然无锁。

  ---

  4. DriveInterface：磁盘异步 IO 后端（libaio / io_uring）

  这是本项目实现高性能存储 IO 的另一个重点（除 SPDK 外）。

  4.1 抽象与请求对象 drive_iocb

  DriveInterface 定义 async_read/write/readv/writev/unmap/write_zero/queue_fsync/submit_batch（返回
  folly::Future<std::error_code>，都带 part_of_batch 参数支持批量提交）和对应的同步接口。

  请求对象 drive_iocb 字段设计紧凑：
  - std::variant<io_interface_comp_cb_t, folly::Promise<...>, FiberManagerLib::Promise<...>>
    completion：用 variant 统一表达三种完成通知方式，避免虚函数和额外堆分配；
  - std::variant<inline_iov_array(内联4个iovec), large_iov_array(堆分配), char*>
    user_data：小对象内联优化，绝大多数 IO 是单 buffer 或 ≤4 个 iovec，完全不需要额外堆分配，只有超过 4
    个才 new iovec[]；
  - 构造时记录 initiating_reactor =
    iomanager.this_reactor()，完成回调要切回发起线程执行，避免跨线程访问用户数据。

  update_iovs_on_partial_result()：遇到 partial read/write 时，从 iovec
  数组尾部反向计算剩余未完成部分，重新构造更短的 iovec 继续提交，而非重新发起整个请求。

  注：drive_iocb 本身是直接 new/delete，没有用专门的对象池——相比 SPDK
  路径（sisl::ObjectAllocator）这里相对不够极致，依赖 glibc/tcmalloc 等分配器的线程局部 cache 兜底。

  4.2 KernelDriveInterface 公共逻辑

  libaio 和 uring 的共同基类：定义 max_buf_size=1MB；同步 IO 走 pread/pwrite/preadv/pwritev +
  短读写重试循环；get_dev_size 区分文件（fstat）和块设备（ioctl BLKGETSIZE64）；get_attributes 默认 4096
  页大小/512 对齐，HDD 还会查多流写入通道数；写零优先用 ioctl
  BLKZEROOUT（纯内核操作不传数据），否则退化为预分配的全零缓冲区批量 writev。

  顶层 detect_drive_type 用 /proc/mounts + /sys/dev/block/<maj:min>/queue/rotational
  判断文件/块设备、HDD/SSD，结果缓存；get_iface_for_drive 综合内核能力和是否 SPDK 模式选择具体后端。

  4.3 AioDriveInterface（Linux libaio）

  每 reactor 独立 io_context（thread_local aio_thread_context）：
  m_ev_fd = eventfd(0, EFD_NONBLOCK);
  io_setup(MAX_OUTSTANDING_IO /*200*/, &m_ioctx);
  各线程的 io_context + eventfd 彼此无锁无竞争。eventfd 被注册进本线程 epoll，libaio 完成事件触发 eventfd
  → epoll 唤醒 → 读空计数 → io_getevents(ioctx, 0, MAX_COMPLETIONS, events, NULL)
  非阻塞一次性捞出所有完成事件。

  批量提交：drive_aio_iocb 先攒进 m_iocb_batch（上限 4 个），攒满后 io_submit(m_ioctx, n, iocbs)
  一次系统调用提交多个，减少系统调用次数。

  背压与自适应轮询间隔：超过 MAX_OUTSTANDING_IO(200)
  的请求进待发队列并设置一次性定时器稍后重试，而非忙等或阻塞；s_poll_interval_table 根据当前在途 IO
  数量动态调整 reactor 的 epoll_wait 超时——在途 IO 多时趋向 0 超时（忙轮询降延迟），在途 IO
  少时允许更长超时（省 CPU）。这是"中断模式"与"轮询模式"的自适应切换。

  完成时 io_set_eventfd(kiocb, m_ev_fd) 把每个 iocb 和 eventfd 绑定，内核 iocb 的 data
  字段直接嵌用户指针，完成时强转还原——零查找表，O(1)。

  局限：MAX_OUTSTANDING_IO=200 硬编码上限；async_unmap、异步 fsync、async_write_zero 均未实现（libaio
  原生不支持异步 fsync），这也是引入 io_uring 后端的动机之一。

  4.4 UringDriveInterface（io_uring，更优后端）

  每线程独立 Ring + eventfd：
  io_uring_queue_init(uring_per_thread_qdepth, &m_ring, 0 /* flags，未用SQPOLL */);
  int ev_fd = eventfd(0, EFD_NONBLOCK);
  io_uring_register_eventfd(&m_ring, ev_fd);
  没有使用 IORING_SETUP_SQPOLL（内核轮询线程模式），而是把完成事件通过注册的 eventfd 接入已有 epoll
  循环——让 io_uring 无缝融入现有 reactor 模型，不需要专门起轮询线程。此外还注册了一个"哨兵回调"每轮
  reactor 循环都主动 poll 一次 CQ，降低依赖 eventfd 通知带来的延迟。

  提交与背压：get_sqe_or_enqueue 若超过队列深度/SQ 已满则先排队或先提交清空；can_submit() 保证 在途+已准备
  <= qdepth，防止 CQ 溢出。

  批量提交：part_of_batch=true 时只是设置 SQE 数据暂不提交，外部显式 submit_batch() 统一一次
  io_uring_submit，摊销系统调用成本。

  完成处理：用 io_uring_peek_cqe（非阻塞轮询，而非阻塞等待）在循环里一次性取完所有 CQE；检测 koverflow（CQ
  溢出）视为严重故障直接抛异常；io_uring_cqe_get_data 拿回 drive_iocb*，同样 O(1)
  无查找表还原。部分读写不是失败，而是调用 update_iovs_on_partial_result 续传剩余部分；EAGAIN
  无限重试，其它错误超过 max_resubmit_cnt 才判定失败。

  "同步"接口的真异步实现（亮点）：sync_write/sync_read 并非阻塞系统调用，而是把调用方所在的 fiber
  挂起（FiberManagerLib::Promise/Future），IO 完成后在 handle_completions 中唤醒该
  fiber——线程本身不阻塞，可以继续调度其他 fiber。只有当前线程不支持 fiber
  调度或队列已满时，才真正退化为阻塞的 pwrite。

  相比 AIO 的优势：原生支持异步 fsync、readv/writev；获取完成事件 io_uring_peek_cqe
  不需要系统调用（用户态环形缓冲区直接读），只有提交才需要系统调用且可批量；没有 libaio 的 200
  这种硬限制，队列深度可配；原生支持部分读写精细重试。未使用的高级特性：IORING_SETUP_SQPOLL、fixed
  buffers/fixed files 均未启用，说明当前实现更偏向"稳妥兼容各内核版本"而非榨干 io_uring
  的全部性能潜力，是可进一步优化的空间。

  4.5 后端选型逻辑

  if (spdk_mode && 非HDD)                    -> spdk
  else if (uring_capable && !spdk_mode)      -> uring
  else                                        -> aio (兜底)
  三种后端可以在同一进程内共存（如 HDD 走 aio/uring，NVMe 走
  spdk），由设备类型探测结果和内核能力共同决定。

  ---

  5. SPDK 集成：极致性能的 Tight-Loop 模式

  5.1 Tight Loop Reactor

  IOReactorSPDK::listen()：
  for (auto& fiber : m_io_fibers) spdk_thread_poll(fiber->spdk_thr, 0, 0);
  for (auto& thr : m_external_spdk_threads) spdk_thread_poll(thr, 0, 0);
  没有任何阻塞/睡眠调用，纯粹的 busy-polling；spdk_thread_poll 驱动该 spdk_thread 上注册的所有
  poller（包括 bdev/nvme 完成队列）和消息队列。

  CPU 绑核：spdk_env_thread_launch_pinned(lcore, ...) 把 reactor 线程物理 pin 到独占 lcore；全局
  s_core_assignment（set+mutex，仅在启动阶段一次性分配时用到）保证不会重复分配同一 core。若检测到 cgroup
  cpuset 未隔离（读 cpu.cfs_quota_us 等文件判断），会退化为非绑核的普通线程并打印严重警告——SPDK busy-poll
  线程若不独占核心，与其它线程抢占调度可能导致饥饿式死锁，这是生产部署的重要约束。

  5.2 SPDK Thread ↔ IO Thread(fiber) 映射

  每个 reactor 内的 fiber 都一一对应创建一个 spdk_thread（存于 fiber->spdk_thr），并把该 spdk_thread 的
  cpuset 锁定到所在系统线程的同一个 core，避免跨核 cache bouncing。跨线程消息投递复用 SPDK
  自己的无锁消息环（spdk_thread_send_msg，底层是 DPDK 的 lock-free ring），不需要 IOManager
  自己加锁。外部产生的 spdk_thread（第三方库自建）通过 round-robin 被"收养"进某个 reactor
  一并轮询，不会出现孤儿线程。

  5.3 内存池与零拷贝

  SpdkMemPool 按 2 的幂次大小维护一组 spdk_mempool*（底层是 DPDK rte_mempool，预先从 hugepage
  批量分配固定大小对象，分配/释放走 per-lcore cache，正常路径无锁）。SpdkAlignedAllocImpl
  把这套机制接入通用对齐分配器接口：
  - aligned_alloc/free 走 spdk_malloc(..., SPDK_MALLOC_DMA)——分配DMA 可用（物理地址连续、可直接给 NVMe
    控制器 DMA）的内存，应用层和内核态之间不存在数据拷贝；
  - 高频小块分配走固定大小 mempool，避免每次 malloc 的系统调用与锁开销。

  启动时按配置预创建多个 mempool，并把 sisl::AlignedAllocator::instance() 整体切换为 SPDK 分配器——上层所有
  IO buffer 自动变成 DMA-safe、hugepage-backed，为零拷贝创造条件。

  5.4 SpdkDriveInterface 对接 bdev

  每线程独立 IO channel（核心无锁设计）：设备被加入某个 tight-loop reactor 时，为该 reactor 下每一个
  fiber单独调用 spdk_bdev_get_io_channel()，存入设备的按-fiber-ordinal
  索引的稀疏数组（锁只保护"数组扩容"这一步，槽位分配完成后访问零竞争）。这是 SPDK "每线程独立 I/O channel"
  模式：同一个 bdev 被多核并发访问时，各自用独立 channel（对应独立 NVMe
  队列对或独立软件队列），完全不需要跨核锁。

  提交路径按调用线程类型走三条路径：
  1. 本身就是 spdk reactor 线程 → 直接同步调用 submit_io，零额外跳转；
  2. 非 spdk 线程 → 通过 run_on_forget(least_busy_worker, ...) 把请求投递给最空闲的 spdk worker
     reactor——这是 epoll 线程想发起块设备 IO 时的桥接路径；
  3. batch 模式 → thread_local 攒批，攒够后一次性投递消息批量提交，减少跨线程消息投递次数。

  submit_io 调用 spdk_bdev_read/write(v)/unmap/write_zeroes，立即返回，真异步不阻塞 reactor；若返回
  -ENOMEM（资源不足），不是失败而是 spdk_bdev_queue_io_wait 把请求挂到 bdev 等待队列，资源可用时 SPDK
  自动回调重试——背压/流控机制而非简单失败重试。

  完成路径：spdk_thread_poll()（即 listen()）轮询到完成后触发回调，失败先尝试有限次自动重试，否则通过
  std::visit 分发到三种可能的完成 sink（folly::Promise/FiberManagerLib::Promise/普通回调），最后对象池
  sisl::ObjectAllocator<SpdkIocb>::deallocate 归还——整条链路没有一次阻塞等待，纯事件驱动。

  5.5 SPDK 与 Epoll 的互斥与协同

  - is_iodev_addable() 限定只有 spdk 设备才能加入 spdk reactor，普通 fd 不会路由过去；
  - IOManagerEpollImpl/IOManagerSpdkImpl 互斥（一个 IOManager 实例启动时只选一种作为 worker 实现），对应
    README"worker reactor 不能混用"；
  - 协同点正是上面"非 spdk 线程发起 IO"的桥接路径——epoll reactor 处理网络事件，一旦需要落盘
    IO，就通过统一的 iomgr_msg 机制甩给 spdk worker reactor 处理，两种 reactor
    通过消息机制解耦协作，而不是直接函数调用或共享锁。

  5.6 高性能证据汇总（SPDK 路径）

  - 零拷贝：DMA-safe hugepage 内存 + spdk_bdev_readv/writev 直接传用户 iovec；
  - 无锁：SPDK 自带的 lock-free 消息环、DPDK mempool 的 per-lcore cache、每 fiber 独立 io channel；
  - CPU 独占轮询：系统线程绑核 + spdk_thread 绑同一核双重绑定，彻底消除中断/上下文切换/epoll_wait
    系统调用开销，代价是独占 CPU；

  ---

  6. 全书母题的代码级对照表

  ┌────────────────────────────┬──────────────────────────────────────────────────────────────────────┐
  │          设计理念          │                               代码落地                               │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ 每线程私有状态，无共享     │ 每 reactor 独立 epoll fd / io_context / io_uring ring                │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ 协作式调度代替锁           │ 同一 reactor 内多个 fiber 互斥执行，访问共享结构天然无竞争           │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ 无锁跨线程通信             │ folly UMPSCQueue（reactor间）、SPDK lock-free ring（spdk             │
  │                            │ thread间）、boost channel/folly Baton（fiber内）                     │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ 唤醒抑制                   │ m_msg_handler_on 原子标志，避免重复 eventfd write                    │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ 批处理防饿死               │ max_msgs_before_yield、epoll 一轮最多处理 MAX_EVENTS=20              │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ 惊群优化                   │ EPOLLEXCLUSIVE                                                       │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ 边缘触发减少唤醒           │ 固定 EPOLLET                                                         │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ 指针直达，O(1) 分发        │ epoll_event.data.ptr/内核 iocb data/io_uring CQE user_data           │
  │                            │ 都直接存用户指针                                                     │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ CPU                        │ SPDK 绑核 + busy loop                                                │
  │ 独占轮询（低延迟高吞吐）   │                                                                      │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ 自适应退避（省 CPU）       │ epoll poll.backoff_* 指数退避 + 在途IO自适应超时                     │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ CPU                        │ SPDK 绑核 + busy loop                                                │
  │ 独占轮询（低延迟高吞吐）   │                                                                      │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ 自适应退避（省 CPU）       │ epoll poll.backoff_* 指数退避 + 在途IO自适应超时                     │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ 批量系统调用               │ io_submit/io_uring_submit 批量提交                                   │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ 零拷贝                     │ SPDK DMA hugepage 内存                                               │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ 小对象内联/对象池          │ iovec≤4内联、sisl::ObjectAllocator、DPDK mempool                     │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ 能力一次探测、热路径零分支 │ io_uring 能力探测、设备类型探测均缓存                                │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ 无锁 barrier/计数器        │ sisl::atomic_counter::decrement_testz() 驱动启停状态机               │
  ├────────────────────────────┼──────────────────────────────────────────────────────────────────────┤
  │ fail-fast                  │ IOWatchDog 检测到 IO 卡死直接 RELEASE_ASSERT 崩溃                    │
  └────────────────────────────┴──────────────────────────────────────────────────────────────────────┘

  7. 小结

  IOManager 的高性能来源不是单一黑科技，而是分层一致的设计原则：能用线程本地状态就不共享，能用协程协作互斥
  就不用锁，能批量系统调用就不单发，能绑核忙轮询就不依赖中断，能缓存探测结果就不在热路径判断。epoll
  路径用自适应退避在吞吐和 CPU 占用间取平衡，SPDK 路径则完全放弃这种平衡、用独占 CPU
  换取极致的低延迟高吞吐——而两条路径通过统一的 Reactor / IOFiber / IODevice / iomgr_msg
  抽象共存协作，使得应用可以按设备类型（网络用 epoll、NVMe 用 SPDK）自由组合，这正是 README
  里"桥接两种编程模型"这句话在代码层面的真正含义。