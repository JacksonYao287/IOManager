# IOManager 设计与实现详解

IOManager 是一个面向 C++ 存储、数据库、KV 引擎等高并发场景的**统一异步 I/O 运行时**。它试图解决的问题不是“如何异步读写一个文件”这么简单，而是：

> 在同一个应用内，让网络 socket、普通 fd、计时器、内核异步块 I/O、`io_uring`、SPDK 用户态 NVMe I/O 等事件，能够在统一的 reactor / 消息 / fiber 模型中协作；同时允许在低 CPU 占用的中断模式与极低延迟的忙轮询模式间选择或混用。

其设计动机和架构概述写在 [`README.md`](../README.md)；核心实现位于 [`src/lib`](../src/lib)，公开 API 位于 [`src/include/iomgr`](../src/include/iomgr)。

---

## 1. 要解决的核心矛盾

传统服务端程序经常采用如下模型：

```text
网络事件线程
  -> 收到请求
  -> 投递到线程池
  -> 线程池中用阻塞 read/write/pread/pwrite
  -> 处理完成后回到网络线程
```

这很通用，但会有代价：

1. **线程上下文切换**：网络线程与 I/O 线程之间反复切换。
2. **缓存与 NUMA 局部性变差**：请求数据、连接状态和 I/O 状态在不同 CPU 核间迁移。
3. **同步 I/O 阻塞线程**：大量慢盘或高延迟设备下，需要很多线程才能维持并发。
4. **调度和锁竞争成本**：共享线程池往往带来工作队列竞争、锁、唤醒和负载不均。
5. **网络和存储的事件循环分裂**：网络可由 `epoll` 管理，但传统磁盘 I/O 常被丢给额外线程池。

IOManager 主张的替代模型是 **run-to-completion（请求在同一 reactor 逻辑上下文内一路处理到完成）**：

```text
Reactor 收到网络/消息/定时器/I/O 完成事件
  -> 运行请求逻辑
  -> 发起真正的异步存储 I/O
  -> 当前轻量协程让出执行权
  -> I/O 完成后回到 reactor / fiber
  -> 完成请求
```

关键不在于“绝不切线程”，而是尽量做到：

- 请求状态和设备状态保持在同一个逻辑执行域；
- 慢 I/O 不阻塞 OS 线程；
- 跨 reactor 时使用显式消息传递，而不是任意共享状态；
- 高负载时维持高队列深度，减少 syscall、调度、拷贝和锁竞争。

---

## 2. 分层架构

可以把 IOManager 分为六层：

```text
应用层
  │
  ├── IOManager / IOEnvironment 生命周期、线程和 reactor 管理
  ├── 消息投递、reactor 选择、计时器、内存分配
  │
  ├── IOReactor 抽象
  │     ├── IOReactorEPoll：epoll + eventfd + timerfd
  │     └── IOReactorSPDK：polling + SPDK thread
  │
  ├── Fiber 层
  │     ├── 一个 reactor 内多个逻辑 I/O fiber
  │     └── Boost.Fiber 或可选 Folly Fiber
  │
  ├── IOInterface / IODevice 抽象
  │     ├── GenericIOInterface：普通 fd、事件 fd、timer fd
  │     └── DriveInterface：块/文件 I/O
  │
  └── 存储后端
        ├── AioDriveInterface：Linux libaio
        ├── UringDriveInterface：io_uring
        └── SpdkDriveInterface：SPDK bdev / NVMe
```

关键公开类型可见：

- [`iomgr.hpp`](../src/include/iomgr/iomgr.hpp)
- [`iomgr_types.hpp`](../src/include/iomgr/iomgr_types.hpp)
- [`io_interface.hpp`](../src/include/iomgr/io_interface.hpp)
- [`io_device.hpp`](../src/include/iomgr/io_device.hpp)
- [`drive_interface.hpp`](../src/include/iomgr/drive_interface.hpp)

---

## 3. 核心对象与职责

### 3.1 `IOManager`：全局运行时与控制平面

`IOManager` 是 singleton：

```cpp
#define iomanager iomgr::IOManager::instance()
```

定义于 [`iomgr.hpp`](../src/include/iomgr/iomgr.hpp)，实现主要在 [`iomgr.cpp`](../src/lib/iomgr.cpp)。

它负责：

- 生命周期：`start()` / `stop()`；
- 创建 worker reactor 和 user reactor；
- 管理接口列表 `m_iface_list`；
- 管理 drive 接口列表 `m_drive_ifaces`；
- 向 reactor / fiber 路由消息；
- 创建全局和本地 timer；
- 对接内存分配器；
- 选择 AIO、io_uring 或 SPDK I/O 后端；
- 暴露当前 reactor、当前 fiber、同步 I/O 是否安全等上下文信息。

其状态机是：

```text
stopped
  -> interface_init
  -> reactor_init
  -> sys_init
  -> running
  -> stopping
  -> stopped
```

这不是简单的状态标记。它避免了几个典型启动/停止竞争：

- reactor 尚未初始化，接口就开始注册设备；
- worker reactor 尚未全部启动，全局 timer 就开始向其投递；
- stop 时 timer、接口、reactor 的资源释放顺序不正确；
- 应用在 manager 尚未 ready 前发起跨 reactor 操作。

### 3.2 `IOReactor`：一个 OS 线程上的事件循环

`IOReactor` 是抽象基类，定义于 [`reactor.hpp`](../src/lib/reactor/reactor.hpp)。

一个 reactor 对应一个系统线程，并拥有：

- reactor ID；
- worker / user reactor 属性；
- 本线程 timer；
- 一个或多个 fiber；
- I/O 设备集合；
- 指标和统计信息；
- poll interval；
- backoff 回调；
- 具体事件循环实现。

核心接口：

```cpp
virtual void listen() = 0;
virtual void init_impl() = 0;
virtual void stop_impl() = 0;
virtual void put_msg(iomgr_msg* msg) = 0;
virtual int add_iodev_impl(...) = 0;
virtual int remove_iodev_impl(...) = 0;
```

因此，IOManager 的上层逻辑基本不需要关心底层是 `epoll_wait`、`io_uring` completion、SPDK poller，或未来的新 reactor 实现。

#### Reactor 生命周期

`IOReactor::run()` 的主要流程在 [`reactor.cpp`](../src/lib/reactor/reactor.cpp)：

1. 初始化当前线程的 `thread_local IOReactor::this_reactor`；
2. 设置 loop 类型、轮询间隔、adaptive backoff 参数；
3. 创建 fiber；
4. 初始化具体 reactor 后端；
5. 通知所有已经注册的 `IOInterface`；
6. 进入 `listen_once()` 循环；
7. 停止时卸载接口、唤醒和回收 fiber、清理 reactor 后端。

主循环概念上是：

```cpp
while (listen_once()) {
}
```

而一次 `listen_once()` 的行为是：

```text
底层 listen()
  -> 分发事件 / 完成回调 / 消息
  -> 运行 sentinel 回调
  -> 判断是否应 adaptive backoff
  -> main fiber 主动 yield，让其他 fiber 有机会运行
  -> 必要时 sleep 一小段时间
```

这体现出 IOManager 的核心思想：**一个 reactor 线程不是只做事件等待，也不是只做用户代码，而是持续协调事件源、消息、fiber 与 I/O completion。**

### 3.3 Worker Reactor 与 User Reactor

README 对这两者有说明，见 [`README.md`](../README.md)。

#### Worker reactor

由 IOManager 启动和管理：

```cpp
iomanager.start({
    .num_threads = N,
    .is_spdk = false,
    .num_fibers = F
});
```

- `is_spdk == false`：worker 是 epoll reactor；
- `is_spdk == true`：worker 是 SPDK tight-loop reactor；
- 由 `m_worker_reactors` 保存；
- 可被 `random_worker`、`least_busy_worker`、`all_worker` 等路由规则选中。

#### User reactor

应用也可以让某线程成为 reactor：

```cpp
iomanager.run_io_loop(INTERRUPT_LOOP);
```

或让 IOManager 创建一个新 reactor 线程：

```cpp
iomanager.create_reactor("user1", INTERRUPT_LOOP);
```

User reactor 的意义是：

- 应用可以控制线程的角色；
- 某类连接、分片、租户或分区可固定到指定 reactor；
- 可将应用自己的事件循环逻辑与 IOManager 融合；
- 不必把所有任务都塞给固定 worker 池。

---

## 4. 两种 reactor：中断模式与忙轮询模式

### 4.1 `IOReactorEPoll`：中断驱动模式

实现位于：

- [`reactor_epoll.hpp`](../src/lib/epoll/reactor_epoll.hpp)
- [`reactor_epoll.cpp`](../src/lib/epoll/reactor_epoll.cpp)

每个 epoll reactor 拥有：

```cpp
int m_epollfd;
io_device_ptr m_msg_iodev;
folly::UMPSCQueue<iomgr_msg*, false> m_msg_q;
```

即：

- 一个独立的 `epoll` 实例；
- 一个用于跨线程消息唤醒的 `eventfd`；
- 一个多生产者、单消费者消息队列；
- 普通 fd、timerfd 和消息 fd 都在同一 epoll 集合内。

#### `listen()` 的执行过程

1. 调用 `epoll_wait(m_epollfd, ..., get_poll_interval())`；
2. 若超时，调用 `idle_time_wakeup_poller()`，处理消息并触发 poll interval 回调，例如轮询 AIO completion；
3. 若有事件，按 `IODevice::priority()` 排序；
4. 区分消息 fd、timer fd、普通用户 fd、AIO / io_uring completion eventfd；
5. 普通设备通过下列 callback 分发：

   ```cpp
   iodev->cb(iodev, iodev->cookie, event);
   ```

这种结构的好处是网络、timer、消息、AIO completion 都能放到一个线程的统一事件循环中处理。

#### Edge-triggered 与 `EPOLLEXCLUSIVE`

注册 fd 时使用：

```cpp
ev.events = EPOLLET | EPOLLEXCLUSIVE | iodev->ev;
```

意图是：

- `EPOLLET`：边沿触发，减少同一“就绪状态”重复通知；
- `EPOLLEXCLUSIVE`：多个 epoll waiters 监听相同 fd 时，减少惊群。

边沿触发要求 callback 严格读/写至 `EAGAIN`，否则可能漏掉后续数据；这是接口使用者必须遵守的约束。

#### 删除设备时的 UAF 防护

在 epoll event batch 正被遍历时，如果删除一个 `IODevice` 并立即释放，batch 中后续事件可能还持有旧的 `data.ptr`，会造成 use-after-free。

因此，`IOReactorEPoll` 在事件处理阶段把待删除设备暂存在：

```cpp
std::unordered_map<IODevice*, shared<IODevice>> m_removed_iodevs;
```

本轮 event batch 结束后才清空。这是一项很实际的并发安全设计。

### 4.2 `IOReactorSPDK`：忙轮询、低延迟模式

实现位于：

- [`reactor_spdk.hpp`](../src/lib/spdk/reactor_spdk.hpp)
- [`reactor_spdk.cpp`](../src/lib/spdk/reactor_spdk.cpp)

它不调用 `epoll_wait`，而是在每次循环中不断：

```cpp
spdk_thread_poll(fiber->spdk_thr, 0, 0);
```

特点：

- 不睡眠；
- 不依赖内核中断来唤醒；
- 每个 reactor 持续主动检查 SPDK 消息、poller、I/O completion；
- 典型使用场景是绑定 CPU 核的 NVMe/SPDK 高性能路径。

优势：

- 延迟低；
- 不需要内核 wakeup；
- 不涉及 `epoll_wait`、中断和频繁调度；
- reactor、SPDK thread、I/O channel 的关系更稳定；
- CPU cache 局部性更好。

代价：

- 空闲也消耗一个 CPU 核；
- 通常需要 CPU pinning；
- 核数不足或不固定绑核时性能和稳定性都会受影响；
- 不适合作为所有业务线程的默认模型。

SPDK 初始化时会检测 CPU pinning，并为 tight-loop worker 分配独立 lcore，见 [`iomgr_impl_spdk.cpp`](../src/lib/spdk/iomgr_impl_spdk.cpp)。

---

## 5. Fiber：在一个 reactor 线程中实现逻辑并发

### 5.1 为什么需要 fiber

一个 reactor 只有一个 OS 线程。如果业务在 reactor 上调用同步接口：

```cpp
sync_read(...)
```

若这个调用真的阻塞 OS 线程，则整个 reactor 的网络、消息、timer 和其他 I/O 都会停住。

IOManager 的解决方案是：

- 一个 reactor 运行多个协作式 fiber；
- main fiber 负责 reactor event loop；
- 非 main fiber 可执行“看似同步”的 I/O；
- 等待 future 时只挂起当前 fiber，不阻塞 OS 线程；
- reactor 继续驱动其他 fiber、其他 I/O 和 completion。

抽象定义于 [`fiber_lib.hpp`](../src/include/iomgr/fiber_lib.hpp)。默认实现使用 Boost.Fiber；也可以用 `USE_FOLLY_FIBER` 编译为 Folly Fiber。

### 5.2 Fiber 的结构

`IOFiber` 包含：

```cpp
IOReactor* reactor;
spdk_thread* spdk_thr;
uint32_t ordinal;
```

其中：

- `reactor`：所属 reactor；
- `spdk_thr`：SPDK 模式下，fiber 对应的 SPDK thread；
- `ordinal`：全局唯一 fiber 编号，用于设备 per-fiber context。

每个 reactor 初始化时创建 `num_fibers` 个 fiber：

```cpp
for (...) {
    m_io_fibers.emplace_back(...);
    if (i == 0) {
        // main fiber
    } else {
        // sync I/O fiber
        start_iofiber(...)
    }
}
```

设计约定是：

- fiber 0 是 **main fiber**，承载 reactor 主循环；
- fiber 1..N-1 是 **sync-I/O capable fibers**；
- 不能在 main fiber 中执行会等待的 `run_on_wait()` 或同步 I/O，否则容易形成 event-loop deadlock；
- `IOManager::am_i_sync_io_capable()` 用于判断当前上下文是否允许等待。

### 5.3 Fiber 消息通道

Boost 实现中，每个 fiber 有：

```cpp
boost::fibers::buffered_channel<iomgr_msg*> channel;
std::queue<iomgr_msg*> m_overflow_msgs;
```

默认 channel 容量为 1024。满了以后消息放到 overflow queue，而不是直接失败：

```cpp
if (status == full) {
    m_overflow_msgs.push(msg);
}
```

这避免了瞬时消息高峰导致丢消息，但 overflow queue 是无界的；持续过载时，内存可能增长。因此，使用者仍应实现业务层限流和背压。

---

## 6. 跨线程消息模型

IOManager 不鼓励应用线程任意直接访问其他 reactor 的状态，而是通过 `iomgr_msg` 路由。

消息结构定义在 [`iomgr_msg.hpp`](../src/include/iomgr/iomgr_msg.hpp)：

```cpp
struct iomgr_msg {
    io_fiber_t m_dest_fiber;
    std::function<void()> m_method;
};
```

支持两类模式：

```cpp
run_on_forget(...)
run_on_wait(...)
```

### 6.1 `run_on_forget`：异步投递

例如：

```cpp
iomanager.run_on_forget(
    reactor_regex::random_worker,
    [] {
        // 在目标 reactor 中执行
    });
```

适用场景：

- 发起异步 I/O；
- 跨线程迁移工作；
- 广播配置；
- 不需要立即得到结果。

### 6.2 `run_on_wait`：投递后等待

它使用 fiber-aware `Promise/Future`：

```text
iomgr_waitable_msg
  -> Promise<bool>
  -> 目标执行完成后 setValue(true)
  -> 调用方 future.get()
```

但它明确禁止在 reactor main fiber 里等待，避免：

```text
main fiber 等待目标 reactor 或当前 reactor 的消息
  -> main fiber 不再执行 event loop
  -> 消息/完成事件无法被消费
  -> 死锁
```

这是一个非常重要的 API 使用规则。

### 6.3 Reactor 选择策略

`reactor_regex` 提供：

- `all_io`；
- `all_worker`；
- `all_user`；
- `all_tloop`；
- `random_worker`；
- `least_busy_worker`；
- `least_busy_user`；
- `least_busy_io`。

`least_busy_*` 根据 reactor 的：

```cpp
m_metrics->outstanding_ops
```

做近似选择。这是一个低成本、粗粒度的负载均衡策略，不是严格的排队论调度器。它没有纳入队列长度、CPU 使用率、设备队列深度、P99 latency、消息队列积压或跨 NUMA 代价。

---

## 7. `IOInterface` 与 `IODevice`：设备抽象

### 7.1 `IOInterface`

`IOInterface` 是一种“设备类型/后端”的抽象：

```cpp
virtual int add_io_device(...);
virtual int remove_io_device(...);
virtual void close_dev(...);
```

它维护：

```cpp
std::unordered_map<backing_dev_t, io_device_ptr> m_iodev_map;
reactor_regex m_thread_scope;
```

一个 interface 可以按 scope 注册给所有 reactor、所有 worker 或所有 user reactor。

接口被加入 manager 后，会在每个适用 reactor 启动时调用：

```cpp
iface->on_reactor_start(reactor);
```

该函数会把已存在的设备逐个添加到 reactor。

### 7.2 `IODevice`

`IODevice` 是被 reactor 监听或被 I/O 后端管理的具体设备。

它可以承载：

```cpp
std::variant<int, spdk_bdev_desc*, spdk_nvmf_qpair*> dev;
```

因此同一抽象可代表：

- 普通 file descriptor；
- socket；
- eventfd；
- timerfd；
- SPDK bdev；
- SPDK NVMe-oF qpair。

还包含回调 `cb`、用户 cookie、设备优先级、所属 scope、设备类型 `dtype`、生命周期 `ready`、每 fiber 的设备上下文和设备级 metrics。

### 7.3 全局设备与本地设备

`thread_specifier` 是：

```cpp
std::variant<reactor_regex, io_fiber_t>
```

因此设备可以是：

#### 全局设备

```text
scope = reactor_regex::all_io / all_worker / all_user
```

设备会被注册到范围内所有 reactor。典型例子包括普通网络 fd、全局 timer、SPDK bdev。

#### 本地设备

```text
scope = 某个 io_fiber_t
```

设备只属于指定 reactor / fiber。典型例子包括每个 reactor 自己的 eventfd、每线程 AIO completion eventfd、每线程 io_uring completion eventfd 与本线程 timerfd。

这一设计的关键价值是：

> 全局对象可通过消息广播完成注册；局部对象则不必承担跨线程共享、锁竞争和错误的所有权转移。

---

## 8. Epoll 消息唤醒与队列设计

跨线程向 epoll reactor 发送消息时，流程是：

```text
生产者线程
  -> UMPSCQueue enqueue(msg)
  -> 若当前没有消息处理者：write(eventfd, 1)
  -> 目标 reactor 的 epoll_wait 返回
  -> 读取 eventfd
  -> drain message queue
```

### 为什么有 `m_msg_handler_on`

如果每个生产者都对 eventfd 写一次，在高并发投递下会产生大量 syscall 和 eventfd 竞争。

所以实现采用：

```cpp
if (!m_msg_handler_on.load(...)) {
    write(eventfd, 1);
}
```

当 reactor 已经在 drain queue 时，新的生产者只入队，不再额外唤醒。这是典型的**队列 + doorbell 合并**优化。

### 为什么设置最大 message batch

处理消息时受配置项限制：

```text
message.max_msgs_before_yield = 100
```

达到上限且队列仍非空时，会重新写 eventfd 并退出本轮处理：

```text
避免无限处理消息
  -> 饿死 socket、timer、storage completion 等其他事件
```

这是公平性与吞吐量之间的折中：

- batch 太小：频繁唤醒和调度，吞吐较低；
- batch 太大：消息洪峰会造成 I/O 或 timer 饥饿；
- 默认 100 是经验值，实际应压测调节。

---

## 9. 定时器设计

定时器接口见 [`iomgr_timer.hpp`](../src/include/iomgr/iomgr_timer.hpp)。

支持：

| 范围 | 单次 | 周期性 |
|---|---:|---:|
| 当前 reactor/fiber | 支持 | 支持 |
| 全局 worker/user 范围 | 支持 | 支持 |

### 9.1 Epoll 下的单次 timer：共享 timerfd + 最小堆

单次 timer 可能非常频繁，例如 I/O retry、request deadline、超时控制和延迟批处理。

若每个单次 timer 都创建一个 `timerfd` 并注册到 epoll，会有显著系统调用和 fd 管理成本。因此 IOManager 使用：

```text
一个公共 timerfd
  + 一个按 expiry_time 排序的 binomial heap
```

对应：

```cpp
timer_heap_t m_timer_list;
std::shared_ptr<IODevice> m_common_timer_io_dev;
```

单次任务插入 heap；timerfd 到期时，依次弹出已到期任务执行。这是高频 timeout 场景中一个重要的性能优化。

### 9.2 周期性 timer：每个 timer 一个 timerfd

周期 timer 与单次 timer 不同，代码为它们分别创建 timerfd：

```text
一个 recurring timer
  -> 一个 timerfd
  -> 注册进入对应 epoll
```

好处是逻辑直接，内核会维护周期节拍；代价是周期 timer 数量很大时 fd 数与 epoll 管理成本会上升。

### 9.3 SPDK timer

SPDK 模式下不使用 Linux `timerfd`，而是将 timer 映射为 `spdk_poller`。

全局 recurring timer 会向各 worker 注册 poller，但利用 `cur_term_num` 的 CAS，确保每一个周期只有一个线程真正执行 callback。这使“全局 timer”能够在分布式 poller 环境里只执行一次，而不是 N 个 reactor 重复触发。

---

## 10. 存储 I/O 的统一接口

`DriveInterface` 是所有驱动后端的统一抽象，见 [`drive_interface.hpp`](../src/include/iomgr/drive_interface.hpp)。

主要 API：

```cpp
async_write(...)
async_writev(...)
async_read(...)
async_readv(...)
async_unmap(...)
async_write_zero(...)
queue_fsync(...)

sync_write(...)
sync_writev(...)
sync_read(...)
sync_readv(...)
sync_write_zero(...)
```

异步接口返回：

```cpp
folly::Future<std::error_code>
```

这避免了为每个 I/O 自己维护 callback 状态机。

---

## 11. `drive_iocb`：一次 I/O 的状态对象

所有存储后端都围绕 `drive_iocb` 工作。它包含：

- `iodev`：目标设备；
- `iface`：所属 drive interface；
- `op_type`：READ / WRITE / UNMAP / WRITE_ZERO / FSYNC；
- `size`、`offset`；
- `iovcnt`；
- completion promise / callback；
- 错误重试次数；
- partial read 重试次数；
- 请求发起 reactor；
- 操作开始/提交时间；
- watchdog unique ID。

其 completion 是变体：

```cpp
std::variant<
    io_interface_comp_cb_t,
    folly::Promise<std::error_code>,
    FiberManagerLib::Promise<std::error_code>
> completion;
```

这意味着同一 I/O 控制块可以服务 callback 风格异步接口、Folly Future 异步接口和 fiber 中看似同步的等待接口。

### 11.1 内联 iovec 优化

`drive_iocb` 对常见少量 scatter/gather I/O 使用：

```cpp
static constexpr int inlined_iov_count = 4;
std::array<iovec, 4>
```

只有当 `iovcnt > 4` 才额外堆分配 iovec 数组。

这是典型的小对象优化：常见读写往往只有 1～4 个 iovec，因此可避免每次 `readv/writev` 都做 heap allocation，降低 allocator 压力和 cache miss。

---

## 12. 后端一：Linux AIO / `libaio`

实现：

- [`aio_drive_interface.hpp`](../src/lib/interfaces/aio_drive_interface.hpp)
- [`aio_drive_interface.cpp`](../src/lib/interfaces/aio_drive_interface.cpp)

这是 io_uring 不可用时的回退后端。

### 12.1 每 reactor 一个 AIO context

每个 reactor 初始化 AIO interface 时，建立一个 thread-local `aio_thread_context`：

```cpp
thread_local std::unique_ptr<aio_thread_context> t_aio_ctx;
```

它包含：

- 一个 `io_context_t`；
- 一个 completion eventfd；
- 一个提交计数；
- 一组 completion buffer；
- pending retry queue；
- 一个小批量提交数组。

这样做的性能意义：

- 每个 reactor 维护独立 AIO context；
- 避免所有线程竞争一个全局 context；
- completion 在提交 I/O 的 reactor 上处理；
- 设备状态与 CPU cache 有较好的亲和性。

### 12.2 AIO completion 回到 epoll

提交流程：

```text
io_submit
  -> io_set_eventfd(iocb, reactor-local eventfd)
  -> kernel 完成 I/O
  -> eventfd 可读
  -> epoll reactor 收到事件
  -> io_getevents()
  -> 完成 promise / callback
```

这正是 IOManager “网络 fd 与磁盘 I/O completion 同一个 epoll loop” 的核心实现。

### 12.3 动态调整 epoll 等待时间

AIO 有 outstanding I/O 时，系统不应长时间阻塞于 `epoll_wait`，否则 completion 虽然已经可读取，也可能因为超时配置而延迟处理。

AIO context 会根据 outstanding 数量调整：

```cpp
iomanager.this_reactor()->set_poll_interval(...)
```

配置默认是：

```text
无 I/O：epoll 最多等待 10ms
高并发 I/O：poll interval -> 0，进入紧密检查
```

这是一种“自适应 polling”的实现：

```text
闲时：睡眠，节省 CPU
忙时：缩短等待甚至不等待，降低 completion latency
```

---

## 13. 后端二：`io_uring`

实现：

- [`uring_drive_interface.hpp`](../src/lib/interfaces/uring_drive_interface.hpp)
- [`uring_drive_interface.cpp`](../src/lib/interfaces/uring_drive_interface.cpp)

启动时，IOManager 会探测内核能力。若可用且不是 SPDK 模式，默认选择 `UringDriveInterface`；否则退回 libaio。

### 13.1 每 reactor 一个 io_uring ring

`uring_drive_channel` 是 thread-local：

```cpp
static thread_local uring_drive_channel* t_uring_ch;
```

每个 channel 包含：

```cpp
io_uring m_ring;
std::queue<drive_iocb*> m_iocb_waitq;
uint32_t m_prepared_ios;
uint32_t m_in_flight_ios;
```

即每个 reactor 有独立的 submission queue、completion queue、等待队列与 in-flight 计数；没有全局 ring 锁竞争。

### 13.2 批量提交

当 API 调用标明 `part_of_batch=true` 时，I/O 可以先只准备 SQE：

```cpp
++m_prepared_ios;
```

之后由 `submit_batch()` 统一调用 `io_uring_submit()`。

这减少了每个 I/O 都一次 syscall 的成本，尤其适合写入日志、批量 KV flush、compaction 等工作负载。

### 13.3 队列深度与软件背压

提交是否允许，依赖：

```cpp
m_in_flight_ios + m_prepared_ios <= uring_per_thread_qdepth
```

默认每线程 qdepth 是 256。

如果 SQ/CQ 已满，请求放进：

```cpp
m_iocb_waitq
```

completion 到来后：

```text
消费 CQE
  -> in_flight--
  -> drain_waitq()
  -> 将等待请求继续填入 SQE
```

这形成一个本地的 I/O 流量控制机制。不过它不是完整的端到端背压：wait queue 是无界 `std::queue`，业务侧仍应限制同时提交的请求数量。

### 13.4 completion 获取

`io_uring` 注册 eventfd 到 epoll：

```cpp
io_uring_register_eventfd(&m_ring, ev_fd);
```

同时又挂了 reactor sentinel callback。因此 completion 有两层机会被及时处理：

1. eventfd 触发 epoll；
2. 每轮 reactor loop 结束时的 sentinel 检查。

这有利于降低“eventfd 通知合并、时序或 loop 边界”导致的 completion 延迟。

### 13.5 partial read 和错误重试

若 CQE 返回字节数小于预期：

```text
更新 iovec / offset / remaining size
  -> 重新入 wait queue
  -> 后续重试剩余未读部分
```

它说明 IOManager 不把“完成”简单理解成“内核返回了一个 CQE”，而是要求 read/write 达到预期长度，才完成用户 future。

---

## 14. 后端三：SPDK

SPDK 是 IOManager 的最高性能路径，也是设计最有特色的部分。

相关实现：

- [`iomgr_impl_spdk.cpp`](../src/lib/spdk/iomgr_impl_spdk.cpp)
- [`reactor_spdk.cpp`](../src/lib/spdk/reactor_spdk.cpp)
- [`spdk_drive_interface.cpp`](../src/lib/interfaces/spdk_drive_interface.cpp)

### 14.1 SPDK 的本质

SPDK 通常意味着：

- 用户态 NVMe driver；
- DPDK hugepage memory；
- poll-mode driver；
- 尽量绕过内核块层；
- 设备 completion 由应用主动 poll；
- 强烈依赖 CPU 核独占 / 绑核。

所以 SPDK 的高性能不是“接口换成 SPDK”就自动得到，而是运行模型改变为：

```text
一个 core
  -> 一个 tight-loop reactor
  -> 持续轮询 SPDK thread / device completion
  -> 尽量不发生上下文切换
```

### 14.2 Reactor 与 SPDK thread 的映射

初始化时，IOManager 为 reactor 内每个 fiber 创建一个 `spdk_thread`：

```cpp
for (auto& fiber : m_io_fibers) {
    auto sthread = create_spdk_thread();
    fiber->spdk_thr = sthread;
}
```

因此它不是简单的：

```text
1 reactor = 1 SPDK thread
```

而是：

```text
1 OS reactor thread
  -> 多个 IOManager fiber
  -> 每个 fiber 对应一个 SPDK thread
```

这样一个 OS 线程能轮询多个 SPDK logical thread，同时为上层提供 fiber 隔离和同步等待能力。

### 14.3 SPDK I/O channel 的 per-fiber 绑定

当 SPDK 设备加到 reactor 后，IOManager 为该 reactor 的每个 fiber 分配：

```cpp
spdk_io_channel* channel;
```

并放入：

```cpp
iodev->m_iodev_fiber_ctx[fiber->ordinal]
```

提交 I/O 时会随机选择一个 fiber，并取该 fiber 对应 channel。设计意图：

- SPDK 的 thread affinity 得到尊重；
- 不同 fiber 可使用各自的 I/O channel；
- 避免多个逻辑执行流共享同一个 channel；
- 更容易保持 SPDK 的无锁/低锁模型。

### 14.4 SPDK 内存与 hugepage

SPDK mode 启动时：

1. 初始化 SPDK/DPDK 环境；
2. 建立 hugepage 文件系统；
3. 按配置创建 mempool；
4. 把 SISL 的 aligned allocator 替换成 SPDK allocator。

默认 pool 分布为：

```text
4 KiB：总 hugepage 内存的 50%
64 KiB：总 hugepage 内存的 20%
```

收益包括：

- 避免频繁通用 malloc/free；
- 更易满足 DMA 对齐需求；
- 大页减少 TLB miss；
- 内存更适合 NVMe/SPDK DMA；
- 分配延迟更稳定。

---

## 15. 同步 API 为什么仍然高效

IOManager 同时提供同步 API，例如：

```cpp
sync_read(...)
sync_write(...)
```

这不表示它在 reactor 内做真正阻塞 I/O。

以 io_uring 为例：

```text
sync_read()
  -> 在当前 sync-I/O fiber 创建 Fiber Promise
  -> 提交 io_uring SQE
  -> future.get()
  -> 当前 fiber 挂起
  -> OS reactor 线程继续运行 event loop / 其他 fiber
  -> CQE 到来
  -> fulfill promise
  -> 原 fiber 恢复
```

所以对于业务代码来说：

```cpp
auto ec = iface->sync_read(...);
```

看似同步；但对于 reactor 的 OS 线程来说，它仍是并发异步执行。

这降低了业务代码写 callback 状态机的复杂度，同时保留异步吞吐。

但必须遵守限制：

- 不要在 main fiber 调用同步 I/O；
- 要让 `num_fibers >= 2`；
- 如果当前不是合适的 reactor/fiber 上下文，部分实现会退回传统 `pread/pwrite`。

---

## 16. 内存管理与 I/O 对齐

IOManager 向外暴露：

```cpp
iobuf_alloc()
iobuf_free()
iobuf_pool_alloc()
iobuf_pool_free()
iobuf_realloc()
iobuf_size()
```

普通 epoll 模式下，它使用 SISL 的 aligned allocator；SPDK 模式下替换为 SPDK/hugepage allocator。

应用应按设备属性分配 buffer：

```cpp
auto attr = DriveInterface::get_attributes(dev);
auto* buf = iomanager.iobuf_alloc(attr.align_size, size);
```

设备属性包括：

```cpp
phys_page_size
align_size
atomic_phys_page_size
num_streams
```

正确对齐的重要性：

- O_DIRECT / block I/O 可能强制要求地址、长度、offset 对齐；
- DMA 需要合适的 buffer；
- 对齐能减少 bounce buffer；
- 合理块大小更接近 SSD/NVMe 的最佳 I/O 粒度；
- `writev/readv` 可避免把多个逻辑段拼成临时连续 buffer。

---

## 17. 高性能来自哪里

IOManager 的性能不是单一技巧，而是多个机制叠加。

### 17.1 不把块 I/O 丢进阻塞线程池

核心收益：

```text
异步块 I/O completion
  -> 回到发起 reactor
  -> 避免额外 worker handoff
  -> 减少 context switch
  -> 减少共享队列竞争
```

尤其对大量小 I/O、NVMe、高并发读写更有意义。

### 17.2 每 reactor 独立状态，降低共享竞争

包括：

- epoll fd；
- AIO context；
- io_uring ring；
- eventfd；
- timer；
- metrics；
- SPDK I/O channel；
- fiber queues。

这种 **sharded reactor state** 减少全局锁和 cache-line bouncing。

### 17.3 消息传递代替跨线程直接操作

跨 reactor 的工作通过：

```text
UMPSC queue + eventfd
```

传递。这让实际设备状态尽量只由其所属 reactor 操作，避免复杂的任意线程访问。

### 17.4 I/O batch

支持多个层面批处理：

- io_uring SQE 批量提交；
- libaio 最多 4 个 iocb 的本地 batch；
- SPDK batch 聚合；
- epoll event batch，单次最多 20 个事件；
- message batch，默认最多 100 条。

批处理的本质是将：

```text
每个 I/O 一次 syscall / 一次调度 / 一次队列操作
```

转为：

```text
一批 I/O 一次 syscall / 一次调度 / 一组 completion
```

### 17.5 适应负载的 polling

在 epoll/AIO 模式下：

```text
空闲：较长 epoll timeout，省 CPU
有少量 outstanding I/O：缩短 timeout
高 outstanding I/O：timeout = 0，紧密轮询
```

在 SPDK 模式下：

```text
默认 tight loop
可选 adaptive backoff
没有 outstanding I/O 时，逐步 sleep
```

其背后取舍是：

| 模式 | 延迟 | 吞吐 | CPU |
|---|---:|---:|---:|
| 纯 epoll | 较高 | 较好 | 低 |
| 自适应 epoll polling | 中低 | 高 | 中 |
| SPDK tight loop | 最低 | 极高 | 高/固定占核 |
| SPDK adaptive loop | 较低 | 高 | 可降低空闲 CPU |

### 17.6 Future + fiber 把同步编程转为非阻塞执行

这一点对应用开发和性能都重要：

- 应用不必把每个流程拆成 callback 链；
- 不会因为等待 I/O 占住 OS 线程；
- 同一 reactor 可并发管理多个等待 I/O 的 fiber；
- 请求状态保留在 fiber 栈，而不是散落在手写状态机对象中。

---

## 18. 生命周期顺序

### 18.1 启动

`IOManager::start()` 的顺序：

```text
1. 读取参数、动态配置、CPU quota、内存限制
2. 选择 Epoll 或 SPDK 实现
3. 执行后端 pre_interface_init()
4. 探测 io_uring 能力
5. 创建 GenericIOInterface
6. 创建 DriveInterface：io_uring 或 libaio；SPDK mode 下额外创建 SPDK drive interface
7. 创建 worker reactors
8. 等待所有 worker reactor 启动
9. 创建全局 worker/user timer
10. 执行后端 post_interface_init()
11. 进入 running
12. 创建 I/O watchdog
```

这说明 drive interface 必须先存在，reactor 启动时再由 interface 完成每线程 context 初始化。

### 18.2 停止

`IOManager::stop()` 的顺序：

```text
1. 后端 pre_interface_stop()
2. 状态置为 stopping
3. 销毁全局 timer
4. 等待 pending timer 操作完成
5. 广播调用每个 reactor 的 stop()
6. 等待所有 reactor 退出
7. join worker 线程
8. 清理 reactor、interface、device 列表
9. 后端 post_interface_stop()
```

值得注意的是：

- timer 先停止，防止 teardown 时继续投递回调；
- reactor 通过消息被要求 stop，确保大部分资源仍在所属线程上下文销毁；
- interface 的 `on_reactor_stop()` 负责卸载设备。

---

## 19. 可观测性

IOManager 内建比较完整的 metric 点。

每 reactor 的指标包括：

- message 唤醒次数；
- timer 唤醒次数；
- I/O event 唤醒次数；
- idle 唤醒次数；
- 当前事件 fd 数；
- 接收消息数；
- message eventfd `EAGAIN` 次数；
- outstanding ops；
- I/O batch 数；
- 实际提交 I/O 数；
- drive I/O 数；
- 平均 drive latency；
- completion callback 数；
- AIO completion 数。

每设备也可统计：

- read/write/fsync size histogram；
- read/write/fsync latency histogram。

这对于性能诊断非常重要。例如：

- `outstanding_ops` 太低：队列深度不足，设备没吃满；
- `retry_list_size` 增大：AIO 队列/设备压力过大；
- `CQ overflow`：io_uring 队列配置或消费速度有问题；
- `msg_iodev_busy_count` 高：跨线程消息量过大；
- 高 `idle_wakeup_count`：poll timeout 太激进，可能浪费 CPU；
- drive latency 正常但端到端 latency 高：问题可能在消息排队、fiber 饥饿或业务逻辑。

---

## 20. I/O Watchdog

`IOWatchDog` 位于：

- [`watchdog.hpp`](../src/lib/watchdog.hpp)
- [`watchdog.cpp`](../src/lib/watchdog.cpp)

可选地跟踪 SPDK I/O：

```text
提交时 add_io(iocb)
完成时 complete_io(iocb)
周期 timer 扫描 outstanding I/O
超时则记录 critical 日志并触发断言
```

默认关闭：

```text
io_watchdog_timer_on = false
```

这是故障检测机制，而非性能特性；开启时有 mutex 和 map 操作，代码也明确警告可能影响性能。

---

## 21. 测试展示的典型使用方式

[`test_drive_interface.cpp`](../src/test/test_drive_interface.cpp) 展示了三种发起 I/O 的方式：

1. **worker reactor 中发起**；
2. **user reactor 中发起**；
3. **普通非 reactor 线程中发起**。

普通线程发起时，I/OManager 会将提交请求路由到随机 worker reactor。

测试中还使用了 `available_qs` 控制每个 workload 的并发深度，默认是 8。这个例子传达了一个关键性能原则：

> 对 SSD/NVMe 来说，只发一个 I/O、等一个 I/O 完成，通常无法获得高吞吐；要保持适度 queue depth。

---

## 22. 设计局限与值得警惕的实现细节

以下是从当前代码直接能够看出的局限、约束或风险。

### 22.1 平台现实：核心实现高度 Linux 化

虽然代码包含部分 macOS 条件分支，但关键高性能路径依赖：

- `epoll`；
- `eventfd`；
- `timerfd`；
- `libaio`；
- `io_uring`；
- SPDK/DPDK；
- `/proc`、`/sys`；
- cgroup cpuset。

因此其主要生产目标显然是 Linux，而非 macOS 或 Windows。

### 22.2 `num_fibers` 必须合理设置

同步 I/O fiber 选择代码隐含要求：

```text
num_fibers >= 2
```

否则没有 sync-capable fiber，选择范围会无效。默认配置是 4。

### 22.3 Reactor 内“同步”不等于任意位置都能阻塞

只有在非 main fiber 的 reactor 上，`sync_read/sync_write` 才能优雅转化为 fiber 等待。

在普通线程或不满足条件的上下文中，一些路径会退回 `pread/pwrite`。因此应用不要误以为所有同步调用都不会阻塞 OS 线程。

### 22.4 全局设备会被注册到多个 reactor

一个 scope 为 `all_io` 的普通 fd 可能被加入多个 epoll reactor。这依赖 `EPOLLEXCLUSIVE` 降低多个 reactor 同时被唤醒的概率，但应用仍必须明确设备访问语义：

- callback 是否线程安全；
- 是否允许多个 reactor 读取同一 socket；
- 是否应把连接固定到单 reactor；
- 是否有协议层串行化要求。

对于典型 TCP 连接，通常更合适的模型是连接固定到一个 reactor，而不是全局广播注册。

### 22.5 繁忙时 unbounded queue 仍可能耗尽内存

以下队列都可能增长：

- fiber overflow messages；
- AIO pending list；
- io_uring wait queue；
- 应用自身 future / 请求队列。

IOManager 做了局部队列深度控制，但没有完整的、自动向上游传播的背压协议。因此高压场景仍要由调用方限制 request concurrency、限制 outstanding I/O、设置租户级或设备级 QD，并在队列过长时拒绝、延迟或降级请求。

### 22.6 `async_write_zero` 并非所有后端都是真异步

- AIO 后端尚不支持；
- io_uring 后端当前实现为同步 `sync_write_zero()` 后立即返回 future；
- SPDK 后端可用原生 `spdk_bdev_write_zeroes`。

所以不要假设统一 API 在所有后端都有等价性能和语义。

### 22.7 AIO 和 io_uring 的异常/重试路径需压测验证

当前实现包含较多 retry 和错误处理，但其中一些逻辑值得在目标版本上做压力测试。例如：

- libaio `EAGAIN` / queue full 重试；
- io_uring partial read 重试；
- CQ overflow 直接抛错；
- 设备关闭时 outstanding I/O 的竞态；
- stop 与 completion 同时发生的边界；
- 高并发 `run_on_wait` 的死锁风险。

尤其 `drive_iocb::update_iovs_on_partial_result()` 会重建剩余 iovec，partial-read 这类少见路径通常最容易隐藏边界问题。

### 22.8 SPDK 模式的性能前提很苛刻

SPDK 的收益建立在：

- 正确 hugepage 配置；
- DPDK/SPDK 环境成功初始化；
- CPU pinning；
- 足够 CPU 核；
- 设备和 DMA 对齐；
- 避免从非 SPDK reactor 频繁提交；
- 合理 I/O queue depth；
- 尽量减少跨 reactor 迁移。

如果应用频繁从普通线程或 epoll reactor 向 SPDK worker 投递 I/O，那么：

```text
消息队列 + cross-reactor handoff
```

会部分抵消 SPDK 的低延迟优势。

---

## 23. 总结：它如何做到高性能 I/O

可以把 IOManager 的性能模型压缩为下面这条链路：

```text
请求固定或路由到 reactor
  -> reactor 本地创建/维护 I/O 状态
  -> 使用 fiber 保留同步编程模型但不阻塞 OS 线程
  -> 使用 io_uring / libaio / SPDK 异步提交
  -> completion 回到同一 reactor
  -> 少锁、少线程迁移、少上下文切换
  -> 批量提交、局部队列深度、适应性 polling
```

其最重要的性能设计点是：

1. **统一事件循环**：消息、timer、网络 fd、内核 I/O completion 在同一个 reactor 处理。
2. **reactor 分片**：每线程持有独立 epoll/AIO/uring/SPDK 状态，降低全局共享竞争。
3. **fiber 化同步 I/O**：让代码可写成同步流程，但底层仍能并发执行。
4. **SPDK poll-mode**：极端性能路径中尽量绕过内核中断和调度。
5. **每 fiber 的 SPDK thread / I/O channel**：遵循 SPDK affinity，减少跨线程共享。
6. **message + eventfd doorbell 合并**：跨线程通信避免每条消息都造成昂贵唤醒。
7. **I/O batching**：减少提交 syscall、队列操作和调度次数。
8. **内存对齐、pool、hugepage**：适配 DMA、减少 allocator 与 TLB 成本。
9. **动态 poll interval / adaptive backoff**：在 CPU 消耗与 completion latency 间动态权衡。
10. **metrics 和 watchdog**：使队列深度、延迟、重试、溢出和卡死可以被观测。

从定位上说，IOManager 不是轻量 socket event loop，也不是单纯 I/O 库；它更接近一个**为高性能存储服务量身定制的用户态异步执行运行时**。它最适合的应用是：

- 存储引擎；
- KV / LSM 数据库；
- 高并发日志或 WAL 系统；
- NVMe-heavy 服务；
- 需要把网络请求、异步磁盘 I/O 与 timer 整合在一起的服务；
- 希望在普通 epoll 模式与 SPDK 高性能模式间切换的系统。
