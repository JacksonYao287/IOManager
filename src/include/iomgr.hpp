/**
 * Copyright eBay Corporation 2018
 */

#pragma once

extern "C" {
#include <event.h>
#include <sys/time.h>
}
#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <vector>
#include <utility/thread_buffer.hpp>
#include <utility/atomic_counter.hpp>
#include <fds/sparse_vector.hpp>
#include <folly/Synchronized.h>
#include "iomgr_msg.hpp"
#include "reactor.hpp"
#include "iomgr_timer.hpp"
#include "io_interface.hpp"
#include "drive_interface.hpp"
#include <functional>
#include <fds/utils.hpp>
#include <fds/id_reserver.hpp>
#include <utility/enum.hpp>

struct spdk_bdev_desc;
struct spdk_bdev;
struct spdk_nvmf_qpair;

namespace iomgr {

struct timer_info;

// TODO: Make this part of an enum, to force add count upon adding new inbuilt io interface.
static constexpr int inbuilt_interface_count = 1;

class DriveInterface;

ENUM(iomgr_state, uint16_t, stopped, interface_init, reactor_init, sys_init, running, stopping)

template < class... Ts >
struct overloaded : Ts... {
    using Ts::operator()...;
};
template < class... Ts >
overloaded(Ts...)->overloaded< Ts... >;

using msg_handler_t = std::function< void(iomgr_msg*) >;
using interface_adder_t = std::function< void(void) >;
using reactor_info_t = std::pair< std::thread, std::shared_ptr< IOReactor > >;

class IOManager {
public:
    friend class IOReactor;
    friend class IOReactorEPoll;
    friend class IOInterface;

    static IOManager& instance() {
        static IOManager inst;
        return inst;
    }

    // TODO: Make this a dynamic config (albeit non-hotswap)
    static constexpr uint32_t max_msg_modules = 64;
    static constexpr uint32_t max_io_threads = 1024; // Keep in mind increasing this cause increased mem footprint

    IOManager();
    ~IOManager();

    /********* Start/Stop Control Related Operations ********/
    void start(size_t num_threads, bool is_spdk = false, const thread_state_notifier_t& notifier = nullptr,
               const interface_adder_t& iface_adder = nullptr);
    void stop();
    void run_io_loop(bool is_tloop_reactor, const iodev_selector_t& iodev_selector = nullptr,
                     const thread_state_notifier_t& addln_notifier = nullptr) {
        _run_io_loop(-1, is_tloop_reactor, iodev_selector, addln_notifier);
    }
    void stop_io_loop();

    /********* Interface/Device Related Operations ********/
    void add_interface(std::shared_ptr< IOInterface > iface);
    void add_drive_interface(std::shared_ptr< DriveInterface > iface, bool is_default);
    void device_reschedule(const io_device_ptr& iodev, int event);

    /*template < class... Args >
    int run_on(thread_specifier s, Args&&... args) {
        if (std::holds_alternative< thread_regex >(s)) {
            run_on(std::get< thread_regex >(s), std::forward< Args >(args)...);
        } else {
            run_on(std::get< io_thread_t >(s), std::forward< Args >(args)...);
        }
    }*/

    int run_on(thread_regex r, const auto& fn, bool wait_for_completion = false) {
        int sent_to = 0;
        if (wait_for_completion) {
            sync_iomgr_msg smsg(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, fn);
            sent_to = multicast_msg_and_wait(r, smsg);
            LOGDEBUGMOD(iomgr, "Run method sync msg completion done"); // TODO: Remove this line
        } else {
            sent_to = multicast_msg(r, iomgr_msg::create(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, fn));
        }
        return sent_to;
    }

    int run_on(const io_thread_t& thread, const auto& fn, bool wait_for_completion = false) {
        bool sent = false;
        if (wait_for_completion) {
            sync_iomgr_msg smsg(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, fn);
            sent = send_msg_and_wait(thread, smsg);
        } else {
            sent = send_msg(thread, iomgr_msg::create(iomgr_msg_type::RUN_METHOD, m_internal_msg_module_id, fn));
        }
        return ((int)sent);
    }

    /********* Access related methods ***********/
    const io_thread_t& iothread_self() const;
    IOReactor* this_reactor() const;

    DriveInterface* default_drive_interface() { return m_default_drive_iface.get(); }
    GenericIOInterface* generic_interface() { return m_default_general_iface.get(); }
    bool am_i_io_reactor() const {
        auto r = this_reactor();
        return r && r->is_io_reactor();
    }

    bool am_i_tight_loop_reactor() const {
        auto r = this_reactor();
        return r && r->is_tight_loop_reactor();
    }

    bool am_i_worker_reactor() const {
        auto r = this_reactor();
        return r && r->is_worker();
    }

    /********* State Machine Related Operations ********/
    bool is_ready() const { return (get_state() == iomgr_state::running); }
    // bool is_interface_registered() const { return ((uint16_t)get_state() > (uint16_t)iomgr_state::interface_init); }
    void wait_to_be_ready() {
        std::unique_lock< std::mutex > lck(m_cv_mtx);
        m_cv.wait(lck, [this] { return (get_state() == iomgr_state::running); });
    }

    void wait_to_be_stopped() {
        std::unique_lock< std::mutex > lck(m_cv_mtx);
        if (get_state() != iomgr_state::stopped) {
            m_cv.wait(lck, [this] { return (get_state() == iomgr_state::stopped); });
        }
    }

    /*void wait_for_interface_registration() {
        std::unique_lock< std::mutex > lck(m_cv_mtx);
        m_cv.wait(lck, [this] { return is_interface_registered(); });
    }*/

    void wait_for_state(iomgr_state expected_state) {
        std::unique_lock< std::mutex > lck(m_cv_mtx);
        if (get_state() != expected_state) {
            m_cv.wait(lck, [&] { return (get_state() == expected_state); });
        }
    }

    void ensure_running() {
        if (get_state() != iomgr_state::running) {
            LOGINFO("IOManager is not running, will wait for it to be ready");
            wait_to_be_ready();
            LOGINFO("IOManager is ready now");
        }
    }
    thread_state_notifier_t& thread_state_notifier() { return m_common_thread_state_notifier; }

    /******** IO Thread related infra ********/
    io_thread_t make_io_thread(IOReactor* reactor);

    /******** Message related infra ********/
    bool send_msg(const io_thread_t& thread, iomgr_msg* msg);
    bool send_msg_and_wait(const io_thread_t& thread, sync_iomgr_msg& smsg);
    int multicast_msg(thread_regex r, iomgr_msg* msg);
    int multicast_msg_and_wait(thread_regex r, sync_iomgr_msg& smsg);

    msg_module_id_t register_msg_module(const msg_handler_t& handler);
    msg_handler_t& get_msg_module(msg_module_id_t id);

    /******** IO Buffer related ********/
    uint8_t* iobuf_alloc(size_t align, size_t size);
    void iobuf_free(uint8_t* buf);
    uint8_t* iobuf_realloc(uint8_t* buf, size_t align, size_t new_size);

    /******** Timer related Operations ********/
    int64_t idle_timeout_interval_usec() const { return -1; };
    void idle_timeout_expired() {
        if (m_idle_timeout_expired_cb) { m_idle_timeout_expired_cb(); }
    }

    timer_handle_t schedule_thread_timer(uint64_t nanos_after, bool recurring, void* cookie,
                                         timer_callback_t&& timer_fn);
    timer_handle_t schedule_global_timer(uint64_t nanos_after, bool recurring, void* cookie, thread_regex r,
                                         timer_callback_t&& timer_fn);

    void cancel_timer(timer_handle_t thdl) { return thdl.first->cancel(thdl); }

private:
    void foreach_interface(const auto& iface_cb);

    void _run_io_loop(int iomgr_slot_num, bool is_tloop_reactor, const iodev_selector_t& iodev_selector,
                      const thread_state_notifier_t& addln_notifier);

    void reactor_started(std::shared_ptr< IOReactor > reactor); // Notification that iomanager thread is ready to serve
    void reactor_stopped();                                     // Notification that IO thread is reliquished

    void start_spdk();
    void set_state(iomgr_state state) { m_state.store(state, std::memory_order_release); }
    iomgr_state get_state() const { return m_state.load(std::memory_order_acquire); }
    void set_state_and_notify(iomgr_state state) {
        set_state(state);
        m_cv.notify_all();
    }

    void _pick_reactors(thread_regex r, const auto& cb);
    void all_reactors(const auto& cb);
    void specific_reactor(int thread_num, const auto& cb);

    [[nodiscard]] auto iface_wlock() { return m_iface_list.wlock(); }
    [[nodiscard]] auto iface_rlock() { return m_iface_list.rlock(); }

private:
    // size_t m_expected_ifaces = inbuilt_interface_count;        // Total number of interfaces expected
    std::atomic< iomgr_state > m_state = iomgr_state::stopped;    // Current state of IOManager
    sisl::atomic_counter< int16_t > m_yet_to_start_nreactors = 0; // Total number of iomanager threads yet to start
    sisl::atomic_counter< int16_t > m_yet_to_stop_nreactors = 0;

    folly::Synchronized< std::vector< std::shared_ptr< IOInterface > > > m_iface_list;
    folly::Synchronized< std::unordered_map< backing_dev_t, io_device_ptr > > m_iodev_map;
    folly::Synchronized< std::vector< std::shared_ptr< DriveInterface > > > m_drive_ifaces;

    std::shared_ptr< DriveInterface > m_default_drive_iface;
    std::shared_ptr< GenericIOInterface > m_default_general_iface;
    folly::Synchronized< std::vector< uint64_t > > m_global_thread_contexts;

    sisl::ActiveOnlyThreadBuffer< std::shared_ptr< IOReactor > > m_reactors;

    std::mutex m_cv_mtx;
    std::condition_variable m_cv;
    std::function< void() > m_idle_timeout_expired_cb = nullptr;

    sisl::sparse_vector< reactor_info_t > m_worker_reactors;

    bool m_is_spdk = false;
    std::unique_ptr< timer_epoll > m_global_user_timer;
    std::unique_ptr< timer > m_global_worker_timer;

    std::mutex m_msg_hdlrs_mtx;
    std::array< msg_handler_t, max_msg_modules > m_msg_handlers;
    uint32_t m_msg_handlers_count = 0;
    msg_module_id_t m_internal_msg_module_id;
    thread_state_notifier_t m_common_thread_state_notifier = nullptr;
    sisl::IDReserver m_thread_idx_reserver;
};

struct SpdkAlignedAllocImpl : public sisl::AlignedAllocatorImpl {
    uint8_t* aligned_alloc(size_t align, size_t sz) override;
    void aligned_free(uint8_t* b) override;
    uint8_t* aligned_realloc(uint8_t* old_buf, size_t align, size_t new_sz, size_t old_sz = 0) override;
};

#define iomanager iomgr::IOManager::instance()
} // namespace iomgr
