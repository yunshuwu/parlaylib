
#ifndef PARLAY_ATOMIC_ATOMIC_RC_PTR_H
#define PARLAY_ATOMIC_ATOMIC_RC_PTR_H

#include <atomic>

#include "../parallel.h"

#include "acquire_retire.h"

namespace parlay {
namespace internal {

// An instance of an object of type T prepended with an atomic reference count.
// Use of this type for allocating shared objects ensures that the ref count can
// be found from a pointer to the object, and that the two are both corectly aligned.
template<typename T>
struct counted_object {
  T object;
  std::atomic<uint64_t> ref_cnt;

  template<typename... Args>
  counted_object(Args&&... args) : object(std::forward<Args>(args)...), ref_cnt(1) { }
  ~counted_object() = default;

  counted_object(const counted_object&) = delete;
  counted_object(counted_object&&) = delete;

  T* get() { return std::addressof(object); }
  const T* get() const { return std::addressof(object); }

  uint64_t add_refs(uint64_t count) { return ref_cnt.fetch_add(count); }
  uint64_t release_refs(uint64_t count) { return ref_cnt.fetch_sub(count); }
};

}  // namespace internal


template<typename T>
class atomic_rc_ptr;

template<typename T>
class rc_ptr {

 public:
  rc_ptr() noexcept : ptr(nullptr) { }
  rc_ptr(std::nullptr_t) noexcept : ptr(nullptr) { }
  rc_ptr(const rc_ptr& other) noexcept : ptr(other.ptr) { if (ptr) increment_counter(ptr); }
  rc_ptr(rc_ptr&& other) noexcept : ptr(other.release()) { }
  ~rc_ptr() { if (ptr) decrement_counter(ptr); }

  // copy assignment
  rc_ptr& operator=(const rc_ptr& other) {
    auto tmp = ptr;
    ptr = other.ptr;
    if (ptr) increment_counter(ptr);
    if (tmp) decrement_ctr(tmp);
    return *this;
  }

  // move assignment
  rc_ptr& operator=(rc_ptr&& other) {
    auto tmp = ptr;
    ptr = other.release();
    if (tmp) decrement_counter(tmp);
    return *this;
  }

  typename std::add_lvalue_reference_t<T> operator*() const { return *(ptr->get()); }

  T* get() { return (ptr == nullptr) ? nullptr : ptr->get(); }
  const T* get() const { return (ptr == nullptr) ? nullptr : ptr->get(); }

  T* operator->() { return (ptr == nullptr) ? nullptr : ptr->get(); }
  const T* operator->() const { return (ptr == nullptr) ? nullptr : ptr->get(); }

  explicit operator bool() const { return ptr != nullptr; }

  bool operator==(const rc_ptr& other) const { return get() == other.get(); }
  bool operator!=(const rc_ptr& other) const { return get() != other.get(); }

  size_t use_count() const noexcept { return (ptr == nullptr) ? 0 : ptr->ref_cnt.load(); }

  // Create a new rc_ptr containing an object of type T constructed from (args...).
  template<typename... Args>
  static rc_ptr<T> make_shared(Args&&... args) {
    internal::counted_object<T>* ptr = new internal::counted_object<T>(std::forward<Args>(args)...);
    return rc_ptr<T>(ptr, AddRef::no);
  }

 private:
  friend class atomic_rc_ptr<T>;

  enum class AddRef { yes, no };

  explicit rc_ptr(internal::counted_object<T>* ptr_, AddRef add_ref) : ptr(ptr_) {
    if (ptr && add_ref == AddRef::yes) increment_counter(ptr);
  }

  internal::counted_object<T>* release() {
    auto p = ptr;
    ptr = nullptr;
    return p;
  }

  internal::counted_object<T>* get_counted() const {
    return ptr;
  }

  static void increment_counter(internal::counted_object<T>* ptr) {
    assert(ptr != nullptr);
    ptr->add_refs(1);
  }

  static void decrement_counter(internal::counted_object<T>* ptr) {
    assert(ptr != nullptr);
    if (ptr->release_refs(1) == 1) {
      delete ptr;
    }
  }

  internal::counted_object<T>* ptr;
};

// Create a new rc_ptr containing an object of type T constructed from (args...).
template<typename T, typename... Args>
static rc_ptr<T> make_shared(Args&&... args) {
  return rc_ptr<T>::make_shared(std::forward<Args>(args)...);
}

template<typename T>
class atomic_rc_ptr {
 public:
  atomic_rc_ptr() : atomic_ptr(nullptr) { }
  atomic_rc_ptr(rc_ptr<T> desired) : atomic_ptr(desired.release()) { }

  ~atomic_rc_ptr() {
    auto ptr = atomic_ptr.load();
    if (ptr != nullptr) decrement_counter(ptr);
  }

  atomic_rc_ptr(const atomic_rc_ptr&) = delete;
  atomic_rc_ptr& operator=(const atomic_rc_ptr&) = delete;
  atomic_rc_ptr(atomic_rc_ptr&&) = delete;
  atomic_rc_ptr& operator=(atomic_rc_ptr&&) = delete;

  bool is_lock_free() const noexcept { return true; }
  static constexpr bool is_always_lock_free = true;

  void store(rc_ptr<T> desired) noexcept {
    auto new_ptr = desired.release();
    auto old_ptr = atomic_ptr.exchange(new_ptr, std::memory_order_seq_cst);
    if (old_ptr != nullptr) ar.retire(old_ptr, worker_id());
    ar.perform_deferred_decrements(worker_id());
  }

  rc_ptr<T> load() const noexcept {
    auto ptr = ar.acquire(&atomic_ptr, worker_id());
    rc_ptr<T> result(ptr, rc_ptr<T>::AddRef::yes);
    ar.release(worker_id());
    return result;
  }

  bool compare_exchange_strong(rc_ptr<T>& expected, const rc_ptr<T>& desired) noexcept {
    auto desired_ptr = desired.get_counted();
    auto expected_ptr = expected.get_counted();

    if (atomic_ptr.compare_exchange_strong(expected_ptr, desired_ptr, std::memory_order_seq_cst)) {
      if (expected_ptr != nullptr) {
        ar.retire(expected_ptr, worker_id());
        ar.perform_deferred_decrements(worker_id());
      }

      // Need to announce desired to prevent the ref count getting clobbered
      if (desired_ptr != nullptr) increment_counter(desired_ptr);
      return true;
    }
    else {
      expected = rc_ptr<T>(expected_ptr, rc_ptr<T>::AddRef::yes);
      return false;
    }
  }

  bool compare_exchange_strong(rc_ptr<T>& expected, rc_ptr<T>&& desired) noexcept {
    auto desired_ptr = desired.get_counted();
    auto expected_ptr = expected.get_counted();

    if (atomic_ptr.compare_exchange_strong(expected_ptr, desired_ptr, std::memory_order_seq_cst)) {
      if (expected_ptr != nullptr) {
        ar.retire(expected_ptr, worker_id());
        ar.perform_deferred_decrements(worker_id());
      }

      if (desired_ptr != nullptr) desired.release();
      return true;
    }
    else {
      expected = rc_ptr<T>(expected_ptr, rc_ptr<T>::AddRef::yes);
      return false;
    }
  }

  bool compare_exchange_weak(rc_ptr<T>& expected, const rc_ptr<T>& desired) noexcept {
    return compare_exchange_strong(expected, desired);
  }

  bool compare_exchange_weak(rc_ptr<T>& expected, rc_ptr<T>&& desired) noexcept {
    return compare_exchange_strong(expected, std::move(desired));
  }

  bool compare_and_swap(const rc_ptr<T>& expected, const rc_ptr<T>& desired) noexcept {
    auto desired_ptr = desired.get_counted();
    auto expected_ptr = expected.get_counted();

    if (atomic_ptr.compare_exchange_strong(expected_ptr, desired_ptr, std::memory_order_seq_cst)) {
      if (expected_ptr != nullptr) {
        ar.retire(expected_ptr, worker_id());
        ar.perform_deferred_decrements(worker_id());
      }

      // Note: We need to protect desired
      if (desired_ptr != nullptr) increment_counter(desired_ptr);
      return true;
    }
    else {
      return false;
    }
  }

  bool compare_and_swap(const rc_ptr<T>& expected, rc_ptr<T>&& desired) noexcept {
    auto desired_ptr = desired.get_counted();
    auto expected_ptr = expected.get_counted();

    if (atomic_ptr.compare_exchange_strong(expected_ptr, desired_ptr, std::memory_order_seq_cst)) {
      if (expected_ptr != nullptr) {
        ar.retire(expected_ptr, worker_id());
        ar.perform_deferred_decrements(worker_id());
      }

      if (desired_ptr != nullptr) desired.release();
      return true;
    }
    else {
      return false;
    }
  }

  // Swaps the currently stored shared pointer with the given
  // shared pointer. This operation does not affect the reference
  // counts of either shared pointer.
  //
  // Note that it is not safe to concurrently access desired
  // while this operation is taking place, since desired is a
  // non-atomic shared pointer!
  void swap(rc_ptr<T>& desired) noexcept {
    auto desired_ptr = desired.release();
    auto current_ptr = atomic_ptr.get_counted();
    desired = rc_ptr<T>(current_ptr, rc_ptr<T>::AddRef::no);
    while (!atomic_ptr.compare_exchange_weak(desired.ptr, desired_ptr)) { }
  }

/*
  push:

  // Standard inefficient version
  auto new_node = parlay::make_shared<Node>(head.load(), some_val);
  while (!head.compare_exchange_weak(new_node->next, new_node)) { }

  // Efficient version using swap
  auto new_node = parlay::make_shared<Node>(nullptr, some_val);
  auto& next_ptr = new_node->next;
  new_node->next = std::move(new_node);
  head.swap(next_ptr);

  // Version using snapshot??
  auto new_node = parlay::make_shared<Node>(nullptr, some_val);
  auto cur_head = head.get_snapshot();
  auto cur_next = cur_head->next.get_snapshot();
  ???

  pop:
  node = head.get_snapshot();
  while (node && !head.compare_exchange_weak(node, node->next)) { }
  return node ? node->value : std::nullopt;

  peek:


  find:


  compare_exchange(snapshot_ptr<T>& expected, const rc_ptr<T>& desired) {
    auto desired_ptr = desired.get_counted();
    auto expected_ptr = expected.get_counted();

    if (atomic_ptr.compare_exchange_strong(expected_ptr, desired_ptr, std::memory_order_seq_cst)) {
      if (expected_ptr != nullptr) {
        ar.retire(expected_ptr, worker_id());
        ar.perform_deferred_decrements(worker_id());
      }

      // Note: We do not need to protect the reference count here with acquire-retire
      // because the input parameter *desired* ensures that the reference count is at
      // least one at all times, so the object will never be destructed.
      if (desired_ptr != nullptr) increment_counter(desired_ptr);
      return true;
    }
    else {
      expected = snapshot_ptr<T>(expected_ptr);
      return false;
    }
  }
*/

  rc_ptr<T> exchange(rc_ptr<T> desired) noexcept {
    auto new_ptr = desired.release();
    auto old_ptr = atomic_ptr.exchange(new_ptr, std::memory_order_seq_cst);
    return rc_ptr<T>(old_ptr, rc_ptr<T>::AddRef::no);
  }

  void operator=(rc_ptr<T> desired) noexcept { store(std::move(desired)); }
  operator rc_ptr<T>() const noexcept { return load(); }

 private:
  std::atomic<internal::counted_object<T>*> atomic_ptr;

  static void increment_counter(internal::counted_object<T>* ptr) {
    assert(ptr != nullptr);
    ptr->add_refs(1);
  }

  static void decrement_counter(internal::counted_object<T>* ptr) {
    assert(ptr != nullptr);
    if (ptr->release_refs(1) == 1) {
      delete ptr;
    }
  }

  struct counted_deleter {
    void operator()(internal::counted_object<T>* ptr) const {
      assert(ptr != nullptr);
      decrement_counter(ptr);
    }
  };

  inline static internal::acquire_retire<internal::counted_object<T>*, counted_deleter> ar{num_workers()};
};

}  // namespace parlay

#endif  // PARLAY_ATOMIC_RC_PTR_H
