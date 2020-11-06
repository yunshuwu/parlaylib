
#ifndef PARLAY_ATOMIC_ATOMIC_RC_PTR_H
#define PARLAY_ATOMIC_ATOMIC_RC_PTR_H

#include <atomic>

#include "../parallel.h"

#include "acquire_retire.h"
#include "rc_ptr.h"
#include "snapshot_ptr.h"

namespace parlay {

template<typename T>
class atomic_rc_ptr {
 public:
  atomic_rc_ptr() : atomic_ptr(nullptr) {}
  atomic_rc_ptr(rc_ptr<T> desired) : atomic_ptr(desired.release()) {}

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
    if (old_ptr != nullptr) ar.retire(old_ptr);
  }

  rc_ptr<T> load() const noexcept {
    auto acquired_ptr = ar.acquire(&atomic_ptr);
    rc_ptr<T> result(acquired_ptr.value, rc_ptr<T>::AddRef::yes);
    return result;
  }

  snapshot_ptr<T> get_snapshot() const noexcept {
    auto [value, slot] = ar.protect_snapshot(&atomic_ptr);
    return snapshot_ptr<T>(value, slot);
  }

  // Atomically compares the underlying rc_ptr with expected, and if they are equal,
  // replaces the current rc_ptr with a copy of desired (incrementing its reference
  // count) and returns true. Otherwise returns false.
  bool compare_and_swap(const rc_ptr<T>& expected, const rc_ptr<T>& desired) noexcept {
    return compare_and_swap_copy(expected.get_counted(), desired);
  }

  // Atomically compares the underlying rc_ptr with expected, and if they refer to
  // the same managed object, replaces the current rc_ptr with a copy of desired
  // (incrementing its reference count) and returns true. Otherwise returns false.
  bool compare_and_swap(const snapshot_ptr<T>& expected, const rc_ptr<T>& desired) noexcept {
    return compare_and_swap_copy(expected.get_counted(), desired);
  }

  // Atomically compares the underlying rc_ptr with expected, and if they are equal,
  // replaces the current rc_ptr with desired by move assignment, hence leaving its
  // reference count unchanged. Otherwise returns false and leaves desired unmodified.
  bool compare_and_swap(const rc_ptr<T>& expected, rc_ptr<T>&& desired) noexcept {
    return compare_and_swap_move(expected.get_counted(), std::move(desired));
  }

  // Atomically compares the underlying rc_ptr with expected, and if they refer to
  // the same managed object, replaces the current rc_ptr with a copy of desired,
  // by move assignment, hence leaving its reference count unchanged. Otherwise
  // returns false and leaves desired unmodified.
  bool compare_and_swap(const snapshot_ptr<T>& expected, rc_ptr<T>&& desired) noexcept {
    return compare_and_swap_move(expected.get_counted(), std::move(desired));
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
    auto current_ptr = atomic_ptr.load();
    desired = rc_ptr<T>(current_ptr, rc_ptr<T>::AddRef::no);
    while (!atomic_ptr.compare_exchange_weak(desired.ptr, desired_ptr)) {
    }
  }

  rc_ptr<T> exchange(rc_ptr<T> desired) noexcept {
    auto new_ptr = desired.release();
    auto old_ptr = atomic_ptr.exchange(new_ptr, std::memory_order_seq_cst);
    return rc_ptr<T>(old_ptr, rc_ptr<T>::AddRef::no);
  }

  void operator=(rc_ptr<T> desired) noexcept { store(std::move(desired)); }
  operator rc_ptr<T>() const noexcept { return load(); }

 private:
  std::atomic<internal::counted_object<T>*> atomic_ptr;

  bool compare_and_swap_copy(internal::counted_object<T>* expected_ptr, const rc_ptr<T>& desired) noexcept {
    auto desired_ptr = desired.get_counted();

    // Note: In the copy case, we need to protect desired to prevent a race where
    // the CAS succeeds, but before the reference count is incremented, a store
    // clobbers desired and decrements its reference count, possibly to zero.
    auto reservation = ar.reserve(desired_ptr);

    if (atomic_ptr.compare_exchange_strong(expected_ptr, desired_ptr, std::memory_order_seq_cst)) {
      if (expected_ptr != nullptr) {
        ar.retire(expected_ptr);
      }

      if (desired_ptr != nullptr) increment_counter(desired_ptr);
      return true;
    } else {
      return false;
    }
  }

  bool compare_and_swap_move(internal::counted_object<T>* expected_ptr, rc_ptr<T>&& desired) noexcept {
    auto desired_ptr = desired.get_counted();

    // Note: No need to protect desired in the move case because after a successful
    // move, the reference owned by desired is now owned by the atomic pointer.
    if (atomic_ptr.compare_exchange_strong(expected_ptr, desired_ptr, std::memory_order_seq_cst)) {
      if (expected_ptr != nullptr) {
        ar.retire(expected_ptr);
      }

      if (desired_ptr != nullptr) desired.release();
      return true;
    } else {
      return false;
    }
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

  struct counted_deleter {
    void operator()(internal::counted_object<T>* ptr) const {
      assert(ptr != nullptr);
      decrement_counter(ptr);
    }
  };

  struct counted_incrementer {
    void operator()(internal::counted_object<T>* ptr) const {
      assert(ptr != nullptr);
      increment_counter(ptr);
    }
  };

  inline static internal::acquire_retire<internal::counted_object<T>*, counted_deleter, counted_incrementer> ar{num_workers()};
};

}  // namespace parlay

#endif  // PARLAY_ATOMIC_RC_PTR_H
