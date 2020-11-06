
#ifndef PARLAY_ATOMIC_SNAPSHOT_PTR_H_
#define PARLAY_ATOMIC_SNAPSHOT_PTR_H_

#include "rc_ptr.h"

namespace parlay {

template<typename T>
class atomic_rc_ptr;

template<typename T>
class snapshot_ptr {

 public:
  snapshot_ptr(std::nullptr_t) : ptr(nullptr), slot(nullptr) {}

  snapshot_ptr(snapshot_ptr&& other) noexcept : ptr(other.ptr), slot(other.slot) {
    other.ptr = nullptr;
    other.slot = nullptr;
  }

  snapshot_ptr& operator=(snapshot_ptr&& other) {
    clear();
    swap(other);
    return *this;
  }

  snapshot_ptr(const snapshot_ptr&) = delete;
  snapshot_ptr& operator=(const snapshot_ptr&) = delete;

  typename std::add_lvalue_reference_t<T> operator*() const { return *(ptr->get()); }

  T* get() { return (ptr == nullptr) ? nullptr : ptr->get(); }
  const T* get() const { return (ptr == nullptr) ? nullptr : ptr->get(); }

  T* operator->() { return (ptr == nullptr) ? nullptr : ptr->get(); }
  const T* operator->() const { return (ptr == nullptr) ? nullptr : ptr->get(); }

  explicit operator bool() const { return ptr != nullptr; }

  bool operator==(const snapshot_ptr<T>& other) const { return get() == other.get(); }
  bool operator!=(const snapshot_ptr<T>& other) const { return get() != other.get(); }

  void swap(snapshot_ptr& other) {
    std::swap(ptr, other.ptr);
    std::swap(slot, other.slot);
  }

  ~snapshot_ptr() { clear(); }

 private:
  friend class atomic_rc_ptr<T>;

  snapshot_ptr(internal::counted_object<T>* ptr_, std::atomic<internal::counted_object<T>*>* slot_) :
      ptr(ptr_), slot(slot_) {}

  internal::counted_object<T>* get_counted() const {
    return ptr;
  }

  void clear() {
    if (ptr != nullptr) {
      if (slot->load(std::memory_order_seq_cst) == ptr)
        slot->store(nullptr);
      else
        decrement_counter(ptr);
      ptr = nullptr;
      slot = nullptr;
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

  internal::counted_object<T>* ptr;
  std::atomic<internal::counted_object<T>*>* slot;
};

}  // namespace parlay

#endif  // PARLAY_ATOMIC_SNAPSHOT_PTR_H_
