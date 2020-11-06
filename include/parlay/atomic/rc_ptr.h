
#ifndef PARLAY_RC_PTR_H_
#define PARLAY_RC_PTR_H_

#include <atomic>

namespace parlay {

template<typename T>
class atomic_rc_ptr;

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
    if (tmp) decrement_counter(tmp);
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

  void swap(rc_ptr& other) {
    std::swap(ptr, other.ptr);
  }

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

}

#endif  // PARLAY_RC_PTR_H_
