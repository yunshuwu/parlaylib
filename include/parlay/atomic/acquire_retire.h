
#ifndef PARLAY_ATOMIC_ACQUIRE_RETIRE_H
#define PARLAY_ATOMIC_ACQUIRE_RETIRE_H

#include <cstddef>

#include <array>
#include <atomic>
#include <vector>

#include "../parallel.h"

namespace parlay {
namespace internal {

// An array that begins at 64-byte-aligned memory
template<typename _Tp, size_t sz>
struct alignas(64) AlignedArray : public std::array<_Tp, sz> {
  size_t size{0};
  void push_back(_Tp&& p) { (*this)[size++] = std::move(p); }
};

// A vector that is stored at 64-byte-aligned memory (this
// means that the header of the vector, not the heap buffer,
// is aligned to 64 bytes)
template<typename _Tp>
struct alignas(64) AlignedVector : public std::vector<_Tp> {};

// A chaining hashtable optimized for storing a small number of entries.
// Allows duplicate entries. B is the number of top-level buckets
// to use. Should be set to a power of two that is roughly four
// times larger than the maximum number of elements you expect
// to insert.
//
// T =    The type of the item stored in the hashtable
// B =    The number of buckets to use for the hashtable. A power
//        of two roughly four times bigger than N is a good choice.
//        B does not stricly have to be larger than N, but it is
//        strongly recommended for good performance.
// Hash = A functor that computes the hash of an element of type T
template<typename T, size_t B, typename Hash = std::hash<T>>
struct tiny_table {
 private:
  using size_type = size_t;
  using small_index = unsigned short;

  struct Entry {
    T value;
    small_index next;
  };

  size_t size;
  std::vector<Entry> entries;
  std::array<small_index, B> table = {0};

 public:
  // Create a table of maximum capacity N
  tiny_table(size_t N) : size(0), entries(N) {}

  // Insert the value p into the hashtable, increasing
  // its size by one. Duplicate values are allowed.
  // Requires that the table is not already full.
  void insert(T p) {
    assert(size < entries.size());
    entries[size].value = std::move(p);
    size_t pos = Hash()(p) % B;
    entries[size].next = table[pos];
    table[pos] = size + 1;
    size++;
  }

  // Return a pointer to a stored copy of p inside
  // the table if it exists. If multiple copies of
  // p exist in the table, an arbitrary one is found.
  // Returns nullptr if no copy of p is present.
  T* find(const T& p) {
    small_index pos = Hash()(p) % B;
    small_index id = table[pos];
    while (id != 0) {
      if (entries[id - 1].value == p) {
        return &(entries[id - 1].value);
      }
      id = entries[id - 1].next;
    }
    return nullptr;
  }
};

// An interface for safe memory reclamation that protects atomic access to resources
// by deferring their destruction until no thread is still reading them.
//
// Unlike hazard pointers, acquire-retire allows multiple concurrent retires of
// the same handle, which makes it suitable for managing reference counted pointers,
// since multiple copies of the same reference counted pointer may need to be
// destructed (i.e., have their counter decremented) concurrently.
//
// T =        The type of the handle being protected. Must be trivially copyable.
// Deleter =  A stateless functor whose call operator takes a handle to a free'd
//            resource and performs the corresponding deferred destruction
// Empty =    A sentinel value that corresponds to an empty value of type T
// delay =    The maximum number of deferred destructions that will be held by
//            any one worker thread is at most delay * #threads.
//
template<typename T, typename Deleter, T Empty = T{}, size_t delay = 5>
struct acquire_retire {

  static_assert(std::is_trivially_copyable_v<T>, "T must be a trivially copyable type for acquire-retire");

 private:
  // Align to cache line boundary to avoid false sharing
  struct alignas(64) LocalSlot {
    std::atomic<T> announcement;
    LocalSlot() : announcement(Empty) { }
  };

 public:

  // An RAII wrapper around an acquired handle. Automatically
  // releases the handle when the wrapper goes out of scope.
  struct acquired {
   public:
    T value;
    acquired& operator=(acquired&& other) { value = other.value; other.value = Empty; }
    ~acquired() { if (value != Empty) { slot.store(Empty, std::memory_order_release); } }
   private:
    friend struct acquire_retire;
    std::atomic<T>& slot;
    acquired(T value_, std::atomic<T>& slot_) : value(value_), slot(slot_) { }
  };

  acquire_retire(size_t num_threads) : announcement_slots(num_threads), deferred_destructs(num_threads) { }

  [[nodiscard]] acquired acquire(const std::atomic<T>* p) {
    auto id = worker_id();
    T result;
    do {
      result = p->load(std::memory_order_seq_cst);
      announcement_slots[id].announcement.store(result, std::memory_order_seq_cst);
    } while (p->load(std::memory_order_seq_cst) != result);
    return {result, announcement_slots[id].announcement};
  }

  // Like acquire, but assuming that the caller already has a
  // copy of the handle and knows that it is protected
  [[nodiscard]] acquired reserve(T p) {
    auto id = worker_id();
    announcement_slots[id].announcement.store(p, std::memory_order_seq_cst);
    return {p, announcement_slots[id].announcement};
  }

  void release() {
    auto id = worker_id();
    auto& slot = announcement_slots[id].announcement;
    slot.store(Empty, std::memory_order_release);
  }

  void retire(T p) {
    auto id = worker_id();
    deferred_destructs[id].push_back(p);
    perform_deferred_decrements();
  }

  ~acquire_retire() {
    for (const auto& dds : deferred_destructs) {
      for (auto x : dds) {
        Deleter()(x);
      }
    }
  }

 private:

  void perform_deferred_decrements() {
    auto id = worker_id();
    if (deferred_destructs[id].size() == announcement_slots.size() * delay) {
      tiny_table<T, 512> announced(announcement_slots.size());
      for (const auto& announcement_slot : announcement_slots) {
        auto& slot = announcement_slot.announcement;
        auto reserved = slot.load(std::memory_order_seq_cst);
        if (reserved != Empty) {
          announced.insert(reserved);
        }
      }
      auto f = [&](auto x) {
        auto it = announced.find(x);
        if (it == nullptr) {
          Deleter()(x);
          return true;
        } else {
          *it = Empty;
          return false;
        }
      };

      // Remove the deferred decrements that were successfully applied
      deferred_destructs[id].erase(
          deferred_destructs[id].begin(),
          remove_if(deferred_destructs[id].begin(), deferred_destructs[id].end(), f));
    }
  }

  std::vector<LocalSlot> announcement_slots;
  std::vector<AlignedVector<T>> deferred_destructs;
};

}  // namespace internal
}  // namespace parlay

#endif  // PARLAY_ATOMIC_ACQUIRE_RETIRE_H
