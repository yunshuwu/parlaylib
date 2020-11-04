
#ifndef PARLAY_ATOMIC_ACQUIRE_RETIRE_H
#define PARLAY_ATOMIC_ACQUIRE_RETIRE_H

#include <cstddef>

#include <array>
#include <atomic>
#include <vector>

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
// T =        The type of the handle being protected. Must be a pointer type, or
//            compatible with pointer types (at least must be assignable to nullptr)
// Deleter =  A stateless functor whose call operator takes a handle to a free'd
//            resource and performs the corresponding deferred destruction
// delay =    The maximum number of deferred destructions that will be held by
//            any one worker thread is at most delay * #threads.
//
template<typename T, typename Deleter, size_t delay = 5>
struct acquire_retire {

  // Align to cache line boundary to avoid false sharing
  struct alignas(64) LocalSlot {
    std::atomic<T> announcement;
  };

  acquire_retire(size_t num_threads) : announcement_slots(num_threads), deferred_decrements(num_threads) { }

  T acquire(const std::atomic<T>* p, size_t worker_id) {
    T result;
    do {
      result = p->load(std::memory_order_seq_cst);
      auto& slot = announcement_slots[worker_id].announcement;
      slot.store(result, std::memory_order_seq_cst);
    } while (p->load(std::memory_order_seq_cst) != result);
    return result;
  }

  void release(size_t worker_id) {
    auto& slot = announcement_slots[worker_id].announcement;
    slot.store(nullptr, std::memory_order_release);
  }

  void retire(T p, size_t worker_id) { deferred_decrements[worker_id].push_back(p); }

  void perform_deferred_decrements(size_t worker_id) {
    if (deferred_decrements[worker_id].size() == announcement_slots.size() * delay) {
      tiny_table<T, 512> announced(announcement_slots.size());
      for (const auto& announcement_slot : announcement_slots) {
        auto& slot = announcement_slot.announcement;
        auto reserved = slot.load(std::memory_order_seq_cst);
        if (reserved != nullptr) {
          announced.insert(reserved);
        }
      }
      auto f = [&](auto x) {
        auto it = announced.find(x);
        if (it == nullptr) {
          Deleter()(x);
          return true;
        } else {
          *it = nullptr;
          return false;
        }
      };

      // Remove the deferred decrements that were successfully applied
      deferred_decrements[worker_id].erase(deferred_decrements[worker_id].begin(),
                                           remove_if(deferred_decrements[worker_id].begin(),
                                                     deferred_decrements[worker_id].end(), f));
    }
  }

  ~acquire_retire() {
    for (const auto& dds : deferred_decrements) {
      for (auto x : dds) {
        Deleter()(x);
      }
    }
  }

  std::vector<LocalSlot> announcement_slots;
  std::vector<AlignedVector<T>> deferred_decrements;
};

}  // namespace internal
}  // namespace parlay

#endif  // PARLAY_ATOMIC_ACQUIRE_RETIRE_H
