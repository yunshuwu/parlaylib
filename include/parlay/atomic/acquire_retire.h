
#ifndef PARLAY_ATOMIC_ACQUIRE_RETIRE_H
#define PARLAY_ATOMIC_ACQUIRE_RETIRE_H

#include <cstddef>

#include <array>
#include <atomic>
#include <vector>

#include "snapshot_ptr.h"

#include "../parallel.h"

namespace parlay {
namespace internal {

// A vector that is stored at 64-byte-aligned memory (this
// means that the header of the vector, not the heap buffer,
// is aligned to 64 bytes)
template<typename _Tp>
struct alignas(64) AlignedVector : public std::vector<_Tp> {};

// A cache-line-aligned bool to prevent false sharing
struct alignas(64) AlignedBool {
  AlignedBool() : b(false) { }
  /* implicit */ AlignedBool(bool b_) : b(b_) { }
  operator bool() const { return b; }
 private:
  bool b;
};

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

// An interface for safe memory reclamation that protects reference-counted
// resources by deferring their reference count decrements until no thread
// is still reading them.
//
// Unlike hazard pointers, acquire-retire allows multiple concurrent retires
// of the same handle, which is what makes it suitable for managing reference
// counted pointers, since multiple copies of the same reference counted pointer
// may need to be destructed (i.e., have their counter decremented) concurrently.
//
// T =          The type of the handle being protected. Must be trivially copyable
//              and pointer-like.
// Deleter =    A stateless functor whose call operator takes a handle to a free'd
//              pointer and performs the corresponding deferred decrement
// Incrementer = A stateless functor whose call operator takes a handle to a snapshotted
//               object and increments its reference count
// delay =    The maximum number of deferred destructions that will be held by
//            any one worker thread is at most delay * #threads.
// snapshot_slots = The number of additional announcement slots available for
//                  snapshot pointers. More allows more snapshots to be alive
//                  at a time, but makes reclamation slower
//
template<typename T, typename Deleter, typename Incrementer, size_t delay = 5, size_t snapshot_slots = 3>
struct acquire_retire {

  static_assert(std::is_trivially_copyable_v<T>, "T must be a trivially copyable type for acquire-retire");

 private:
  // Align to cache line boundary to avoid false sharing
  struct alignas(64) LocalSlot {
    std::atomic<T> announcement;
    size_t last_free{0};
    std::array<std::atomic<T>, snapshot_slots> snapshot_announcements{};
    LocalSlot() : announcement(nullptr) {
      for (auto& a : snapshot_announcements) {
        std::atomic_init(&a, nullptr);
      }
    }
  };

 public:
  // An RAII wrapper around an acquired handle. Automatically
  // releases the handle when the wrapper goes out of scope.
  struct acquired {
   public:
    T value;
    acquired& operator=(acquired&& other) {
      value = other.value;
      other.value = nullptr;
    }
    ~acquired() {
      if (value != nullptr) {
        slot.store(nullptr, std::memory_order_release);
      }
    }

   private:
    friend struct acquire_retire;
    std::atomic<T>& slot;
    acquired(T value_, std::atomic<T>& slot_) : value(value_), slot(slot_) {}
  };

  acquire_retire(size_t num_threads) :
      announcement_slots(num_threads), in_progress(num_threads), deferred_destructs(num_threads) {}

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

  [[nodiscard]] std::pair<T, std::atomic<T>*> protect_snapshot(const std::atomic<T>* p) {
    auto* slot = get_free_slot();
    T result;
    do {
      result = p->load(std::memory_order_seq_cst);
      slot->store(result, std::memory_order_seq_cst);
    } while (p->load(std::memory_order_seq_cst) != result);
    return std::make_pair(result, slot);
  }

  void release() {
    auto id = worker_id();
    auto& slot = announcement_slots[id].announcement;
    slot.store(nullptr, std::memory_order_release);
  }

  void retire(T p) {
    auto id = worker_id();
    deferred_destructs[id].push_back(p);
    perform_deferred_decrements();
  }

  // Perform any remaining deferred destruction. Need to be very careful
  // about additional objects being queued for deferred destruction by
  // an object that was just destructed.
  ~acquire_retire() {
    in_progress.assign(in_progress.size(), true);

    // Loop because the destruction of one object could trigger the deferred
    // destruction of another object (possibly even in another thread), and
    // so on recursively.
    while (std::any_of(deferred_destructs.begin(), deferred_destructs.end(),
                       [](const auto& v) { return !v.empty(); })) {

      // Move all of the contents from the deferred destruction lists
      // into a single local list. We don't want to just iterate the
      // deferred lists because a destruction may trigger another
      // deferred destruction to be added to one of the lists, which
      // would invalidate its iterators
      std::vector<T> destructs;
      for (auto& v : deferred_destructs) {
        destructs.insert(destructs.end(), v.begin(), v.end());
        v.clear();
      }

      // Perform all of the pending deferred destructions
      for (auto x : destructs) {
        Deleter{}(x);
      }
    }
  }

 private:
  // Apply the function f to every currently announced handle
  template<typename F>
  void scan_slots(F&& f) {
    for (const auto& announcement_slot : announcement_slots) {
      auto x = announcement_slot.announcement.load(std::memory_order_seq_cst);
      if (x != nullptr) f(x);
      for (const auto& free_slot : announcement_slot.snapshot_announcements) {
        auto y = free_slot.load(std::memory_order_seq_cst);
        if (y != nullptr) f(y);
      }
    }
  }

  [[nodiscard]] std::atomic<T>* get_free_slot() {
    auto id = worker_id();
    for (size_t i = 0; i < snapshot_slots; i++) {
      if (announcement_slots[id].snapshot_announcements[i].load(std::memory_order_seq_cst) == nullptr) {
        return std::addressof(announcement_slots[id].snapshot_announcements[i]);
      }
    }
    auto& last_free = announcement_slots[id].last_free;
    auto kick_ptr = announcement_slots[id].snapshot_announcements[last_free].load(std::memory_order_seq_cst);
    assert(kick_ptr != nullptr);
    Incrementer{}(kick_ptr);
    std::atomic<T>* return_slot = std::addressof(announcement_slots[id].snapshot_announcements[last_free]);
    last_free = (last_free + 1 == snapshot_slots) ? 0 : last_free + 1;
    return return_slot;
  }

  void perform_deferred_decrements() {
    auto id = worker_id();
    while (!in_progress[id] && deferred_destructs[id].size() >= announcement_slots.size() * delay) {
      in_progress[id] = true;
      auto deferred = AlignedVector<T>(std::move(deferred_destructs[id]));
      tiny_table<T, 1024> announced(announcement_slots.size() * (1 + snapshot_slots));
      scan_slots([&](auto reserved) { announced.insert(reserved); });

      // For a given deferred decrement, we first check if it is announced, and, if so,
      // we defer it again. If it is not announced, it can be safely applied. If an
      // object is deferred / announced multiple times, each announcement only protects
      // against one of the deferred decrements, so for each object, the amount of
      // decrements applied in total will be #deferred - #announced
      auto f = [&](auto x) {
        auto it = announced.find(x);
        if (it == nullptr) {
          Deleter{}(x);
          return true;
        } else {
          *it = nullptr;
          return false;
        }
      };

      // Remove the deferred decrements that are successfully applied
      deferred.erase(remove_if(deferred.begin(), deferred.end(), f), deferred.end());
      deferred_destructs[id].insert(deferred_destructs[id].end(), deferred.begin(), deferred.end());
      in_progress[id] = false;
    }
  }

  std::vector<LocalSlot> announcement_slots;          // Announcement array slots
  std::vector<AlignedBool> in_progress;               // Local flags to prevent reentrancy while destructing
  std::vector<AlignedVector<T>> deferred_destructs;   // Thread-local lists of pending deferred destructs
};

}  // namespace internal
}  // namespace parlay

#endif  // PARLAY_ATOMIC_ACQUIRE_RETIRE_H
