
#ifndef PARLAY_ATOMIC_STACK_H
#define PARLAY_ATOMIC_STACK_H

#include "atomic_rc_ptr.h"
#include "rc_ptr.h"

namespace parlay {

// A concurrent stack that uses parlay::atomic_rc_ptr.
template<typename T>
class atomic_stack {

  struct Node {
    T t;
    rc_ptr<Node> next;
    Node(T t_) : t(std::move(t_)) { }
  };

  atomic_rc_ptr<Node> head;

  atomic_stack(atomic_stack&) = delete;
  void operator=(atomic_stack) = delete;

 public:
  atomic_stack() = default;
  ~atomic_stack() = default;

  bool find(T t) {
    // Holding a snapshot protects the entire list from
    // destruction while we are reading it. The alternative
    // is to copy rc_ptrs all the way down, which allows
    // the list to be destroyed while the find is in progress
    // but will slow down find by requiring it to perform
    // lots of reference count increments and decrements.
    auto ss = head.get_snapshot();
    auto node = ss.get();
    while (node && node->t != t)
      node = node->next.get();
    return node != nullptr;
  }

  void push_front(T t) {
    auto new_node = parlay::make_shared<Node>(t);
    auto& next_ptr = new_node->next;
    new_node->next = std::move(new_node);
    head.swap(next_ptr);
  }

  std::optional<T> front() {
    auto ss = head.get_snapshot();
    if (ss) return {ss->t};
    else return {};
  }

  std::optional<T> pop_front() {
    auto ss = head.get_snapshot();
    // Note: This CAS only increments the reference
    // count of ss->next if it succeeds.
    while (ss && !head.compare_and_swap(ss, ss->next)) {
      // Note. This could be a tiny bit more efficient since
      // get_snapshot() might not reuse the same slot that ss
      // is currently holding. We could add a method like
      // ss.update(head) that grabs a new snapshot but uses
      // the existing slot.
      ss = head.get_snapshot();
    }
    if (ss) return {ss->t};
    else return {};
  }
};

}  // namespace parlay

#endif  // PARLAY_STACK_H
