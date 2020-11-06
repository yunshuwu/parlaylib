#include "gtest/gtest.h"

#include <algorithm>
#include <string>
#include <vector>

#include <parlay/atomic/atomic_rc_ptr.h>
#include <parlay/atomic/stack.h>


TEST(TestAtomicRcPointer, TestConstruction) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));
  ASSERT_EQ(string_ptr.use_count(), 2);
}

TEST(TestAtomicRcPointer, TestStore) {
  parlay::atomic_rc_ptr<std::string> atomic_string;
  atomic_string.store(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));
  ASSERT_EQ(string_ptr.use_count(), 2);
}

TEST(TestAtomicRcPointer, TestStoreNullptr) {
  parlay::atomic_rc_ptr<std::string> atomic_string;
  atomic_string.store(nullptr);
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(string_ptr, nullptr);
}

TEST(TestAtomicRcPointer, TestStoreLoadMany) {
  parlay::atomic_rc_ptr<std::string> atomic_string;
  for (size_t i = 0; i < 10000; i++) {
    auto str = std::string("Hello, string #") + std::to_string(i);
    atomic_string.store(parlay::make_shared<std::string>(str));
    auto ptr = atomic_string.load();
    ASSERT_EQ(*ptr, str);
  }
}

TEST(TestAtomicRcPointer, TestAssign) {
  parlay::atomic_rc_ptr<std::string> atomic_string;
  atomic_string = parlay::make_shared<std::string>("Hello, World");
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));
  ASSERT_EQ(string_ptr.use_count(), 2);
}

TEST(TestAtomicRcPointer, TestConversionToRcPointer) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = parlay::rc_ptr<std::string>(atomic_string);
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));
  ASSERT_EQ(string_ptr.use_count(), 2);
}

TEST(TestAtomicRcPointer, TestKeepManyCopies) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  std::vector<parlay::rc_ptr<std::string>> ptrs;

  for (size_t i = 0; i < 1000; i++) {
    // Clear the snapshots
    if (i % 100 == 0) {
      ptrs.clear();
    }
      // Swap out a new string
    else if (i % 10 == 0) {
      atomic_string.store(parlay::make_shared<std::string>(std::string("Hello, string #") + std::to_string(i)));
    }
      // Grab a snapshot
    else {
      ptrs.push_back(atomic_string.load());
      for (const auto& ptr : ptrs) {
        std::string str = *ptr;
        ASSERT_FALSE(str.empty());
      }
    }
  }
}


/*
TEST(TestAtomicRcPointer, TestCompareExchangeStrongSuccess) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));
  ASSERT_EQ(string_ptr.use_count(), 2);

  auto new_string_ptr = parlay::make_shared<std::string>("A second string");
  auto success = atomic_string.compare_exchange_strong(string_ptr, new_string_ptr);

  ASSERT_TRUE(success);
  ASSERT_EQ(atomic_string.load(), new_string_ptr);
  ASSERT_GE(string_ptr.use_count(), 1);
  ASSERT_EQ(new_string_ptr.use_count(), 2);
}

TEST(TestAtomicRcPointer, TestCompareExchangeStrongMoveSuccess) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));

  auto new_string_ptr = parlay::make_shared<std::string>("A second string");
  auto new_string_ptr_copy = new_string_ptr;
  auto success = atomic_string.compare_exchange_strong(string_ptr, std::move(new_string_ptr));

  ASSERT_TRUE(success);
  ASSERT_EQ(new_string_ptr.get(), nullptr);
  ASSERT_EQ(atomic_string.load(), new_string_ptr_copy);
  ASSERT_GE(string_ptr.use_count(), 1);
  ASSERT_EQ(new_string_ptr_copy.use_count(), 2);
}

TEST(TestAtomicRcPointer, TestCompareExchangeStrongFail) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));

  auto new_string_ptr = parlay::make_shared<std::string>("A second string");
  auto another_string_ptr = parlay::make_shared<std::string>("Hello, World");  // Not the same shared object!
  auto success = atomic_string.compare_exchange_strong(another_string_ptr, new_string_ptr);

  ASSERT_FALSE(success);
  ASSERT_EQ(atomic_string.load(), another_string_ptr);
  ASSERT_EQ(another_string_ptr.use_count(), 3);
  ASSERT_NE(atomic_string.load(), new_string_ptr);
  ASSERT_EQ(new_string_ptr.use_count(), 1);
}

TEST(TestAtomicRcPointer, TestCompareExchangeStrongMoveFail) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));

  auto new_string_ptr = parlay::make_shared<std::string>("A second string");
  auto another_string_ptr = parlay::make_shared<std::string>("Hello, World");  // Not the same shared object!
  auto success = atomic_string.compare_exchange_strong(another_string_ptr, std::move(new_string_ptr));

  ASSERT_FALSE(success);
  ASSERT_NE(new_string_ptr.get(), nullptr);
  ASSERT_EQ(new_string_ptr.use_count(), 1);
  ASSERT_EQ(atomic_string.load(), another_string_ptr);
  ASSERT_EQ(string_ptr.use_count(), 3);
  ASSERT_NE(atomic_string.load(), new_string_ptr);
}

TEST(TestAtomicRcPointer, TestCompareExchangeWeakSuccess) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));

  auto new_string_ptr = parlay::make_shared<std::string>("A second string");
  bool success;
  for (; !(success = atomic_string.compare_exchange_weak(string_ptr, new_string_ptr)); ) { }

  ASSERT_TRUE(success);
  ASSERT_EQ(atomic_string.load(), new_string_ptr);
  ASSERT_EQ(new_string_ptr.use_count(), 2);
  ASSERT_GE(string_ptr.use_count(), 1);
}

TEST(TestAtomicRcPointer, TestCompareExchangeWeakMoveSuccess) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));

  auto new_string_ptr = parlay::make_shared<std::string>("A second string");
  auto new_string_ptr_copy = new_string_ptr;
  bool success;
  for (; !(success = atomic_string.compare_exchange_weak(string_ptr, std::move(new_string_ptr))); ) { }

  ASSERT_TRUE(success);
  ASSERT_EQ(new_string_ptr.get(), nullptr);
  ASSERT_EQ(atomic_string.load(), new_string_ptr_copy);
}

TEST(TestAtomicRcPointer, TestCompareExchangeWeakFail) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));

  auto new_string_ptr = parlay::make_shared<std::string>("A second string");
  auto another_string_ptr = parlay::make_shared<std::string>("Hello, World");  // Not the same shared object!
  auto success = atomic_string.compare_exchange_weak(another_string_ptr, new_string_ptr);

  ASSERT_FALSE(success);
  ASSERT_EQ(atomic_string.load(), another_string_ptr);
  ASSERT_NE(atomic_string.load(), new_string_ptr);
}

TEST(TestAtomicRcPointer, TestCompareExchangeWeakMoveFail) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));

  auto new_string_ptr = parlay::make_shared<std::string>("A second string");
  auto another_string_ptr = parlay::make_shared<std::string>("Hello, World");  // Not the same shared object!
  auto success = atomic_string.compare_exchange_weak(another_string_ptr, std::move(new_string_ptr));

  ASSERT_FALSE(success);
  ASSERT_NE(new_string_ptr.get(), nullptr);
  ASSERT_EQ(atomic_string.load(), another_string_ptr);
  ASSERT_NE(atomic_string.load(), new_string_ptr);
}
*/

TEST(TestAtomicRcPointer, TestExchange) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));
  auto new_string_ptr = parlay::make_shared<std::string>("A second string");

  auto old_ptr = atomic_string.exchange(new_string_ptr);
}

TEST(TestAtomicRcPointer, TestCompareAndSwapCopySuccess) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));
  ASSERT_EQ(string_ptr.use_count(), 2);

  auto new_string_ptr = parlay::make_shared<std::string>("A second string");
  auto success = atomic_string.compare_and_swap(string_ptr, new_string_ptr);

  ASSERT_TRUE(success);
  ASSERT_EQ(atomic_string.load(), new_string_ptr);
  ASSERT_GE(string_ptr.use_count(), 1);
  ASSERT_EQ(new_string_ptr.use_count(), 2);
}

TEST(TestAtomicRcPointer, TestCompareAndSwapMoveSuccess) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));
  ASSERT_EQ(string_ptr.use_count(), 2);

  auto new_string_ptr = parlay::make_shared<std::string>("A second string");
  auto new_string_ptr_copy = new_string_ptr;
  auto success = atomic_string.compare_and_swap(string_ptr, std::move(new_string_ptr));

  ASSERT_TRUE(success);
  ASSERT_EQ(atomic_string.load(), new_string_ptr_copy);
  ASSERT_EQ(new_string_ptr.get(), nullptr);
  ASSERT_GE(string_ptr.use_count(), 1);
  ASSERT_EQ(new_string_ptr_copy.use_count(), 2);
}

TEST(TestAtomicRcPointer, TestCompareAndSwapCopyFail) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));

  auto new_string_ptr = parlay::make_shared<std::string>("A second string");
  auto another_string_ptr = parlay::make_shared<std::string>("Hello, World");  // Not the same shared object!
  auto success = atomic_string.compare_and_swap(another_string_ptr, new_string_ptr);

  ASSERT_FALSE(success);
  ASSERT_EQ(atomic_string.load(), string_ptr);
  ASSERT_EQ(string_ptr.use_count(), 2);
  ASSERT_EQ(another_string_ptr.use_count(), 1);
  ASSERT_NE(atomic_string.load(), new_string_ptr);
  ASSERT_EQ(new_string_ptr.use_count(), 1);
}

TEST(TestAtomicRcPointer, TestCompareAndSwapMoveFail) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));

  auto new_string_ptr = parlay::make_shared<std::string>("A second string");
  auto another_string_ptr = parlay::make_shared<std::string>("Hello, World");  // Not the same shared object!
  auto success = atomic_string.compare_and_swap(another_string_ptr, std::move(new_string_ptr));

  ASSERT_FALSE(success);
  ASSERT_EQ(atomic_string.load(), string_ptr);
  ASSERT_EQ(string_ptr.use_count(), 2);
  ASSERT_EQ(another_string_ptr.use_count(), 1);
  ASSERT_NE(atomic_string.load(), new_string_ptr);
  ASSERT_EQ(new_string_ptr.use_count(), 1);
}

TEST(TestAtomicRcPointer, TestGetSnapshot) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto snapshot = atomic_string.get_snapshot();
  atomic_string.store(parlay::make_shared<std::string>("Hello, other world"));
  ASSERT_EQ(*snapshot, std::string("Hello, World"));
}

TEST(TestAtomicRcPointer, TestSnapshotCompareAndSwapCopySuccess) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));
  ASSERT_EQ(string_ptr.use_count(), 2);

  auto new_string_ptr = parlay::make_shared<std::string>("A second string");
  auto snapshot = atomic_string.get_snapshot();
  auto success = atomic_string.compare_and_swap(snapshot, new_string_ptr);

  ASSERT_TRUE(success);
  ASSERT_EQ(atomic_string.load(), new_string_ptr);
  ASSERT_GE(string_ptr.use_count(), 1);
  ASSERT_EQ(new_string_ptr.use_count(), 2);
}

TEST(TestAtomicRcPointer, TestSnapshotCompareAndSwapMoveSuccess) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));
  ASSERT_EQ(string_ptr.use_count(), 2);

  auto new_string_ptr = parlay::make_shared<std::string>("A second string");
  auto new_string_ptr_copy = new_string_ptr;
  auto snapshot = atomic_string.get_snapshot();
  auto success = atomic_string.compare_and_swap(snapshot, std::move(new_string_ptr));

  ASSERT_TRUE(success);
  ASSERT_EQ(atomic_string.load(), new_string_ptr_copy);
  ASSERT_EQ(new_string_ptr.get(), nullptr);
  ASSERT_GE(string_ptr.use_count(), 1);
  ASSERT_EQ(new_string_ptr_copy.use_count(), 2);
}

TEST(TestAtomicRcPointer, TestSnapshotCompareAndSwapCopyFail) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));

  auto new_string_ptr = parlay::make_shared<std::string>("A second string");
  auto another_string_ptr = parlay::make_shared<std::string>("Hello, World");  // Not the same shared object!
  parlay::atomic_rc_ptr<std::string> another_atomic_string(another_string_ptr);
  ASSERT_EQ(another_string_ptr.use_count(), 2);
  auto snapshot = another_atomic_string.get_snapshot();
  auto success = atomic_string.compare_and_swap(snapshot, new_string_ptr);

  ASSERT_FALSE(success);
  ASSERT_EQ(atomic_string.load(), string_ptr);
  ASSERT_EQ(string_ptr.use_count(), 2);
  ASSERT_EQ(another_string_ptr.use_count(), 2);
  ASSERT_NE(atomic_string.load(), new_string_ptr);
  ASSERT_EQ(new_string_ptr.use_count(), 1);
}

TEST(TestAtomicRcPointer, TestSnapshotCompareAndSwapMoveFail) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));

  auto new_string_ptr = parlay::make_shared<std::string>("A second string");
  auto another_string_ptr = parlay::make_shared<std::string>("Hello, World");  // Not the same shared object!
  parlay::atomic_rc_ptr<std::string> another_atomic_string(another_string_ptr);
  ASSERT_EQ(another_string_ptr.use_count(), 2);
  auto snapshot = another_atomic_string.get_snapshot();
  auto success = atomic_string.compare_and_swap(snapshot, std::move(new_string_ptr));

  ASSERT_FALSE(success);
  ASSERT_EQ(atomic_string.load(), string_ptr);
  ASSERT_EQ(string_ptr.use_count(), 2);
  ASSERT_EQ(another_string_ptr.use_count(), 2);
  ASSERT_NE(atomic_string.load(), new_string_ptr);
  ASSERT_EQ(new_string_ptr.use_count(), 1);
}

TEST(TestAtomicRcPointer, TestGetSnapshotMany) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  std::vector<parlay::snapshot_ptr<std::string>> snapshots;

  for (size_t i = 0; i < 1000; i++) {
    // Clear the snapshots
    if (i % 100 == 0) {
      snapshots.clear();
    }
    // Swap out a new string
    else if (i % 10 == 0) {
      atomic_string.store(parlay::make_shared<std::string>(std::string("Hello, string #") + std::to_string(i)));
    }
    // Grab a snapshot
    else {
      snapshots.push_back(atomic_string.get_snapshot());
      for (const auto& snapshot : snapshots) {
        std::string str = *snapshot;
        ASSERT_FALSE(str.empty());
      }
    }
  }
}

// Build a linked list that is destructed recursively
TEST(TestAtomicRcPointer, TestRecursiveDestruct) {
  struct Node {
    int val;
    parlay::atomic_rc_ptr<Node> next;
    Node(int x) : val(x), next(nullptr) { }
  };

  parlay::atomic_rc_ptr<Node> head;

  parlay::rc_ptr<Node> cur = parlay::make_shared<Node>(0);
  head.store(cur);
  for (int i = 1; i < 100000; i++) {
    cur->next = parlay::make_shared<Node>(i);
    cur = cur->next;
  }
  cur = nullptr;

  // Force the (possibly deferred) destruction of the list
  head.store(nullptr);
  ASSERT_EQ(head.load(), nullptr);
}

// Build a binary tree whose destructor destroys the left and right
// children recursively in parallel.
TEST(TestAtomicRcPointer, TestRecursiveParallelDestruct) {
  struct Node {
    int val;
    parlay::atomic_rc_ptr<Node> left, right;
    Node(int x) : val(x), left(nullptr), right(nullptr) { }
    ~Node() {
      // Destroy in parallel
      parlay::par_do(
        [&]() { left = nullptr; },
        [&]() { right = nullptr; }
      );
    }
  };

  parlay::atomic_rc_ptr<Node> root;

  std::function<parlay::rc_ptr<Node>(int,int)> make_tree = [&](int i, int j) {
    if (i == j - 1) return parlay::make_shared<Node>(i);
    else {
      int mid = i + (j - i) / 2;
      auto root = parlay::make_shared<Node>(mid);

      // Construct children in parallel
      parlay::par_do(
        [&]() { root->left = make_tree(i, mid); },
        [&]() { root->right = make_tree(mid, j); }
      );
      return root;
    }
  };

  root.store(make_tree(0,100000));

  // Force the (possibly deferred) destruction of the tree
  root.store(nullptr);
  ASSERT_EQ(root.load(), nullptr);
}

TEST(TestAtomicRcPointer, TestStackPush) {
  parlay::atomic_stack<int> s;
  parlay::parallel_for(0, 100000, [&](int i) {
    s.push_front(i);
  });

  std::vector<int> all;
  for (int i = 0; i < 100000; i++) {
    auto res = s.pop_front();
    ASSERT_TRUE(res.has_value());
    all.push_back(res.value());
  }

  std::sort(all.begin(), all.end());
  for (int i = 0; i < 100000; i++) {
    ASSERT_EQ(all[i], i);
  }
}

TEST(TestAtomicRcPointer, TestStackPop) {
  parlay::atomic_stack<int> s;
  for (int i = 0; i < 100000; i++) {
    s.push_front(i);
  }
  std::vector<std::vector<int>> contents(parlay::num_workers());
  parlay::parallel_for(0, 100000, [&](int) {
    auto res = s.pop_front();
    ASSERT_TRUE(res.has_value());
    contents[parlay::worker_id()].push_back(res.value());
  });

  std::vector<int> all;
  for (const auto& v : contents) {
    all.insert(all.end(), v.begin(), v.end());
  }
  std::sort(all.begin(), all.end());
  for (int i = 0; i < 100000; i++) {
    ASSERT_EQ(all[i], i);
  }
}

TEST(TestAtomicRcPointer, TestStackFind) {
  parlay::atomic_stack<int> s;
  for (int i = 0; i < 10000; i++) {
    s.push_front(2*i);
  }

  parlay::parallel_for(0, 10000, [&](int i) {
    auto res = s.find(i);
    if (i % 2 == 0) ASSERT_TRUE(res);
    else ASSERT_FALSE(res);
  });
}

TEST(TestAtomicRcPointer, TestStackFindAndPush) {
  parlay::atomic_stack<int> s;
  for (int i = 0; i < 10000; i++) {
    s.push_front(2*i);
  }

  parlay::parallel_for(0, 10000, [&](int i) {
    auto res = s.find(i);
    s.push_front(2*(i + 10000));
    if (i % 2 == 0) ASSERT_TRUE(res);
    else ASSERT_FALSE(res);
  });
}

TEST(TestAtomicRcPointer, TestStackFindAndPop) {
  parlay::atomic_stack<int> s;
  for (int i = 0; i < 20000; i++) {
    s.push_front(2*i);
  }

  parlay::parallel_for(0, 10000, [&](int i) {
    auto res = s.find(i);
    s.pop_front();
    if (i % 2 == 0) ASSERT_TRUE(res);
    else ASSERT_FALSE(res);
  });
}

TEST(TestAtomicRcPointer, TestStackPushAndPop) {
  parlay::atomic_stack<int> s;
  for (int i = 0; i < 1000; i++) {
    s.push_front(2*i);
  }

  parlay::parallel_for(0, 10000, [&](int i) {
    auto res = s.pop_front();
    s.push_front(i);
    ASSERT_TRUE(res.has_value());
  });
}

TEST(TestAtomicRcPointer, TestStackPushAndPopEmpty) {
  parlay::atomic_stack<int> s;

  parlay::parallel_for(0, 10000, [&](int i) {
    s.pop_front();
    s.pop_front();
    s.push_front(i);
  });
}
