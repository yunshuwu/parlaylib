#include "gtest/gtest.h"

#include <parlay/atomic/atomic_rc_ptr.h>

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

TEST(TestAtomicRcPointer, TestExchange) {
  parlay::atomic_rc_ptr<std::string> atomic_string(parlay::make_shared<std::string>("Hello, World"));
  auto string_ptr = atomic_string.load();
  ASSERT_EQ(*string_ptr, std::string("Hello, World"));
  auto new_string_ptr = parlay::make_shared<std::string>("A second string");

  auto old_ptr = atomic_string.exchange(new_string_ptr);
}

TEST(TestAtomicRcPointer, TestCompareAndSwapSuccess) {
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

TEST(TestAtomicRcPointer, TestCompareAndSwapFail) {
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
