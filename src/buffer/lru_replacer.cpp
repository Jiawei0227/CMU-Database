/**
 * LRU implementation
 */
#include <include/common/logger.h>
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  std::lock_guard<std::mutex> guard(mtx);

  // find this element
  if(hash.find(value) != hash.end()){
    auto itr = hash[value];
    list.erase(itr);
  }

  list.push_front(value);
  hash[value] = list.begin();

}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {

  std::lock_guard<std::mutex> guard(mtx);

  if(list.empty())
    return false;

  value = list.back();
  list.pop_back();
  hash.erase(value);

  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {

  std::lock_guard<std::mutex> guard(mtx);
  // cannot find the element
  if(this->hash.find(value) == this->hash.end())
    return false;

  auto itr = hash[value];
  list.erase(itr);
  hash.erase(value);

  return true;
}

template <typename T> size_t LRUReplacer<T>::Size() { return this->list.size(); }

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
