#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) : globalDepth(0), bucketSize(size), bucketCount(1){

  bucketDirectory.emplace_back(std::make_shared<Bucket>(0));
    
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
    return std::hash<K>()(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  return this->globalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  return bucketDirectory[bucket_id]->localDepth;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  return this->bucketCount;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value){
    std::lock_guard<std::mutex> lock(this->mtx);
  auto bucket = bucketDirectory[BucketIndex(key)];

  if(bucket == nullptr || bucket->contents.find(key) == bucket->contents.end())
    return false;

  value = bucket->contents[key];
  return true;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
        std::lock_guard<std::mutex> lock(this->mtx);
    auto bucket = bucketDirectory[BucketIndex(key)];

    if(bucket == nullptr || bucket->contents.find(key) == bucket->contents.end())
      return false;

    bucket->contents.erase(key);
    return true;

}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {

    std::lock_guard<std::mutex> lock(this->mtx);
  size_t index = BucketIndex(key);
  std::shared_ptr<Bucket> bucket = bucketDirectory[index];

  // size limit, neet to split
  while(bucket->contents.size() == this->bucketSize){
    // same depth
    if(bucket->localDepth == this->globalDepth){
      // double the bucket size, do the same mapping
      size_t length = bucketDirectory.size();
      for( size_t i = 0; i < length; i++){
        bucketDirectory.push_back(bucketDirectory[i]);
      }

      globalDepth ++;
      bucketCount ++;
    }

    int mask = 1 << bucket->localDepth;

    //create 2 bucket to replace last one
    std::shared_ptr<Bucket> a = std::make_shared<Bucket>(bucket->localDepth + 1);
    std::shared_ptr<Bucket> b = std::make_shared<Bucket>(bucket->localDepth + 1);

    // replace all items in the previous bucket into this new bucket
    for( auto item : bucket->contents){
      size_t newKey = HashKey(item.first);
      if(newKey & mask){
        b->contents.insert(item);
      }else{
        a->contents.insert(item);
      }
    }
    size_t length = bucketDirectory.size();
    for(size_t i = 0; i < length; i++){
      // replace it with new one
      if(bucketDirectory[i] == bucket){
        if(i & mask){
          bucketDirectory[i] = b;
        }else{
          bucketDirectory[i] = a;
        }
      }
    }

    index = BucketIndex(key);
    bucket = bucketDirectory[index];

  }

  bucketDirectory[index]->contents[key] = value;


}

template  <typename K, typename V>
int ExtendibleHash<K, V>::BucketIndex(const K &key){
  return static_cast<int>(HashKey(key) & ((1 << globalDepth) - 1));
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
