#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <deque>
#include <list>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

private:
  // add your own private member variables here
 //  采用双向链表和哈希表的结构实现LRU_list_，能够很方便地进行节点的插入，删除和变更
 // 我们将最近最少使用的页帧放到front位置
  uint32_t capacity;
  std::list<pair<frame_id_t,bool>> lru_list_;//bool表示该页是否可用可替换
//  std::deque<frame_id_t >lru_list_;
//  std::unordered_map<frame_id_t ,std::deque<frame_id_t>::iterator> lru_Map_;//Hash表，用于索引，找到链表中的对应对象
  std::unordered_map<frame_id_t ,std::list<pair<frame_id_t,bool>>::iterator> lru_Map_;//Hash表，用于索引，找到链表中的对应对象
};
#endif  // MINISQL_LRU_REPLACER_H

//#ifndef MINISQL_CLOCK_REPLACER_H
//#define MINISQL_CLOCK_REPLACER_H
//class ClockReplacer: public Replacer{
// public:
//  explicit ClockReplacer(size_t num_pages);
//
//  ~ClockReplacer() override;
//
//  bool Victim(frame_id_t *frame_id) override;
//
//  void Pin(frame_id_t frame_id) override;
//
//  void Unpin(frame_id_t frame_id) override;
//
//  size_t Size() override;
//};
//#endif