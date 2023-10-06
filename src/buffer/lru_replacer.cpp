#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) {
  capacity=num_pages;
}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lru_list_.empty()) {
    frame_id = nullptr;
    return false;
  } else {
      u_long count=0;
//      for(std::list<pair<frame_id_t,bool>>::iterator it=lru_list_.begin();it!=lru_list_.end();++it){
//        lru_list_.erase(it);
//      }
      while(!lru_list_.begin()->second){
        lru_list_.emplace_back(lru_list_.front());
        lru_list_.pop_front();
        count++;
        if(count==lru_list_.size())//所有的都比过了
          return false;
      }
      *frame_id = lru_list_.front().first;
      lru_list_.pop_front();
      lru_Map_.erase(*frame_id);
    return true;
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  if(lru_Map_.find(frame_id)!=lru_Map_.end())
  {
    lru_Map_[frame_id]->second=false;
//    lru_list_.splice(lru_list_.end(),lru_list_,lru_Map_[frame_id]);
    lru_list_.erase(lru_Map_[frame_id]);
//    lru_Map_.find(frame_id)
    lru_Map_.erase(frame_id);
    return;
  }
//  printf("No Such Frame %d Unpinned\n", frame_id);
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (lru_Map_.find(frame_id) != lru_Map_.end()) {
      lru_Map_[frame_id]->second=true;
//      printf("Target Frame %d has been unpinned\n", frame_id);
    return;
  }

  lru_list_.emplace_back(make_pair(frame_id, true));
  lru_Map_.emplace(frame_id,--lru_list_.end());
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  auto temp=lru_list_.begin();
  int count=0;
  while(temp!=lru_list_.end()){
    if(temp->second){
      count++;
    }
    temp++;
  }
  return count;
}