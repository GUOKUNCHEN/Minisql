#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  if (page_id == INVALID_PAGE_ID) {
    LOG(WARNING) << "Target page is invalid" << std::endl;
    return nullptr;
  }
  if (disk_manager_->IsPageFree(page_id)) {
    LOG(WARNING) << "Target page isn't Allocated" << std::endl;
    return nullptr;
  }
  // 当对象已经在内存中时
  //  1.1    If P exists, pin it and return it immediately.
  if (page_table_.find(page_id) != page_table_.end()) {
    replacer_->Pin(page_table_[page_id]);
    pages_[page_table_[page_id]].pin_count_++;
    return pages_ + page_table_[page_id];
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 还未被page_table_跟踪，需要从磁盘中加载
  else {
    frame_id_t frame_R_id;
    // 优先从free_list中加载
    if (!free_list_.empty()) {
      frame_R_id = free_list_.front();
      free_list_.pop_front();
      // 从磁盘加载数据到内存
      disk_manager_->ReadPage(page_id, pages_[frame_R_id].data_);
      pages_[frame_R_id].page_id_ = page_id;
      page_table_.insert(make_pair(page_id, frame_R_id));
      // 此时从磁盘加载成功，执行Pin操作
      replacer_->Pin(frame_R_id);
      pages_[frame_R_id].pin_count_++;
      return &pages_[frame_R_id];
    }
    // 从replacer中加载
    else {
      if (replacer_->Victim(&frame_R_id)) {
        // 2.     If R is dirty, write it back to the disk.
        if (pages_[frame_R_id].IsDirty()) {
          disk_manager_->WritePage(pages_[frame_R_id].page_id_, pages_[frame_R_id].data_);
          pages_[frame_R_id].is_dirty_ = false;
        }
        // 3.     Delete R from the page table and insert P.
        page_table_.erase(pages_[frame_R_id].page_id_);
        //        free_list_.push_front(frame_R_id);
        page_table_.insert(make_pair(page_id, frame_R_id));

        // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
        disk_manager_->ReadPage(page_id, pages_[frame_R_id].data_);
        pages_[frame_R_id].page_id_ = page_id;
        replacer_->Pin(frame_R_id);
        pages_[frame_R_id].pin_count_++;
        return &pages_[frame_R_id];
      } else {
        LOG(WARNING) << "No usable frame exists." << std::endl;
      }
    }
  }

  return nullptr;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  frame_id_t frame_id = INVALID_PAGE_ID;
  // 此时成功使用AllocatePage获取到一个可用的page
  //  2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  if (!free_list_.empty()) {
    if ((page_id = AllocatePage()) == INVALID_PAGE_ID) {
      LOG(WARNING) << "AllocatePage() Failed." << std::endl;
      return nullptr;
    }
    frame_id = free_list_.front();
    free_list_.pop_front();
    // 3.   Update P's metadata, zero out memory and add P to the page table.
    // 将该页添加到page_table_中跟踪
    page_table_.insert(make_pair(page_id, frame_id));
    pages_[frame_id].ResetMemory();
    // 4.   Set the page ID output parameter. Return a pointer to P.
    pages_[frame_id].page_id_ = page_id;
    replacer_->Pin(frame_id);
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  } else if (replacer_->Victim(&frame_id)) {
    // 检测是否脏页，决定是否写回磁盘
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
      pages_[frame_id].is_dirty_ = false;
    }
    page_table_.erase(pages_[frame_id].page_id_);
    //New_Page的申请需要在replacer取出替换页后进行，不然会出现不符合规则的现象
    if ((page_id = AllocatePage()) == INVALID_PAGE_ID) {
      LOG(WARNING) << "AllocatePage() Failed." << std::endl;
      return nullptr;
    }
    // 3.   Update P's metadata, zero out memory and add P to the page table.
    page_table_.insert(make_pair(page_id, frame_id));
    pages_[frame_id].ResetMemory();
    // 4.   Set the page ID output parameter. Return a pointer to P.
    pages_[frame_id].page_id_ = page_id;
    replacer_->Pin(frame_id);
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  } else {
    // 1.   If all the pages in the buffer pool are pinned, return nullptr.
    // 如果replacer中也全都为pinned页面，则说明所有页面都为pinned
//    LOG(WARNING) << "No page can be used" << std::endl;
//    // 需要将先前分配的page给取消掉
//    disk_manager_->DeAllocatePage(page_id);
    return nullptr;
  }
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!

  // 1.   Search the page table for the requested page (P).
  if (page_table_.find(page_id) == page_table_.end()) {
    // 1.   If P does not exist, return true.
    return true;
  } else if (pages_[page_table_[page_id]].pin_count_ > 0) {
    // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
    LOG(WARNING) << "Target page has non-zero pin-count" << std::endl;
    return false;
  } else {
    // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free
    // list.
    DeallocatePage(page_id);
    pages_[page_table_[page_id]].is_dirty_ = false;
    pages_[page_table_[page_id]].page_id_ = INVALID_PAGE_ID;
    pages_[page_table_[page_id]].pin_count_ = 0;
    pages_[page_table_[page_id]].ResetMemory();
    // 将删除的页按物理页顺序插入到free_list_对应位置中
    free_list_.insert(std::lower_bound(free_list_.begin(), free_list_.end(), page_table_[page_id]),
                      page_table_[page_id]);
    page_table_.erase(page_id);
    return true;
  }
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if (page_id == INVALID_PAGE_ID) {
    LOG(WARNING) << "Target Page is Invalid" << std::endl;
  } else {
    if (page_table_.find(page_id) != page_table_.end()) {
      if (pages_[page_table_[page_id]].pin_count_ > 0) {
        pages_[page_table_[page_id]].is_dirty_ = is_dirty;
        if ((--pages_[page_table_[page_id]].pin_count_) == 0) {
          replacer_->Unpin(page_table_[page_id]);
        }
        return true;
      } else {
        LOG(INFO) << "It is Unpinned" << std::endl;
        return false;
      }
    } else {
      LOG(WARNING) << "This page don't exist in page_table_";
      return false;
    }
  }
  return false;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  //  for(unordered_map<page_id_t,frame_id_t>::iterator it = page_table_.begin();it!=page_table_.end();++it){
  //    disk_manager_->WritePage(pages_[it->second].page_id_,pages_[it->second].data_);
  //    pages_[it->second].is_dirty_=false;
  //  }
  if (page_table_.find(page_id) != page_table_.end()) {
    disk_manager_->WritePage(pages_[page_table_[page_id]].page_id_, pages_[page_table_[page_id]].data_);
    pages_[page_table_[page_id]].is_dirty_ = false;
    return true;
  }
  return false;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) { return disk_manager_->IsPageFree(page_id); }

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}