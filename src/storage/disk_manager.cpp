#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"
//#define ENABLE_BPM_DEBUG 1

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

[[maybe_unused]] void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

[[maybe_unused]] void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  page_id_t logic_page_id;
  uint32_t page_id = 0;
  uint32_t bitmap_id = 0;
  char *bitmap_buf = new char[PAGE_SIZE];
  auto *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_buf);
  auto *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  //检测是否达到最大存储数目
  if(meta_page->num_extents_==MAX_VALID_PAGE_ID){
      LOG(FATAL)<<"Maximum pages reached"<<std::endl;
      return INVALID_PAGE_ID;
  }
  // 第一次创建，不存在Bitmap，则需要创建Bitmap
  if(meta_page->num_extents_==0){
    memset(bitmap_buf,0,PAGE_SIZE);
    bitmap->AllocatePage(page_id);
    logic_page_id = static_cast<page_id_t>(page_id + bitmap_id * BITMAP_SIZE);
    // 获得逻辑页号后之后,转换为物理页号，向Disk和Disk Meta Page中写入数据
    WritePhysicalPage(static_cast<page_id_t>(1 + bitmap_id * (BITMAP_SIZE + 1)),
                      bitmap_buf);
    meta_page->num_allocated_pages_++;
    meta_page->extent_used_page_[bitmap_id]++;
    meta_page->num_extents_++;
    ReadPhysicalPage(static_cast<page_id_t>(1 + bitmap_id * (BITMAP_SIZE + 1)),
                     bitmap_buf);
    delete[] bitmap_buf;
    return logic_page_id;
  }
  else{
    //非第一次创建，检查分区是否已满
    for(bitmap_id=0;bitmap_id<meta_page->num_extents_;bitmap_id++){
      if(meta_page->extent_used_page_[bitmap_id]==BITMAP_SIZE)//该位图页满了
        continue;
      ReadPhysicalPage(static_cast<page_id_t>(1 + bitmap_id * (BITMAP_SIZE + 1)),
                       bitmap_buf);
      bitmap->AllocatePage(page_id);
      logic_page_id = static_cast<page_id_t>(page_id + bitmap_id * BITMAP_SIZE);
      WritePhysicalPage(static_cast<page_id_t>(1 + bitmap_id * (BITMAP_SIZE + 1)),
                        bitmap_buf);
//      ASSERT(IsPageFree(logic_page_id),"Page Allocation Failed\n");
      ReadPhysicalPage(static_cast<page_id_t>(1 + bitmap_id * (BITMAP_SIZE + 1)),
                       bitmap_buf);
      meta_page->num_allocated_pages_++;
      meta_page->extent_used_page_[bitmap_id]++;
      delete[] bitmap_buf;
      return logic_page_id;
    }
    //此时所有分区已经遍历，需要新建分区
    memset(bitmap_buf,0,PAGE_SIZE);
    bitmap->AllocatePage(page_id);
    logic_page_id = static_cast<page_id_t>(page_id + bitmap_id * BITMAP_SIZE);
    // 获得逻辑页号后之后,转换为物理页号，向Disk和Disk Meta Page中写入数据
    WritePhysicalPage(static_cast<page_id_t>(1 + bitmap_id * (BITMAP_SIZE + 1)),
                      bitmap_buf);
    meta_page->num_allocated_pages_++;
    meta_page->extent_used_page_[bitmap_id]++;
    meta_page->num_extents_++;
    ReadPhysicalPage(static_cast<page_id_t>(1 + bitmap_id * (BITMAP_SIZE + 1)),
                     bitmap_buf);
    delete[] bitmap_buf;
    return logic_page_id;
  }
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  if (IsPageFree(logical_page_id)) {
    //    LOG(INFO)<<"Target page is Free."<<std::endl;
    return;
  }
  uint32_t bitmap_id = logical_page_id / BITMAP_SIZE;
  uint32_t page_offset;
  char *bitmap_buff = new char[PAGE_SIZE];
  auto *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

  ReadPhysicalPage(static_cast<page_id_t>(1 + bitmap_id * (BITMAP_SIZE + 1)), bitmap_buff);
  //  ASSERT(reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_buff)->DeAllocatePage(page_offset), "DeAllocatePage
  //  Filed.");
  reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_buff)->DeAllocatePage(page_offset);
  WritePhysicalPage(static_cast<page_id_t>(1 + bitmap_id * (BITMAP_SIZE + 1)), bitmap_buff);

  delete[] bitmap_buff;
  meta_page->extent_used_page_[bitmap_id]--;
  meta_page->num_allocated_pages_--;
}

/**
 * TODO: Student Implement
 *  可能存在问题，对于磁盘内容的直接访问可能会导致指针报错或者内存访问权限错误
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  //  return false;
  uint32_t bitmap_id = logical_page_id / BITMAP_SIZE;
  uint32_t page_offset = logical_page_id - bitmap_id * BITMAP_SIZE;
//  BitmapPage<PAGE_SIZE>*bitmap= nullptr;
  char* bitmap=new char[PAGE_SIZE];
  // 令bitmap为将要回收页面所存在的位图页
  ReadPhysicalPage(1+bitmap_id*(BITMAP_SIZE+1),bitmap);
  return reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap)->IsPageFree(page_offset);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return static_cast<page_id_t>(logical_page_id + logical_page_id / BITMAP_SIZE + 2);
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}