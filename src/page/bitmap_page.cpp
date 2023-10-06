#include "page/bitmap_page.h"

#include "glog/logging.h"

#define __ofs_left_shf_(offset) (7 - (offset % 8))
#define __char_field_(offset) (offset / 8)
//str为检测的char数组，offset为检测的bit位
#define __bit_1_check_(str,offset) str[__char_field_(offset)] & (1 << (__ofs_left_shf_(offset)))
//str为设置的对象，offset为设置的位
#define __bit_set_1_(str,offset) str[__char_field_(offset)]|=(1<<(__ofs_left_shf_(offset)))

template <size_t Pagesize>
void BitmapPage<Pagesize>::Init(){
  this->page_allocated_=0;
  this->next_free_page_=0;
  memset(bytes,0,MAX_CHARS);
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  unsigned int PageMaxAmount = BitmapPage::GetMaxSupportedSize();
  bool isFind=false;
  bool page_isFree;
  if (page_allocated_ == PageMaxAmount)
    return false;
  else {
    while((page_allocated_+1)<=PageMaxAmount){
      //检测当前next_free_page_的状态是否正确
      page_isFree= !(__bit_1_check_(bytes,next_free_page_));
      //未分配好的同时，next_free_page_也没使用
      if (!isFind&&page_isFree)
      {
        isFind=true;
        page_offset=next_free_page_;
        __bit_set_1_(bytes,page_offset);
        page_allocated_++;
      }
      //已经分配同时已找到新的未分配free_page_
      else if(isFind&&page_isFree){
        return true;
      }
      else{
        (next_free_page_ < PageMaxAmount) ? next_free_page_++ : next_free_page_ = 0;
      }
    }
  }
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (__bit_1_check_(bytes,page_offset)) {
    bytes[__char_field_(page_offset)] &= (~(1 << __ofs_left_shf_(page_offset)));
    next_free_page_=page_offset;
    page_allocated_--;
    return true;
  } else {
//    LOG(WARNING) << "Target Page" << page_offset << " doesn't exist" << std::endl;
    return false;
  }
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  //  LOG(INFO)<<"(char_field,ofs_l_shf)=("<<__char_field_(page_offset)<<","<<__ofs_left_shf_(page_offset)<<")"<<std::endl;
  //  LOG(INFO)<<(bytes[__char_field_(page_offset)] & (1<<__ofs_left_shf_(page_offset)))<<std::endl;
  return !((bool)(__bit_1_check_(bytes,page_offset)));//非!的优先级相当高，容易出错
//  return ((bool)!(bytes[__char_field_(page_offset)] & (1 << __ofs_left_shf_(page_offset))));
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;