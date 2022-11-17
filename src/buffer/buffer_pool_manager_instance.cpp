//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
    pages_[i].ResetMemory();
    pages_[i].page_id_ = INVALID_PAGE_ID;
    pages_[i].pin_count_ = 0;
    pages_[i].is_dirty_ = false;
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}
// if all frames are currently in use and not evictable return nullptr
// pick the replacement frame from either the free list or the replacer. If the replacement frame has a dirty page, you should write it back to the disk first. 
// AllocatePage() method to get a new page id, reset the memory and metadata for the new page
// Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false), update replacer_ and page_table
auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (free_list_.empty()){
    if(!replacer_->Evict(&frame_id)){
      return nullptr;
    }
    FlushPgInternal(pages_[frame_id].GetPageId());
  }
  frame_id = free_list_.front();
  free_list_.pop_front();

  pages_[frame_id].page_id_ = AllocatePage();
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;

  *page_id = pages_[frame_id].page_id_;

  page_table_->Insert(*page_id, frame_id);
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  
  return &pages_[frame_id];
}

// Return nullptr if page_id needs to be fetched from the disk but all frames are currently in use and not evictable (in another word, pinned).
// First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or the replacer
// you need to write it back to disk and update the metadata of the new page
// remember to disable eviction and record the access history of the frame like you did for NewPgImp().
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if(page_table_->Find(page_id, frame_id)) {
    pages_[frame_id].pin_count_++;
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  if (free_list_.empty()){
    if(!replacer_->Evict(&frame_id)){
      return nullptr;
    }
    FlushPgInternal(pages_[frame_id].GetPageId());
  }
  frame_id = free_list_.front();
  free_list_.pop_front();
  
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  page_table_->Insert(page_id, frame_id);
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  
  return &pages_[frame_id];

}

// If page_id is not in the buffer pool or its pin count is already 0, return false.
// update pages_ is_dirty, Decrement the pin count of a page. 
// If the pin count reaches 0, the frame should be evictable by the replacer.
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if(!page_table_->Find(page_id, frame_id) || pages_[frame_id].GetPinCount()<=0){
    return false;
  }
  if(is_dirty){
    pages_[frame_id].is_dirty_ = true;
  }
  if(--pages_[frame_id].pin_count_ == 0){
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  return FlushPgInternal(page_id);
}

// whether in page_table_
// Use the DiskManager::WritePage() method to flush a page to disk
// pin_count>0, page can't be dirty
// reset the page's memory and metadata, update free_list, replacer_, page_table_
auto BufferPoolManagerInstance::FlushPgInternal(page_id_t page_id) -> bool{
  frame_id_t frame_id;
  if(!page_table_->Find(page_id, frame_id)){
    return false;
  }
  if(pages_[frame_id].IsDirty()){
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  }
  if(pages_[frame_id].pin_count_>0){
    pages_[frame_id].is_dirty_ = false;
    return true;
  }
  
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;


  free_list_.emplace_back(frame_id);
  replacer_->Remove(frame_id);
  page_table_->Remove(page_id);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (frame_id_t i=0; i < static_cast<int>(pool_size_); i++){
    FlushPgImp(pages_[i].GetPageId());
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
 * page is pinned and cannot be deleted, return false immediately.
 *
 * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
 * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
 * imitate freeing the page on the disk.
 *
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if(!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  if(pages_[frame_id].GetPinCount()>0) {
    return false;
  }
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.emplace_back(page_id);
  
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
