/**
 * Program Title: Buffer Manager
 * File Purpose: Heart of the buffer manager, provides a interface for user
 * Authors & ID: AKOVI BERLOS MENSAH
 *               Yuanbo Zhang(9080344840)
 *               Quming Wang(9079581147);
 * 
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb
{

/**
* Class constructor.
* Allocates an array for the buffer pool with bufs page frames and a corresponding
* BufDesc table
*/
BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs)
{
	bufDescTable = new BufDesc[bufs];

	for (FrameId i = 0; i < bufs; i++)
	{
		bufDescTable[i].frameNo = i;
		bufDescTable[i].valid = false;
	}

	bufPool = new Page[bufs];

	int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
	hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

	clockHand = bufs - 1;
}

/**
* Flushes out all dirty pages and deallocates the buffer pool and the BufDesc table.
*/
BufMgr::~BufMgr()
{
    // Flushes out all dirty pages
    for(int i = 0; i < numBufs; i++){
        BufDesc* frame = &bufDescTable[i];
        if(frame->dirty){
          // flush to disk
          frame->file->writePage(*(bufPool + frame->frameNo));
        }
    }
    // Deallocate
	delete[] bufPool;
    delete[] bufDescTable;
}

/**
 * Advance clock to next frame in the buffer pool
 */
void BufMgr::advanceClock()
{
	clockHand = (clockHand + 1) % numBufs;
}

/**
* Allocate a free frame. If necessary, writing a dirty page back to disk
*
* @param frame   Frame reference, frame ID of allocated frame returned via this variable
* @throws BufferExceededException If no such buffer is found which can be allocated
*/
void BufMgr::allocBuf(FrameId &frame)
{
 // checking for if all pages are arepinned
  bool unpinned = false;
  for(int i = 0; i < numBufs; i++){
    if(bufDescTable[i].pinCnt <= 0){
      // found an unpinned frame
      unpinned = true;
      break;
    }
  }
  if(unpinned == false) 
  throw BufferExceededException();

  //unsigned int startHand = clockHand;
  while(true){
    advanceClock();
    BufDesc* frameInfo = &bufDescTable[clockHand];
    if(frameInfo->valid){
      if(frameInfo->refbit){
	frameInfo->refbit = false;
	continue;
      } else {
	if(frameInfo->pinCnt > 0){
	  continue;
	} else {
	  if(frameInfo->dirty){
	    // flush page to disk
          // frameInfo->file->writePage(frameInfo->pageNo, *(bufPool + frameInfo->frameNo));
          // private?
           frameInfo->file->writePage(*(bufPool + frameInfo->frameNo));
	  }
	  break;
	}
      }
    } else {
      break;
    }
  }
  // set frame
  if(bufDescTable[clockHand].valid){
    hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
  }
  bufDescTable[clockHand].Clear();
  frame = bufDescTable[clockHand].frameNo;
  return;

}

/**
* Reads the given page from the file into a frame and returns the pointer to page.
* If the requested page is already present in the buffer pool pointer to that frame is returned
* otherwise a new frame is allocated from the buffer pool for reading the page.
*
* @param file    File object
* @param PageNo  Page number in the file to be read
* @param page  	 Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.
*/
void BufMgr::readPage(File *file, const PageId pageNo, Page *&page)
{
	FrameId frameNo;
	try
	{
		hashTable->lookup(file, pageNo, frameNo);
		page = &bufPool[frameNo];
		//Increment pin count and set reference bit to 1
		bufDescTable[frameNo].pinCnt++;
		bufDescTable[frameNo].refbit = 1;
	}
	catch (const HashNotFoundException &e)
	{
		// lookup trows HashNotFoundException since the page is not found in
		//in the buffer pool
		allocBuf(frameNo);					//allocate a buffer frame
		Page page_red = file->readPage(pageNo); //read page from disk to mem
		bufPool[frameNo] = page_red;
		page = &bufPool[frameNo];
		hashTable->insert(file, pageNo, frameNo); // insert the page in the hashtable
		bufDescTable[frameNo].Set(file, pageNo);
	}
}

/**
* Unpin a page from memory since it is no longer required for it to remain in memory.
*
* @param file   	File object
* @param PageNo     Page number
* @param dirty		True if the page to be unpinned needs to be marked dirty	
* @throws  PageNotPinnedException If the page is not already pinned
*/
void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
{
	FrameId frameNo;
	try
	{ //try to find the page
		hashTable->lookup(file, pageNo, frameNo);
		int pin_count = bufDescTable[frameNo].pinCnt;
		//Throws PAGENOTPINNED if the pin count is already 0
		if (pin_count == 0)
			throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		//if dirty == true, sets the dirty bit
		if (dirty == true)
			bufDescTable[frameNo].dirty = dirty;
		//Decrements the pinCnt of the frame containing (file, PageNo)
		bufDescTable[frameNo].pinCnt -= 1;
	}
	catch (const HashNotFoundException &e)
	{
		//Does nothing if page is not found in the Hashtable lookup
	}
}

/**
* Allocates a new, empty page in the file and returns the Page object.
* The newly allocated page is also assigned a frame in the buffer pool.
*
* @param file    File object
* @param PageNo  Page number. The number assigned to the page in the file is returned via this reference.
* @param page    Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
*/
void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
{
    FrameId frameNo;
    Page p = file->allocatePage(); //store the newly allocated page in p
    allocBuf(frameNo);           //obtain a buffer pool frame
    bufPool[frameNo] = p;
    //returns both the page number of the newly allocated page
    page = &bufPool[frameNo];
    pageNo = page->page_number();
    hashTable->insert(file, pageNo, frameNo); //insert entry in the Hashtable
    bufDescTable[frameNo].Set(file, pageNo);
}

/**
* Delete page from file and also from buffer pool if present.
* Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
*
* @param file    File object
* @param PageNo  Page number
*/
void BufMgr::disposePage(File *file, const PageId pageNo)
{
    FrameId frameNo;
    try
    {    //makes sure that if the page to be deleted is allocated a frame in the buffer pool, that frame
        //is freed and correspondingly entry from hash table is also removed
        hashTable->lookup(file, pageNo, frameNo);
        hashTable->remove(bufDescTable[frameNo].file, bufDescTable[frameNo].pageNo);
        bufDescTable[frameNo].Clear();
    }
    catch (const HashNotFoundException &e)
    {
    }
    file->deletePage(pageNo); //delete the page from the file
}

/**
* Writes out all dirty pages of the file to disk.
* All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
* Otherwise Error returned.
*
* @param file   File object
* @throws  PagePinnedException If any page of the file is pinned in the buffer pool 
* @throws BadBufferException If any frame allocated to the file is found to be invalid
*/
void BufMgr::flushFile(const File *file)
{
	 // first check if all pages of this file are unpinned
      File* pFile = const_cast<File*>(file);
  for(int i = 0; i < numBufs; i++){
    BufDesc* frame = &bufDescTable[i];
    if(frame->file == pFile){
      if(frame->pinCnt > 0)
        throw PagePinnedException(frame->file->filename(), frame->pageNo, frame->frameNo);
      if(!frame->valid)
          throw BadBufferException(frame->frameNo, frame->dirty, frame->valid, frame->refbit);
      if(frame->dirty){
        // flush to disk
        frame->file->writePage(*(bufPool + frame->frameNo));
        frame->dirty = false;
      }
      hashTable->remove(file, frame->pageNo);
      frame->Clear();
    }
  }
  
}

/**
* Print member variable values. 
*/
void BufMgr::printSelf(void)
{
	BufDesc *tmpbuf;
	int validFrames = 0;

	for (std::uint32_t i = 0; i < numBufs; i++)
	{
		tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

		if (tmpbuf->valid == true)
			validFrames++;
	}

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

} // namespace badgerdb
