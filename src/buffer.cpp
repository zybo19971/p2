/**
 * Program Title: Buffer Manager
 * File Purpose:
 * Authors & ID: AKOVI BERLOS MENSAH
 *               Yuanbo Zhang
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

BufMgr::~BufMgr()
{
	delete[] bufPool;
}

/**
 * Advance clock to next frame in the buffer pool
 */
void BufMgr::advanceClock()
{
	//increments the clockhand by 1 and make sure it is between 0 and numBufs -1
	temp = (clockHand + 1);
	clockHand = temp % numBufs; //clockHand should not be greater than numBufs-1
}

/**
* Allocate a free frame.  
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
	    Status s = frameInfo->file->writePage(frameInfo->pageNo, bufPool + frameInfo->frameNo);
	    CHKSTAT(s); // UNIXERR
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
* @param file   	File object
* @param PageNo  Page number in the file to be read
* @param page  	Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.
*/
void BufMgr::readPage(File *file, const PageId pageNo, Page *&page)
{
	FrameId frame_number;
	try
	{
		hashTable->lookup(file, pageNo, frame_number);
		page = &bufPool[frame_number];
		//Increment pin count and set reference bit to 1
		bufDescTable[frame_number].pinCnt++;
		bufDescTable[frame_number].refbit = 1;
	}
	catch (const std::HashNotFoundException &e)
	{
		// lookup trows HashNotFoundException since the page is not found in
		//in the buffer pool
		allocBuf(frame_number);					//allocate a buffer frame
		Page page_red = file->readPage(pageNo); //read page from disk to mem
		bufPool[frame_number] = page_red;
		page = &bufPool[frame_number];
		hashTable->insert(file, pageNo, frame_number); // insert the page in the hashtable
		bufDescTable[frame_number].Set(file, pageNo);
	}
}

/**
* Unpin a page from memory since it is no longer required for it to remain in memory.
*
* @param file   	File object
* @param PageNo  Page number
* @param dirty		True if the page to be unpinned needs to be marked dirty	
* @throws  PageNotPinnedException If the page is not already pinned
*/
void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
{
	FrameId frame_number;
	try
	{ //try to find the page
		hashTable->lookup(file, pageNo, frame_number);
		int pin_count = bufDescTable[frame_number].pinCnt;
		//Throws PAGENOTPINNED if the pin count is already 0
		if (pin_count == 0)
			throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		//if dirty == true, sets the dirty bit
		if (dirty == true)
			bufDescTable[frame_number].dirty = dirty;
		//Decrements the pinCnt of the frame containing (file, PageNo)
		bufDescTable[frame_number].pinCnt -= 1;
	}
	catch (const std::HashNotFoundException &e)
	{
		//Does nothing if page is not found in the Hashtable lookup
	}
}

/**
* Writes out all dirty pages of the file to disk.
* All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
* Otherwise Error returned.
*
* @param file   	File object
* @throws  PagePinnedException If any page of the file is pinned in the buffer pool 
* @throws BadBufferException If any frame allocated to the file is found to be invalid
*/
void BufMgr::flushFile(const File *file)
{
	 // first check if all pages of this file are unpinned
  File* pFile = const_cast<File*>(file);
  std::vector<BufDesc*> frames;
  for(int i = 0; i < numBufs; i++){
    BufDesc* frame = &bufTable[i];
    if(frame->file == pFile){
      frames.push_back(frame);
    }
    if(frame->pinCnt > 0) 
	throw PagePinnedException();
  }
  for(unsigned int i = 0; i < frames.size(); i++){
    BufDesc* pFrame = frames[i];
    if(pFrame->dirty){
      // flush to disk
      Status s = pFile->writePage(pFrame->pageNo, bufPool + pFrame->frameNo);
      CHKSTAT(s);
      pFrame->dirty = false;
    }
    Status s = hashTable->remove(pFile, pFrame->pageNo);
    CHKSTAT(s);
    pFrame->Clear();
  }
  throw BadBufferException();
}

/**
* Allocates a new, empty page in the file and returns the Page object.
* The newly allocated page is also assigned a frame in the buffer pool.
*
* @param file   	File object
* @param PageNo  Page number. The number assigned to the page in the file is returned via this reference.
* @param page  	Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
*/
void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
{
	FrameId frame_number;
	Page p = file->allocatePage(); //store the newly allocated page in p
	allocBuf(frame_number);		   //obtain a buffer pool frame
	bufPool[frame_number] = p;
	//returns both the page number of the newly allocated page
	page = &bufPool[frame_number];
	pageNo = page->page_number();
	hashTable->insert(file, pageNo, frame_number); //insert entry in the Hashtable
	bufDescTable[frame_number].Set(file, pageNo);
}

/**
* Delete page from file and also from buffer pool if present.
* Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
*
* @param file   	File object
* @param PageNo  Page number
*/
void BufMgr::disposePage(File *file, const PageId PageNo)
{
	FrameId frame_number;
	try
	{	//makes sure that if the page to be deleted is allocated a frame in the buffer pool, that frame
		//is freed and correspondingly entry from hash table is also removed
		hashTable->lookup(file, pageNo, frame_number);
		hashTable->remove(bufDescTable[frameNo].file, bufDescTable[frameNo].pageNo);
		bufDescTable[frameNo].Clear();
	}
	catch (const std::HashNotFoundException &e)
	{
	}
	file->deletePage(PageNo); //delete the page from the file
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
