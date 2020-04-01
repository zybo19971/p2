/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	delete [] bufPool;
}

void BufMgr::advanceClock()
{
	//increments the clockhand by 1 and make sure it is between 0 and numBufs -1
	temp = (clockHand +1)
	clockHand = temp % numBufs;//clockHand should not be greater than numBufs-1
}

void BufMgr::allocBuf(FrameId & frame) 
{
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frame_number;
	try{
		hashTable->lookup(file, pageNo, frame_number);
		page = &bufPool[frame_number];
		//Increment pin count and set reference bit to 1
		bufDescTable[frame_number].pinCnt++;
    	bufDescTable[frame_number].refbit = 1;
	}catch(const std::HashNotFoundException& e){
		// lookup trows HashNotFoundException since the page is not found in 
		//in the buffer pool
		allocBuf(frame_number);//allocate a buffer frame
    	Page page_red = file->readPage(pageNo);//read page from disk to mem
    	bufPool[frame_number] = page_red;
    	page = &bufPool[frame_number];
    	hashTable->insert(file, pageNo, frame_number);// insert the page in the hashtable
    	bufDescTable[frame_number].Set(file, pageNo);
	}
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frame_number;
	try{//try to find the page
		hashTable->lookup(file, pageNo, frame_number);
		int pin_count = bufDescTable[frame_number].pinCnt;
		//Throws PAGENOTPINNED if the pin count is already 0
		if (pin_count == 0) throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		//if dirty == true, sets the dirty bit
  		if (dirty == true) bufDescTable[frame_number].dirty = dirty;
		//Decrements the pinCnt of the frame containing (file, PageNo)
		bufDescTable[frame_number].pinCnt-=1;
	}catch(const std::HashNotFoundException& e){
		//Does nothing if page is not found in the Hashtable lookup
	}
}

void BufMgr::flushFile(const File* file) 
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	FrameId frame_number;
	Page p = file->allocatePage();//store the newly allocated page in p
  	allocBuf(frame_number); //obtain a buffer pool frame
  	bufPool[frame_number] = p;
	//returns both the page number of the newly allocated page
  	page = &bufPool[frame_number];
  	pageNo = page->page_number();
  	hashTable->insert(file, pageNo, frame_number);//insert entry in the Hashtable
  	bufDescTable[frame_number].Set(file, pageNo);
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
	FrameId frame_number;
	try{//makes sure that if the page to be deleted is allocated a frame in the buffer pool, that frame
        //is freed and correspondingly entry from hash table is also removed
		hashTable->lookup(file, pageNo, frame_number);
		hashTable->remove(bufDescTable[frameNo].file, bufDescTable[frameNo].pageNo);
    	bufDescTable[frameNo].Clear();	
	}catch(const std::HashNotFoundException& e){	
	}
  	file->deletePage(PageNo); //delete the page from the file
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
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

}
