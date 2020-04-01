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
#include "bufHashTbl.h"
#include "exceptions/hash_already_present_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/hash_table_exception.h"

namespace badgerdb {

/**
* returns hash value between 0 and HTSIZE-1 computed using file and pageNo
*
* @param file   	File object
* @param pageNo  Page number in the file
* @return  			Hash value.
*/
int BufHashTbl::hash(const File* file, const PageId pageNo)
{
  int tmp, value;
  tmp = (long)file;  // cast of pointer to the file object to an integer
  value = (tmp + pageNo) % HTSIZE;
  return value;
}

BufHashTbl::BufHashTbl(int htSize)
	: HTSIZE(htSize)
{
  // allocate an array of pointers to hashBuckets
  ht = new hashBucket* [htSize];
  for(int i=0; i < HTSIZE; i++)
    ht[i] = NULL;
}

BufHashTbl::~BufHashTbl()
{
  for(int i = 0; i < HTSIZE; i++) {
    hashBucket* tmpBuf = ht[i];
    while (ht[i]) {
      tmpBuf = ht[i];
      ht[i] = ht[i]->next;
      delete tmpBuf;
    }
  }
  delete [] ht;
}

/**
* Insert entry into hash table mapping (file, pageNo) to frameNo.
*
* @param file   	File object
* @param pageNo 	Page number in the file
* @param frameNo Frame number assigned to that page of the file
* @throws  HashAlreadyPresentException	if the corresponding page already exists in the hash table
* @throws  HashTableException (optional) if could not create a new bucket as running of memory
*/
void BufHashTbl::insert(const File* file, const PageId pageNo, const FrameId frameNo)
{
  int index = hash(file, pageNo);

  hashBucket* tmpBuc = ht[index];
  while (tmpBuc) {
    if (tmpBuc->file == file && tmpBuc->pageNo == pageNo)
  		throw HashAlreadyPresentException(tmpBuc->file->filename(), tmpBuc->pageNo, tmpBuc->frameNo);
    tmpBuc = tmpBuc->next;
  }

  tmpBuc = new hashBucket;
  if (!tmpBuc)
  	throw HashTableException();

  tmpBuc->file = (File*) file;
  tmpBuc->pageNo = pageNo;
  tmpBuc->frameNo = frameNo;
  tmpBuc->next = ht[index];
  ht[index] = tmpBuc;
}

/**
* Check if (file, pageNo) is currently in the buffer pool (ie. in
* the hash table).
*
* @param file  	File object
* @param pageNo	Page number in the file
* @param frameNo Frame number reference
* @throws HashNotFoundException if the page entry is not found in the hash table 
*/
void BufHashTbl::lookup(const File* file, const PageId pageNo, FrameId &frameNo) 
{
  int index = hash(file, pageNo);
  hashBucket* tmpBuc = ht[index];
  while (tmpBuc) {
    if (tmpBuc->file == file && tmpBuc->pageNo == pageNo)
    {
      frameNo = tmpBuc->frameNo; // return frameNo by reference
      return;
    }
    tmpBuc = tmpBuc->next;
  }

  throw HashNotFoundException(file->filename(), pageNo);
}

/**
* Delete entry (file,pageNo) from hash table.
*
* @param file   	File object
* @param pageNo  Page number in the file
* @throws HashNotFoundException if the page entry is not found in the hash table 
*/
void BufHashTbl::remove(const File* file, const PageId pageNo) {

  int index = hash(file, pageNo);
  hashBucket* tmpBuc = ht[index];
  hashBucket* prevBuc = NULL;

  while (tmpBuc)
	{
    if (tmpBuc->file == file && tmpBuc->pageNo == pageNo)
		{
      if(prevBuc) 
				prevBuc->next = tmpBuc->next;
      else
				ht[index] = tmpBuc->next;

      delete tmpBuc;
      return;
    }
		else
		{
      prevBuc = tmpBuc;
      tmpBuc = tmpBuc->next;
    }
  }
  throw HashNotFoundException(file->filename(), pageNo);
}

}
