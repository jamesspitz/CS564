/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"

#include "exceptions/tree_empty_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/invalid_record_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb
{
	template<class T>
const int compare(const T a, const T b){
  return a - b;
}

template<> // explicit specialization for T = void
const int compare<char[STRINGSIZE]>( char a[STRINGSIZE], char b[STRINGSIZE]){
  return strncmp(a,b,STRINGSIZE);
}

template <class T>
const void keyCopy( T &a, T &b ){
  a = b;
}

//specialize string
template<>
const void keyCopy<char[STRINGSIZE]>( char (&a)[STRINGSIZE], char (&b)[STRINGSIZE])
{
  strncpy(a,b,STRINGSIZE);
}


/**
 * Return the index of the first key in the given page that is larger than or
 * equal to the param key.
 * 
 * @param thisPage Leaf/NonLeaf node page.
 * @param key      Key to compare
 *
 * @return index of the first key that is not smaller than key
 */
template<class T, class T_NodeType>
const int getIndex( T_NodeType * thisPage, T &key )
{
    int size = thisPage->size;

    // No entries
    if ( size <= 0 ) {
      return -1;
    }

    if ( compare<T>(key, thisPage->keyArray[0]) <= 0 )
      return 0;
    if ( compare<T>(key, thisPage->keyArray[size-1]) > 0 )
      return size;

    // Binary Search
    int left = 1, right = thisPage->size-1;
    T* keyArray = thisPage->keyArray;
    int mid = right - (right - left )/2;
    while ( ! (compare<T>(key, keyArray[mid]) <= 0
               && compare<T>(key, keyArray[mid-1]) > 0 ) ) {
      int cmp = compare<T>(key, keyArray[mid]);
      if ( cmp == 0 ) {
        return mid;
      }  else if ( cmp > 0 ) {
        left = mid + 1;
      } else {
        right = mid - 1; 
      }
      mid = right - (right-left)/2;
    }
    return mid;
}


template<class T_NodeType>
const void BTreeIndex::shiftToNextEntry(T_NodeType *thisPage)
{
	// Next page
    if ( ++nextEntry >= thisPage->size ) {
		// No pages found
      if ( thisPage->rightSibPageNo == 0 ) { 
          nextEntry = -1;
          return;
      }
      PageId nextPageNo = thisPage->rightSibPageNo; 
      bufMgr->unPinPage(file, currentPageNum, false);
      currentPageNum = nextPageNo;
      bufMgr->readPage(file, currentPageNum, currentPageData);
      nextEntry = 0;
    }

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
		 : bufMgr(bufMgrIn),               // initialized data field
        attributeType(attrType),
        attrByteOffset(attrByteOffset){
			{
			std::ostringstream idxStr;
			idxStr << relationName << '.' << attrByteOffset;
			outIndexName = idxStr.str(); 
			}
	// BTreeIndex data field setup
	 switch  (attributeType ) {
    case INTEGER:
      leafOccupancy = INTARRAYLEAFSIZE;
      nodeOccupancy = INTARRAYNONLEAFSIZE;
      break;
    case DOUBLE:
      leafOccupancy = DOUBLEARRAYLEAFSIZE;
      nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
      break;
    case STRING:
      leafOccupancy = STRINGARRAYLEAFSIZE;
      nodeOccupancy = STRINGARRAYNONLEAFSIZE;
      break;
    default:
      std::cout<<"Unsupported data type\n";
  }

  scanExecuting = false;
    nextEntry = -1;
    currentPageNum = 0;

// Check for the existence of the Index
	// Old file attempt
	try {
    file = new BlobFile(outIndexName,false);
    Page *tempPage;
    headerPageNum = file->getFirstPageNo();
    bufMgr->readPage(file, headerPageNum, tempPage);
    IndexMetaInfo *metaInfo = reinterpret_cast<IndexMetaInfo *>(tempPage);
    rootPageNum = metaInfo->rootPageNo;

    bufMgr->unPinPage(file, headerPageNum, false);
    if ( relationName.compare(metaInfo->relationName) != 0
        || metaInfo->attrByteOffset != attrByteOffset
        || metaInfo->attrType != attrType
       ) 
	   {
		   std::cout<<"Meta info does not match the index!\n";
      delete file;
      file = NULL;
      return;
    	}
		 } catch (FileNotFoundException e ) {
	// Deletes file. Creates and contructs the new index file
    Page *tempPage;
    file = new BlobFile(outIndexName, true);

    bufMgr->allocPage(file, headerPageNum, tempPage);
    IndexMetaInfo *metaInfo = reinterpret_cast<IndexMetaInfo *>(tempPage);
    bufMgr->allocPage(file, rootPageNum, tempPage);

    std::copy(relationName.begin(), relationName.end(), metaInfo->relationName);
    metaInfo->attrByteOffset = attrByteOffset;
    metaInfo->attrType = attrType;
    metaInfo->rootPageNo = rootPageNum;

    // Root page construction
    if ( attributeType == INTEGER ) {
      LeafNodeInt* intRootPage = reinterpret_cast<LeafNodeInt*>(tempPage);
      intRootPage->size = 0;
      intRootPage->rightSibPageNo = 0;
    } else if ( attributeType == DOUBLE ) {
      LeafNodeDouble* doubleRootPage = reinterpret_cast<LeafNodeDouble*>(tempPage);
      doubleRootPage->size = 0;
      doubleRootPage->rightSibPageNo = 0;
    } else if ( attributeType == STRING ) {
      LeafNodeString* stringRootPage = reinterpret_cast<LeafNodeString*>(tempPage);
      stringRootPage->size = 0;
      stringRootPage->rightSibPageNo = 0;
    } else {
      std::cout<<"Data type unsupported\n";
    }
    bufMgr->unPinPage(file, rootPageNum, true);
    bufMgr->unPinPage(file, headerPageNum, true);

    // Build the B-Tree
    buildBTree(relationName);
  }
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    if ( currentPageNum ) {
      try {
        bufMgr->unPinPage(file, currentPageNum, false);
      } catch ( PageNotPinnedException e) {
      	}
    }

    scanExecuting = false;
    try {
      if ( file ) {
        bufMgr->flushFile(file);
        delete file;
        file = NULL;
      }
    } catch( PageNotPinnedException e) {
      std::cout<<"Flushing file"<<std::endl;
    } catch ( PagePinnedException e ) {
      std::cout<<"PagePinnedException"<<std::endl;
    } 
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    // Finds data type and then the leaf
    if ( attributeType == INTEGER ) {
        RIDKeyPair<int> rkpair;
        int intKey = *(int*)key;
        rkpair.set(rid, intKey);
        PageId leafToInsert = 
          findLeafNode<int, struct NonLeafNodeInt>(rootPageNum, intKey);
        insertLeafNode<int, struct NonLeafNodeInt, struct LeafNodeInt>(leafToInsert, rkpair);
    } else if ( attributeType == DOUBLE ) {
        RIDKeyPair<double> rkpair;
        double doubleKey = *(double*)key;
        rkpair.set(rid, doubleKey);
        PageId leafToInsert =
          findLeafNode<double, struct NonLeafNodeDouble>(rootPageNum,doubleKey);
        insertLeafNode<double, struct NonLeafNodeDouble, struct LeafNodeDouble>(leafToInsert, rkpair);
    } else if ( attributeType == STRING ) {
        RIDKeyPair<char[STRINGSIZE]> rkpair;
        strncpy(rkpair.key, (char*)key, STRINGSIZE);
        rkpair.rid = rid;
        PageId leafToInsert = 
          findLeafNode<char[STRINGSIZE], struct NonLeafNodeString>(rootPageNum,rkpair.key);
        insertLeafNode<char[STRINGSIZE], struct NonLeafNodeString, struct LeafNodeString>(leafToInsert, rkpair);
    } else {
        std::cout<<"Data type unsupported\n";
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::findLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode>
const PageId BTreeIndex::findLeafNode(PageId pageNo, T &key)
{
  if ( rootPageNum == 2 ) {
    return rootPageNum; 
  }
  Page *tempPage;
  bufMgr->readPage(file, pageNo, tempPage);
  T_NonLeafNode* thisPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
  
  int index = getIndex<T, T_NonLeafNode>(thisPage, key);

  PageId leafNodeNo = thisPage->pageNoArray[index];
  int thisPageLevel  = thisPage->level;
  bufMgr->unPinPage(file, pageNo, false);
  if ( thisPageLevel == 0 ) { 
    leafNodeNo = findLeafNode<T, T_NonLeafNode>(leafNodeNo, key);
  }

  return leafNodeNo;
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode> 
const void BTreeIndex::insertLeafNode(PageId pageNo, RIDKeyPair<T> rkpair)
{
  Page *tempPage;
  bufMgr->readPage(file, pageNo, tempPage);
  T_LeafNode * thisPage = reinterpret_cast<T_LeafNode*>(tempPage);
  T thisKey;
  copyKey((thisKey), ((rkpair.key)));

  int size = thisPage->size;
  if ( size < leafOccupancy ) {
    int index = getIndex<T, T_LeafNode>(thisPage, thisKey);
    if ( index == -1 ) index = 0;

    // use memmove
    memmove((void*)(&(thisPage->keyArray[index+1])),
            (void*)(&(thisPage->keyArray[index])), sizeof(T)*(size-index));
    memmove((void*)(&(thisPage->ridArray[index+1])),
            (void*)(&(thisPage->ridArray[index])), sizeof(RecordId)*(size-index));

    copyKey(((thisPage->keyArray[index])), (thisKey));
    thisPage->ridArray[index] = rkpair.rid;

    (thisPage->size)++;
    bufMgr->unPinPage(file, pageNo, true);
  } else {
    int midIndex = leafOccupancy/2 + leafOccupancy%2;
    bool insertLeftNode = compare<T>(thisKey, thisPage->keyArray[midIndex])<0;

    bufMgr->unPinPage(file, pageNo, false);

    PageId rightPageNo = splitLeafNode<T, T_NonLeafNode, T_LeafNode>(pageNo);

    PageId pageNoToInsert = pageNo;
    if ( !insertLeftNode )
      pageNoToInsert = rightPageNo;
    insertLeafNode<T, T_NonLeafNode, T_LeafNode>(pageNoToInsert, rkpair);

  }
}


// -----------------------------------------------------------------------------
// BTreeIndex::splitLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const PageId BTreeIndex::splitLeafNode(PageId pageNo)
{
    Page *tempPage;
    PageId firstPageNo = pageNo;
    bufMgr->readPage(file, firstPageNo, tempPage);
    T_LeafNode* firstPage = reinterpret_cast<T_LeafNode*>(tempPage);
    PageId secondPageNo;
    bufMgr->allocPage(file, secondPageNo, tempPage);
    T_LeafNode* secondPage = reinterpret_cast<T_LeafNode*>(tempPage);

    secondPage->rightSibPageNo = firstPage->rightSibPageNo;
    firstPage->rightSibPageNo = secondPageNo;

    int midIndex = leafOccupancy/2 + leafOccupancy%2;
    memmove((void*)(&(secondPage->keyArray[0])),
            (void*)(&( firstPage->keyArray[midIndex])),
            sizeof(T)*(leafOccupancy-midIndex));
    memmove((void*)(&(secondPage->ridArray[0])),
            (void*)(&( firstPage->ridArray[midIndex])),
            sizeof(RecordId)*(leafOccupancy-midIndex));

    firstPage->size = midIndex;
    secondPage->size = leafOccupancy - midIndex;

    T copyUpKey;
    copyKey((copyUpKey), ((firstPage->keyArray[midIndex])));

    bool rootIsLeaf = rootPageNum == 2;

    if ( !rootIsLeaf ) {
      PageId firstPageParentNo = findParentOf<T, T_NonLeafNode, T_LeafNode>
        (firstPageNo, firstPage->keyArray[firstPage->size-1]);
      
      bufMgr->unPinPage(file, firstPageNo, true);
      bufMgr->unPinPage(file, secondPageNo, true);
      insertNonLeafNode<T, T_NonLeafNode, T_LeafNode>
        (firstPageParentNo, copyUpKey, secondPageNo);
    } else { 
      PageId parentPageNo;
      bufMgr->allocPage(file, parentPageNo, tempPage);
      T_NonLeafNode* parentPage = reinterpret_cast<T_NonLeafNode*>(tempPage);

      rootPageNum = parentPageNo;
      bufMgr->readPage(file, headerPageNum, tempPage);
      IndexMetaInfo* metaPage = reinterpret_cast<IndexMetaInfo*>(tempPage);
      metaPage->rootPageNo = parentPageNo;
      bufMgr->unPinPage(file, headerPageNum, true);

      bufMgr->unPinPage(file, firstPageNo, true);
      bufMgr->unPinPage(file, secondPageNo, true);

      parentPage->level = 1; 
      parentPage->size = 1;  
      copyKey(((parentPage->keyArray[0])), ((copyUpKey)));
      parentPage->pageNoArray[0] = firstPageNo;
      parentPage->pageNoArray[1] = secondPageNo;

      bufMgr->unPinPage(file, parentPageNo, true);
    }
    return secondPageNo;
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertNonLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::insertNonLeafNode(PageId pageNo, T &key, PageId childPageNo)
{
    Page *tempPage;
    bufMgr->readPage(file, pageNo, tempPage);
    T_NonLeafNode* thisPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
    T thisKey;
    copyKey(thisKey, key);
    int size = thisPage->size;
    if ( size < nodeOccupancy ) {
    int index = getIndex<T, T_NonLeafNode>(thisPage, thisKey);

      memmove((void*)(&(thisPage->keyArray[index+1])),
              (void*)(&(thisPage->keyArray[index])), sizeof(T)*(size-index));
      memmove((void*)(&(thisPage->pageNoArray[index+2])),
              (void*)(&(thisPage->pageNoArray[index+1])), sizeof(PageId)*(size-index));

      // inserts current key
      copyKey(thisPage->keyArray[index], thisKey);
      thisPage->pageNoArray[index+1] = childPageNo;
      (thisPage->size)++;
      bufMgr->unPinPage(file, pageNo, true);
    } else {

      int midIndex = nodeOccupancy/2;

      bool insertLeftNode = compare<T>(thisKey, thisPage->keyArray[midIndex])<0;

      if ( nodeOccupancy%2 == 0 ) {
        midIndex--;
        insertLeftNode = compare<T>(thisKey, thisPage->keyArray[midIndex])<0;
      }

      bufMgr->unPinPage(file, pageNo, false);

      PageId rightPageNo = splitNonLeafNode<T, T_NonLeafNode, T_LeafNode>
        (pageNo, midIndex);

      PageId pageNoToInsert = pageNo;
      if ( !insertLeftNode ) 
        pageNoToInsert = rightPageNo;
      insertNonLeafNode<T, T_NonLeafNode, T_LeafNode>
        (pageNoToInsert, key, childPageNo);
    }
}


// -----------------------------------------------------------------------------
// BTreeIndex::splitNonLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const PageId BTreeIndex::splitNonLeafNode(PageId pageNo, int midIndex)
{
    Page *tempPage;
    PageId firstPageNo = pageNo;
    bufMgr->readPage(file,  firstPageNo, tempPage);
    T_NonLeafNode * firstPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
    PageId secondPageNo;
    bufMgr->allocPage(file, secondPageNo, tempPage);
    T_NonLeafNode* secondPage = reinterpret_cast<T_NonLeafNode*>(tempPage);

    secondPage->level = firstPage->level;

    memmove((void*)(&(secondPage->keyArray[0])),
            (void*)(&( firstPage->keyArray[midIndex+1])),
            sizeof(T)*(nodeOccupancy-midIndex-1));
    memmove((void*)(&(secondPage->pageNoArray[0])),
            (void*)(&( firstPage->pageNoArray[midIndex+1])),
            sizeof(PageId)*(nodeOccupancy-midIndex));

    firstPage->size = midIndex;
    secondPage->size = nodeOccupancy - midIndex -1; 

    T pushUpKey;
    copyKey(pushUpKey, firstPage->keyArray[midIndex]);

      bufMgr->unPinPage(file, firstPageNo, true);
      bufMgr->unPinPage(file, secondPageNo, true);

    bool currentNodeIsRoot = rootPageNum == firstPageNo;

    if ( ! currentNodeIsRoot ) {
      PageId parentPageNo = findParentOf<T, T_NonLeafNode, T_LeafNode>
        (firstPageNo, firstPage->keyArray[firstPage->size-1]);

      insertNonLeafNode<T, T_NonLeafNode, T_LeafNode>
        (parentPageNo, pushUpKey, secondPageNo);
    } else { 
      PageId parentPageNo;
      bufMgr->allocPage(file, parentPageNo, tempPage);
      T_NonLeafNode* parentPage = reinterpret_cast<T_NonLeafNode*>(tempPage);

      rootPageNum = parentPageNo;
      bufMgr->readPage(file, headerPageNum, tempPage);
      IndexMetaInfo* metaPage = reinterpret_cast<IndexMetaInfo*>(tempPage);
      metaPage->rootPageNo = parentPageNo;
      bufMgr->unPinPage(file, headerPageNum, true);

      parentPage->level = 0;
      parentPage->size = 1;
      copyKey(((parentPage->keyArray[0])),((pushUpKey)));
      parentPage->pageNoArray[0] = firstPageNo;
      parentPage->pageNoArray[1] = secondPageNo;
      
      bufMgr->unPinPage(file, parentPageNo, true);
    }
    return secondPageNo;
}


template< class T, class T_NonLeafNode, class T_LeafNode>
const PageId BTreeIndex::findParentOf(PageId childPageNo, T &key)
{
  Page *tempPage;
  PageId nextPageNo = rootPageNum;
  PageId parentNodeNo = 0;
  while ( 1 ) {
    bufMgr->readPage(file, nextPageNo, tempPage);
    T_NonLeafNode* thisPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
    
    int index = getIndex<T, T_NonLeafNode>(thisPage, key);

    parentNodeNo = nextPageNo;
    nextPageNo = thisPage-> pageNoArray[index];
    bufMgr->unPinPage(file, parentNodeNo, false);

    if ( nextPageNo == childPageNo )  {
      break;
    }
  }

  return parentNodeNo;
}


const void BTreeIndex::printTree()
{
  switch  (attributeType ) {
    case INTEGER:
      printTree<int, struct NonLeafNodeInt, struct LeafNodeInt>();
      break;
    case DOUBLE:
      printTree<double, struct NonLeafNodeDouble, struct LeafNodeDouble>();
      break;
    case STRING:
      printTree<char[STRINGSIZE], struct NonLeafNodeString, struct LeafNodeString>();
      break;
    default:
      std::cout<<"Datatype not supported\n";
  }
}


template <class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::printTree() 
{
  std::cout<<"Printing Tree" << std::endl;
  Page *tempPage;
  PageId currNo = rootPageNum;

  bool rootIsLeaf = rootPageNum == 2;
  bufMgr->readPage(file, currNo, tempPage);

  int lineSize = 20;

  try {
    if ( rootIsLeaf ) {
      // print the root
      T_LeafNode *currPage = reinterpret_cast<T_LeafNode*>(tempPage);
      int size = currPage->size;
  std::cout<<" Root is leaf with size "<<size<<std::endl;
  std::cout<<std::endl<<" PageId: "<<currNo<<std::endl;
      for ( int i = 0 ; i < size ; ++i) {
        if ( i%lineSize == 0 ) std::cout<<std::endl<<i<<": ";
        std::cout<<currPage->keyArray[i]<<" ";
      }
      std::cout<<std::endl<<"Root Leaf B-Tree printed"<<std::endl;
      bufMgr->unPinPage(file, currNo, false);
    } else {
      T_NonLeafNode *currPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
      while ( currPage->level != 1 ) {
        PageId nextPageNo = currPage->pageNoArray[0];
        bufMgr->unPinPage(file, currNo, false);
        currNo = nextPageNo;
        bufMgr->readPage(file, currNo, tempPage);
        currPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
      }

      PageId leafNo = currPage->pageNoArray[0];
      bufMgr->unPinPage(file, currNo, false);

      currNo = leafNo;
      bufMgr->readPage(file, currNo, tempPage);
      T_LeafNode *currLeafPage = reinterpret_cast<T_LeafNode*>(tempPage);
      while ( 1 ) {
        int size = currLeafPage->size;
        std::cout<<std::endl<<" PageId: "<<currNo<<std::endl;
        for ( int i = 0 ; i < size ; ++i) {
          if ( i%lineSize == 0 ) std::cout<<std::endl<<i<<": ";
          std::cout<<currLeafPage->keyArray[i]<<" ";
        }
        std::cout<<std::endl;
        bufMgr->unPinPage(file, currNo, false);
        if ( currLeafPage->rightSibPageNo == 0 ) break;
        currNo = currLeafPage->rightSibPageNo;
        bufMgr->readPage(file, currNo, tempPage);
        currLeafPage = reinterpret_cast<T_LeafNode*>(tempPage);
      }
      std::cout<<std::endl<<"B-Tree printed"<<std::endl;
      bufMgr->unPinPage(file, currNo, false);
    }
  } catch ( PageNotPinnedException e ) {
  }
  
}

// -----------------------------------------------------------------------------
// BTreeIndex::deleteEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::deleteEntry(const void* key)
{
    if ( attributeType == INTEGER ) {
        int intKey = *(int*)key;
        PageId leafToDelete = findLeafNode<int, struct NonLeafNodeInt>
          (rootPageNum, intKey);
        deleteLeafNode<int, struct NonLeafNodeInt, struct LeafNodeInt>
          (leafToDelete, intKey);
    } else if ( attributeType == DOUBLE ) {
        double doubleKey = *(double*)key;
        PageId leafToDelete = findLeafNode<double, struct NonLeafNodeDouble>
          (rootPageNum, doubleKey);
        deleteLeafNode<double, struct NonLeafNodeDouble, struct LeafNodeDouble>
          (leafToDelete, doubleKey);
    } else if ( attributeType == STRING ) {
        char stringKey[STRINGSIZE];
        strncpy(stringKey, (char*)key, STRINGSIZE);
        PageId leafToDelete = findLeafNode<char[STRINGSIZE], struct NonLeafNodeString>
          (rootPageNum, stringKey);
        deleteLeafNode<char[STRINGSIZE], struct NonLeafNodeString, struct LeafNodeString>
          (leafToDelete, stringKey);
    }
}


// -----------------------------------------------------------------------------
// BTreeIndex::deleteLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::deleteLeafNode(PageId pageNo, T& key)
{
  Page * tempPage;
  bufMgr->readPage(file, pageNo, tempPage);
  T_LeafNode * thisPage = reinterpret_cast<T_LeafNode*>(tempPage);

  int index = getIndex<T, T_LeafNode>(thisPage, key);
  if ( index == -1 ) {
    bufMgr->unPinPage(file, pageNo, false);
    throw EmptyBTreeException();
  }

  if ( compare(thisPage->keyArray[index], key) != 0 ) {
    std::cout<<"Key does not exist\n";
    bufMgr->unPinPage(file, pageNo, false);
    return;
  }

  (thisPage->size)--;
  int thisSize = thisPage->size;
  memmove((void*)(&(thisPage->keyArray[index])),
          (void*)(&(thisPage->keyArray[index+1])), sizeof(T)*(thisSize-index));
  memmove((void*)(&(thisPage->ridArray[index])),
          (void*)(&(thisPage->ridArray[index+1])), sizeof(RecordId)*(thisSize-index));

  int leafHalfFillNo = leafOccupancy/2;

  if ( pageNo == rootPageNum || thisSize >= leafHalfFillNo ) {
    bufMgr->unPinPage(file, pageNo, true);
    return;
  }

  { 
    bool needToMerge = false; 
    PageId firstPageNo = 0, secondPageNo = 0;
    PageId parentPageNo = findParentOf<T, T_NonLeafNode, T_LeafNode>
      (pageNo, thisPage->keyArray[thisSize-1]);

    bufMgr->readPage(file, parentPageNo, tempPage);
    T_NonLeafNode* parentPage = reinterpret_cast<T_NonLeafNode*>(tempPage);

    PageId rightPageNo = thisPage->rightSibPageNo;

    if ( rightPageNo != 0 ) {
      bufMgr->readPage(file, rightPageNo, tempPage);
      T_LeafNode* rightPage = reinterpret_cast<T_LeafNode*>(tempPage);
      int rightPageSize = rightPage->size;
      if ( rightPageSize > leafHalfFillNo ) { 
        int pindex = getIndex<T, T_NonLeafNode>(parentPage, rightPage->keyArray[0]);
        copyKey(parentPage->keyArray[pindex], rightPage->keyArray[1]);
        copyKey(thisPage->keyArray[thisSize], rightPage->keyArray[0]);
        thisPage->ridArray[thisSize] = rightPage->ridArray[0];
        memmove((void*)(&(rightPage->keyArray[0])),
                (void*)(&(rightPage->keyArray[1])), sizeof(T)*(rightPageSize-1));
        memmove((void*)(&(rightPage->ridArray[0])),
                (void*)(&(rightPage->ridArray[1])), sizeof(RecordId)*(rightPageSize-1));
        (rightPage->size)--;
        (thisPage->size)++;
        bufMgr->unPinPage(file, pageNo, true);
        bufMgr->unPinPage(file, rightPageNo, true);
        bufMgr->unPinPage(file, parentPageNo, true);
        return;
      } else {
        needToMerge = true;
        firstPageNo = pageNo;
        secondPageNo = rightPageNo;
        bufMgr->unPinPage(file, rightPageNo, false);
      }
    }

    int pindex = getIndex<T, T_NonLeafNode>(parentPage, thisPage->keyArray[thisSize-1]);
    if ( pindex > 0 ) {
      PageId leftPageNo = parentPage->pageNoArray[pindex-1];
      bufMgr->readPage(file, leftPageNo, tempPage);
      T_LeafNode* leftPage = reinterpret_cast<T_LeafNode*>(tempPage);
      int leftPageSize = leftPage->size;

      if ( leftPageSize > leafHalfFillNo ) { 
        copyKey(parentPage->keyArray[pindex], leftPage->keyArray[leftPageSize-1]);
        memmove((void*)(&(thisPage->keyArray[1])),
                (void*)(&(thisPage->keyArray[0])), sizeof(T)*(thisSize));
        memmove((void*)(&(thisPage->ridArray[1])),
                (void*)(&(thisPage->ridArray[0])), sizeof(RecordId)*(thisSize));
        copyKey(thisPage->keyArray[0], leftPage->keyArray[leftPageSize-1]);
        thisPage->ridArray[0] = leftPage->ridArray[leftPageSize-1];
        (leftPage->size)--;
        (thisPage->size)++;
        bufMgr->unPinPage(file, pageNo, true);
        bufMgr->unPinPage(file, leftPageNo, true);
        bufMgr->unPinPage(file, parentPageNo, true);
        return;
      } else {
        bufMgr->unPinPage(file, leftPageNo, false);
      }
    }
    bufMgr->unPinPage(file, pageNo, true);

    if ( needToMerge ) {
      bufMgr->readPage(file, firstPageNo, tempPage);
      T_LeafNode* firstPage = reinterpret_cast<T_LeafNode*>(tempPage);
      bufMgr->readPage(file, secondPageNo, tempPage);
      T_LeafNode* secondPage = reinterpret_cast<T_LeafNode*>(tempPage);

      int size1 = firstPage->size, size2 = secondPage->size;
      if ( size1+size2 > leafOccupancy) {
        std::cout<<"Size larger than occupancy\n";
        return;
      }

      memmove((void*)(&( firstPage->keyArray[size1])),
              (void*)(&(secondPage->keyArray[0])), sizeof(T)*(size2));
      memmove((void*)(&( firstPage->ridArray[size1])),
              (void*)(&(secondPage->ridArray[0])), sizeof(RecordId)*(size2));
      firstPage->size = size1+size2;

      firstPage->rightSibPageNo = secondPage->rightSibPageNo;
      bufMgr->unPinPage(file, secondPageNo, false);

      T key;
      copyKey(key, firstPage->keyArray[0]);
      bufMgr->unPinPage(file, firstPageNo, true);
      bufMgr->unPinPage(file, parentPageNo, true);
      deleteNonLeafNode<T, T_NonLeafNode, T_LeafNode>(parentPageNo, key);
    } else {
      bufMgr->unPinPage(file, parentPageNo, true);
    }
  }
}


// -----------------------------------------------------------------------------
// BTreeIndex::mergeLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::mergeLeafNode(PageId firstPageNo, PageId secondPageNo)
{
  Page* tempPage;
  bufMgr->readPage(file, firstPageNo, tempPage);
  T_LeafNode* firstPage = reinterpret_cast<T_LeafNode*>(tempPage);
  bufMgr->readPage(file, secondPageNo, tempPage);
  T_LeafNode* secondPage = reinterpret_cast<T_LeafNode*>(tempPage);

  int size1 = firstPage->size, size2 = secondPage->size;

  if ( size1+size2 > leafOccupancy) {
    std::cout<<" final size larger than leafOccupancy after merging!\n";
    return;
  }

  memmove((void*)(&( firstPage->keyArray[size1])),
          (void*)(&(secondPage->keyArray[0])), sizeof(T)*(size2));
  memmove((void*)(&( firstPage->ridArray[size1])),
          (void*)(&(secondPage->ridArray[0])), sizeof(RecordId)*(size2));
  firstPage->size = size1+size2;

  // deletes second page
  firstPage->rightSibPageNo = secondPage->rightSibPageNo;
  bufMgr->unPinPage(file, secondPageNo, false);

  PageId parentPageNo = findParentOf<T, T_NonLeafNode, T_LeafNode>
    (firstPageNo, firstPage->keyArray[size1-1]);
  T key;
  copyKey(key, firstPage->keyArray[0]);
  bufMgr->unPinPage(file, firstPageNo, true);
  deleteNonLeafNode<T, T_NonLeafNode, T_LeafNode>(parentPageNo, key);
}



// -----------------------------------------------------------------------------
// BTreeIndex::deleteNonLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::deleteNonLeafNode(PageId pageNo, T& key)
{
  Page* tempPage;
  bufMgr->readPage(file, pageNo, tempPage);
  T_NonLeafNode* thisPage = reinterpret_cast<T_NonLeafNode*>(tempPage);

  int index = getIndex<T, T_NonLeafNode>(thisPage, key);
  (thisPage->size)--;
  int thisSize = thisPage->size;
  if ( index == thisSize ) { 
  } else {
	      	memmove((void*)(&(thisPage->keyArray[index])),
            (void*)(&(thisPage->keyArray[index+1])), sizeof(T)*(thisSize-index));
    		memmove((void*)(&(thisPage->pageNoArray[index+1])),
            (void*)(&(thisPage->pageNoArray[index+2])), sizeof(PageId)*(thisSize-index));
  }

  if ( pageNo == rootPageNum && thisSize == 0 ) {
    rootPageNum = thisPage->pageNoArray[0];
    bufMgr->readPage(file, headerPageNum, tempPage);
    IndexMetaInfo* metaPage = reinterpret_cast<IndexMetaInfo*>(tempPage);
    metaPage->rootPageNo = thisPage->pageNoArray[0];
    bufMgr->unPinPage(file, headerPageNum, true);
    
    // deletes curr page
    bufMgr->unPinPage(file, pageNo, false);
    return;
  }

  int nodeHalfFillNo = nodeOccupancy/2;
  if ( pageNo == rootPageNum || thisSize >= nodeHalfFillNo ) {
    bufMgr->unPinPage(file, pageNo, true);
    return;
  }
  {
    bool needToMerge = false;
    PageId firstPageNo = 0, secondPageNo = 0;

    PageId parentPageNo = findParentOf<T, T_NonLeafNode, T_LeafNode>
      (pageNo, thisPage->keyArray[thisSize-1]);
    bufMgr->readPage(file, parentPageNo, tempPage);
    T_NonLeafNode* parentPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
    int pindex = getIndex<T, T_NonLeafNode>(parentPage, thisPage->keyArray[thisSize-1]);

    if ( pindex < parentPage->size ) { // try right page 
      PageId rightPageNo = parentPage->pageNoArray[pindex+1];
      bufMgr->readPage(file, rightPageNo, tempPage);
      T_NonLeafNode* rightPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
      int rightPageSize = rightPage->size;

      if ( rightPageSize > nodeHalfFillNo ) { // just borrow one
        copyKey(thisPage->keyArray[thisSize], parentPage->keyArray[pindex]);
        thisPage->pageNoArray[thisSize+1] = rightPage->pageNoArray[0];
        copyKey(parentPage->keyArray[pindex], rightPage->keyArray[0]);
        memmove((void*)(&(rightPage->keyArray[0])),
                (void*)(&(rightPage->keyArray[1])), sizeof(T)*(rightPageSize-1));
        memmove((void*)(&(rightPage->pageNoArray[0])),
                (void*)(&(rightPage->pageNoArray[1])), sizeof(PageId)*(rightPageSize));

        (thisPage->size)++;
        (rightPage->size)--;
        bufMgr->unPinPage(file, pageNo, true);
        bufMgr->unPinPage(file, parentPageNo, true);
        bufMgr->unPinPage(file, rightPageNo, true);
        return;
      } else { 
        needToMerge = true;
        firstPageNo = pageNo;
        secondPageNo = rightPageNo;
        bufMgr->unPinPage(file, rightPageNo, false);
      }
    }

    { 
      PageId leftPageNo = parentPage->pageNoArray[pindex-1];
      bufMgr->readPage(file, leftPageNo, tempPage);
      T_NonLeafNode* leftPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
      int leftPageSize = leftPage->size;

      if ( leftPageSize > nodeHalfFillNo ) { 
        // space allocation
        memmove((void*)(&(thisPage->keyArray[1])),
                (void*)(&(thisPage->keyArray[0])), sizeof(T)*(thisSize));
        memmove((void*)(&(thisPage->pageNoArray[1])),
                (void*)(&(thisPage->pageNoArray[0])), sizeof(PageId)*(thisSize+1));
        // key to page
        copyKey(thisPage->keyArray[0], parentPage->keyArray[pindex]);
        thisPage->pageNoArray[0] = leftPage->pageNoArray[leftPageSize];
        // key to parent
        copyKey(parentPage->keyArray[pindex], leftPage->keyArray[leftPageSize-1]);
        (thisPage->size)++;
        (leftPage->size)--;
        bufMgr->unPinPage(file, pageNo, true);
        bufMgr->unPinPage(file, parentPageNo, true);
        bufMgr->unPinPage(file, leftPageNo, true);
        return;
      } else { 
        bufMgr->unPinPage(file, leftPageNo, false);
      }
    }
    bufMgr->unPinPage(file, pageNo, true);
    bufMgr->unPinPage(file, parentPageNo, true);
    if (needToMerge) 
      mergeNonLeafNode<T, T_NonLeafNode, T_LeafNode>(firstPageNo, secondPageNo);
  }
}

// -----------------------------------------------------------------------------
// BTreeIndex::mergeNonLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::mergeNonLeafNode(PageId firstPageNo, PageId secondPageNo)
{
  Page* tempPage;
  bufMgr->readPage(file, firstPageNo, tempPage);
  T_NonLeafNode* firstPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
  bufMgr->readPage(file, secondPageNo, tempPage);
  T_NonLeafNode* secondPage = reinterpret_cast<T_NonLeafNode*>(tempPage);

  int size1 = firstPage->size, size2 = secondPage->size;
  if ( size1+size2 > nodeOccupancy) {
    std::cout<<"Size larger occupancy\n";
    return;
  }


  PageId parentPageNo = findParentOf<T, T_NonLeafNode, T_LeafNode>
    (firstPageNo, firstPage->keyArray[size1-1]);
  bufMgr->readPage(file, parentPageNo, tempPage);
  T_NonLeafNode* parentPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
  int pindex = getIndex<T, T_NonLeafNode>(parentPage, firstPage->keyArray[size1-1]);

  // Entry combination
  copyKey(firstPage->keyArray[size1], parentPage->keyArray[pindex]);
  memmove((void*)(&( firstPage->keyArray[size1+1])),
          (void*)(&(secondPage->keyArray[0])), sizeof(T)*(size2));
  memmove((void*)(&( firstPage->pageNoArray[size1+1])),
          (void*)(&(secondPage->pageNoArray[0])), sizeof(RecordId)*(size2+1));
  firstPage->size = size1+size2+1; 
  bufMgr->unPinPage(file, secondPageNo, false);
  T key;
  copyKey(key, firstPage->keyArray[0]);
  bufMgr->unPinPage(file, firstPageNo, true);
  deleteNonLeafNode<T, T_NonLeafNode, T_LeafNode>(parentPageNo, key);
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
	if ( (lowOpParm  != GT && lowOpParm  != GTE) 
      || (highOpParm != LT && highOpParm != LTE) ) {
      throw BadOpcodesException();
    }
    if ( scanExecuting == true ) {
      std::cout<< "Scan in progress\n";
    } else {
      scanExecuting = true;
    }

    lowOp = lowOpParm;
    highOp = highOpParm;

    // Data type handling
    if ( attributeType == INTEGER ) {
      lowValInt = *((int*)lowValParm);
      highValInt = *((int*)highValParm);
      startScanHelper<int, struct NonLeafNodeInt, struct LeafNodeInt>
          (lowValInt, highValInt);
    } else if ( attributeType == DOUBLE ) {
      lowValDouble = *((double*)lowValParm);
      highValDouble = *((double*)highValParm);
      startScanHelper<double, struct NonLeafNodeDouble, struct LeafNodeDouble>
          (lowValDouble, highValDouble);
    } else if ( attributeType == STRING ) {
      strncpy(lowStringKey, (char*)lowValParm, STRINGSIZE);
      strncpy(highStringKey, (char*)highValParm, STRINGSIZE);
      startScanHelper<char[STRINGSIZE], struct NonLeafNodeString, struct LeafNodeString>
          (lowStringKey, highStringKey);
    } else {
       std::cout<<"Unsupported data type\n";
       exit(1); 
    }

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScanHelper
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::startScanHelper(T &lowVal, T &highVal)
{
      if ( lowVal > highVal ) {
        scanExecuting = false;
        throw BadScanrangeException();
      }
      currentPageNum = findLeafNode<T, T_NonLeafNode>(rootPageNum, lowVal);

      bufMgr->readPage(file, currentPageNum, currentPageData);
      T_LeafNode* thisPage;
      thisPage = reinterpret_cast<T_LeafNode*>(currentPageData);
      
      int size = thisPage->size;
      if ( compare<T>(lowVal, thisPage->keyArray[size-1]) > 0 ) {
        nextEntry = size-1;
        shiftToNextEntry<T_LeafNode>(thisPage);
        thisPage = reinterpret_cast<T_LeafNode*>(currentPageData);
      } else {
        nextEntry = getIndex<T, T_LeafNode>(thisPage, lowVal);
      }

      if ( lowOp==GT && compare<T>(lowVal, thisPage->keyArray[nextEntry])==0) {
        shiftToNextEntry<T_LeafNode>(thisPage);
      }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
	
    if ( scanExecuting == false) {
      std::cout<<"No scan started\n";
      throw ScanNotInitializedException();
    }

    if ( attributeType == INTEGER ) {
      scanNextHelper<int, struct LeafNodeInt>(outRid, lowValInt, highValInt);
    } else if ( attributeType == DOUBLE ) {
      scanNextHelper<double, struct LeafNodeDouble>(outRid, lowValDouble, highValDouble);
    } else if ( attributeType == STRING ) {
      scanNextHelper<char[STRINGSIZE], struct LeafNodeString>(outRid, lowStringKey, highStringKey);
    } else {
       std::cout<<"Unsupported data type\n";
       exit(1); 
    }

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNextHelper
// -----------------------------------------------------------------------------

template <class T, class T_LeafNode >
const void BTreeIndex::scanNextHelper(RecordId & outRid, T lowVal, T highVal)
{
    if ( nextEntry == -1 ) 
      throw IndexScanCompletedException();

    T_LeafNode* thisPage = reinterpret_cast<T_LeafNode*>(currentPageData);

    if ( compare<T>(thisPage->keyArray[nextEntry], highVal) > 0 ) {
      throw IndexScanCompletedException();
    } else if ( compare<T>(highVal,thisPage->keyArray[nextEntry])==0 && highOp == LT ) {
    // check with LT or LTE
      throw IndexScanCompletedException();
    }
    outRid = thisPage->ridArray[nextEntry];
    shiftToNextEntry<T_LeafNode>(thisPage);
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
	if ( scanExecuting == false ) {
    	throw ScanNotInitializedException();
  } else {
    scanExecuting = false;
  }

  // unpins scanned pages
  bufMgr->unPinPage(file, currentPageNum, false);

}

}
