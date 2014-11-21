/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <iostream>

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
    RC rc = pf.open(indexname, mode);
    if(rc < 0) return rc;
    switch(mode) {
    case 'r':
    case 'R': 
        // read rootPid and treeHeight
        return readRootAndHeight();
    case 'w':
    case 'W':
    {
        if(pf.endPid() == 0) {
            // create new root page
            rootPid = 1;
            // set tree height to 0
            treeHeight = 0;
            // write the rootPid and treeHeight into file
            return writeRootAndHeight();
        }else
            return readRootAndHeight();
    }
    default:
        return RC_INVALID_FILE_MODE;
    }
}

/*
 * Write rootPid and treeHeight to file.
 * @return error code, 0 if no error
 */
RC BTreeIndex::writeRootAndHeight() {
    // create a buffer in main memory
    char buffer[PageFile::PAGE_SIZE];
    // initialize buffer to all 0
    memset(buffer, 0, PageFile::PAGE_SIZE);
    // make the root pid to be the last page id
    memcpy(buffer, &rootPid, sizeof(PageId));
    // set the tree height to be 0
    memcpy(buffer + sizeof(PageId), &treeHeight, sizeof(int));
    // write the buffer to the page file
    return pf.write(0, buffer);
}

/*
 * Read rootPid and treeHeight from file.
 * @return error code, 0 if no error
 */
RC BTreeIndex::readRootAndHeight() {
    RC rc;
    // create a buffer in main memory
    char buffer[PageFile::PAGE_SIZE];
    // read out content from the first page
    if((rc = pf.read(0, buffer)) < 0) return rc;
    // copy root pid and tree height
    memcpy(&rootPid, buffer, sizeof(PageId));
    memcpy(&treeHeight, buffer + sizeof(PageId), sizeof(int));
    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
    return pf.close();
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
    RC rc;
    char page[PageFile::PAGE_SIZE];
    PageId nextPid = pf.endPid();
    printf("nextpid = %d\n",nextPid);
    if(nextPid == 1) {// the index file is empty, need initialization
        // update rootPid and treeHeight
        rootPid = nextPid;
        treeHeight = 1;
        // create a leaf node as root node
        BTLeafNode rootNode;
        // insert key and rid as the first entry
        rootNode.insert(key, rid);
        // write to file
        rootNode.write(nextPid, pf);
        // update rootPid and treeHeight to file
        rc = writeRootAndHeight();
        // testing read from .tbl
        printf("key count = %d\n", rootNode.getKeyCount());
        // read keys
        for (int i = 0; i < rootNode.getKeyCount(); i ++) {
            int key;
            RecordId rid;
            rootNode.readEntry(i, key, rid);
            cout<<i<<"th entry: \t"<<"key = "<<key<<"\trecordId.pid = "<<rid.pid<<"\trecordId.sid"<<rid.sid<<endl;
        }
        return 0;
    }
    int eid;
    return recursivelyInsert(key, rid, rootPid, eid, treeHeight);
}

RC BTreeIndex::insertAndSplit(BTLeafNode& currNode, PageId& currPid, int eid, int& key, const RecordId& rid) {
    // need a new leaf node to store the keys
    BTLeafNode siblingNode;
    // store the page id of sibling node
    PageId siblingPid = pf.endPid();
    // get the returned sibling key
    int siblingKey;
    // call insertAndSplit
    currNode.insertAndSplit(key, rid, eid, siblingNode, siblingKey);
    // set the next pointer of currNode to siblingNode
    currNode.setNextNodePtr(siblingPid);
    // write changes to file
    currNode.write(currPid, pf);
    siblingNode.write(siblingPid, pf);
    // now we have siblingKey and siblingPid, we can insert it to parent node

    // if there is no non-leaf node exists, initialize root
    if(treeHeight == 1) 
        initializeRoot(currPid, key, siblingPid);
    currPid = siblingPid;
    key = siblingKey;
}

RC BTreeIndex::insertAndSplit(BTNonLeafNode& currNode, PageId& currPid, int eid, int& key, const PageId& pid) {
    // need a new non leaf node to store the keys
    BTNonLeafNode siblingNode;
    // store the page id of sibling node
    PageId siblingPid = pf.endPid();
    // get the returned mid key
    int midKey;
    // call insertAndSplit
    currNode.insertAndSplit(key, pid, eid, siblingNode, midKey);
    // set the next pointer of currNode to siblingNode
    //currNode.setNextNodePtr(siblingPid);
    // write changes to file
    currNode.write(currPid, pf);
    siblingNode.write(siblingPid, pf);
    // now we have midKey and siblingPid, we can insert it to parent node

    // if we need a new root, initialize a root
    if(currPid == rootPid) 
        initializeRoot(currPid, key, siblingPid);
    currPid = siblingPid;
    key = midKey;
}

RC BTreeIndex::initializeRoot(const PageId& currPid, int key, const PageId& siblingPid) {
    // create a new non-leaf node as root
    BTNonLeafNode rootNode;
    // get a new pid as root pid
    rootPid = pf.endPid();
    treeHeight++;
    // initialize the root
    rootNode.initializeRoot(currPid, key, siblingPid);
    // write changes to file
    rootNode.write(rootPid, pf);
    // update root and tree height
    writeRootAndHeight();
}

RC BTreeIndex::recursivelyInsert(int& searchKey, const RecordId& rid, PageId& pid, int& eid, int level)
{
    RC rc;
    // store current pid
    PageId currPid = pid;
    // base case: insert into leaf node
    if(level == 1) {
        // this is the leaf level
        BTLeafNode leafNode;
        // read the page into a leaf node
        if((rc = leafNode.read(pid, pf)) < 0) return rc;
        // find eid
        leafNode.locate(searchKey, eid);
        // insert the key
        if(leafNode.getKeyCount() == BTLeafNode::MAX_KEY_NUM) {
            // need to split
            insertAndSplit(leafNode, pid, eid, searchKey, rid);
            // if this level is full, return rc
            return RC_NODE_FULL;
        } else {
            // insert
            leafNode.insertAtEid(searchKey, rid, eid);
            // write changes to file
            leafNode.write(currPid, pf);
            return 0;
        }
    }
    // read the page into a non-leaf node
    BTNonLeafNode nonleafNode;

    if((rc = nonleafNode.read(currPid, pf)) < 0) return rc;
    // locate child pointer
    rc = nonleafNode.locateChildPtr(searchKey, pid, eid);
    if (rc < 0) return rc;
    // recursively go down a level
    rc = recursivelyInsert(searchKey, rid, pid, eid, level - 1);
    // check if the next level is full
    if(rc == RC_NODE_FULL) {
        // need insert into this node
        if(nonleafNode.getKeyCount() == BTNonLeafNode::MAX_KEY_NUM) {
            // need to split
            insertAndSplit(nonleafNode, currPid, eid, searchKey, pid);
            pid = currPid;
            // if this level is full, return rc
            return RC_NODE_FULL;
        } else {
            // insert
            nonleafNode.insertAtEid(searchKey, pid, eid);
            // write changes to file
            nonleafNode.write(currPid, pf);
            return 0;
        }
    } else
        return rc;
}

/*
 * Find the leaf-node index entry whose key value is larger than or 
 * equal to searchKey, and output the location of the entry in IndexCursor.
 * IndexCursor is a "pointer" to a B+tree leaf-node entry consisting of
 * the PageId of the node and the SlotID of the index entry.
 * Note that, for range queries, we need to scan the B+tree leaf nodes.
 * For example, if the query is "key > 1000", we should scan the leaf
 * nodes starting with the key value 1000. For this reason,
 * it is better to return the location of the leaf node entry 
 * for a given searchKey, instead of returning the RecordId
 * associated with the searchKey directly.
 * Once the location of the index entry is identified and returned 
 * from this function, you should call readForward() to retrieve the
 * actual (key, rid) pair from the index.
 * @param key[IN] the key to find.
 * @param cursor[OUT] the cursor pointing to the first index entry
 *                    with the key value.
 * @return error code. 0 if no error.
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
    RC rc;
    // initially cursor.pid = rootPid
    cursor.pid = rootPid;
    // read the root node into buffer
    char page[PageFile::PAGE_SIZE];

    int currLevel = 1;
    for(; currLevel < treeHeight + 1; currLevel++) {
        // now cursor.pid is the pid of a node at nth level
        if(currLevel == treeHeight) { // if we want to locate the key at leaf nodes
            // read the page into a leaf node
            BTLeafNode leafNode;
            if((rc = leafNode.read(cursor.pid, pf)) < 0) return rc;
            // locate searchKey
            return leafNode.locate(searchKey, cursor.eid);
        } 
        // read the page into a non-leaf node
        BTNonLeafNode nonleafNode;
        if((rc = nonleafNode.read(cursor.pid, pf)) < 0) return rc;
        // locate child pointer
        rc = nonleafNode.locateChildPtr(searchKey, cursor.pid, cursor.eid);
        if (rc < 0) return rc;
    }
    return 0;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
    RC rc;
    // read the page as a leaf node
    BTLeafNode currNode;
    if((rc = currNode.read(cursor.pid, pf)) < 0) return rc;
    // read the entry with eid
    if((rc = currNode.readEntry(cursor.eid, key, rid)) < 0) return rc;
    return 0;
}
