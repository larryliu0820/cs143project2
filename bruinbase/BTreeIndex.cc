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

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    fd = -1;
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
    return pf.write(0, buffer));
}

/*
 * Read rootPid and treeHeight from file.
 * @return error code, 0 if no error
 */
RC readRootAndHeight() {
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
    if(nextPid == 1) {// the index file is empty, need initialization
        // update rootPid and treeHeight
        rootPid = nextPid;
        treeHeight = 1;
        // create a leaf node as root node
        BTLeafNode rootNode;
        // read the new root node
        if((rc = rootNode.read(nextPid, pf)) < 0) return rc;
        // insert key and rid as the first entry
        rootNode.insert(key, rid);
        // set the next sibling node PageId
        rootNode.setNextNodePtr(nextPid+1);
        // write to file
        rootNode.write(nextPid, pf);
        // update rootPid and treeHeight to file
        return writeRootAndHeight();
    }
    IndexCursor cursor;
    // Find the leaf-node index entry and insert the node (key, rid)
    locate(key, cursor);
    // read the page at cursor.pid as leaf node
    BTLeafNode currNode;
    if((rc = currNode.read(cursor.pid, pf)) < 0) return rc;

    // check if eid is smaller than MAX_KEY_NUM
    if(eid < BTLeafNode::MAX_KEY_NUM) {
        currNode.insert(key, rid);
    } else if(eid == BTLeafNode::MAX_KEY_NUM) {
        // need a new leaf node to store the keys
        BTLeafNode siblingNode;
        // initialize sibling node, open a new page
        siblingNode.read(pf.endPid(), pf);
        // get the returned sibling key
        int siblingKey;
        // call insertAndSplit
        insertAndSplit(key, rid, siblingNode, siblingKey);
        // 
    }
    return 0;
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

    while(true) {
        if((rc = pf.read(cursor.pid, page)) < 0) return rc;
        // check the first byte 
        if(page[0] == 'L') {
            // read the page into a leaf node
            BTLeafNode leafNode;
            if((rc = leafNode.read(cursor.pid, pf)) < 0) return rc;
            // locate searchKey
            return leafNode.locate(searchKey, cursor.eid);
        } else if(page[0] == 'N') {
            // read the page into a non-leaf node
            BTNonLeafNode nonleafNode;
            if((rc = nonleafNode.read(cursor.pid, pf)) < 0) return rc;
            // locate child pointer
            rc = nonleafNode.locateChildPtr(searchKey, cursor.pid);
            if (rc < 0) return rc;
        } else { // wrong page id
            return RC_INVALID_PID;
        }
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
    // read the page as a leaf node
    BTLeafNode currNode;
    if((rc = currNode.read(cursor.pid, pf)) < 0) return rc;
    // read the entry with eid
    if((rc = currNode.readEntry(cursor.eid, key, rid)) < 0) return rc;
    return 0;
}
