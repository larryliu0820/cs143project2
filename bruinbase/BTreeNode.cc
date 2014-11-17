#include "BTreeNode.h"

using namespace std;

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ 
	RC rc;
	//read the page with PageId pid from PageFile pf
	if ((rc = pf.read(pid,buffer)) < 0)	return rc;
	return 0; 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
	RC rc;
	//write the content in buffer into the page with PageId pid
	if((rc = pf.write(pid, buffer)) < 0) return rc;
	return 0; 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{
	// the first four bytes of a page contains # keys in the page
	int count;
	memcpy(buffer, &count, sizeof(int)); 
	return count; 
}

/*
 * Set the number of keys stored in the node.
 * @param count[IN] the key count to be written in buffer.
 * @return 0 if successful. Return an error code if there is an error.
 */
 RC BTLeafNode::setKeyCount(int count)
 {
 	//the first four bytes of a page contains # keys in the node/page.
 	memcpy(buffer, &count, sizeof(int));
 }

/*
 * Return the pointer to the new entry in the buffer.
 * @param eid[IN] the key count to be written in buffer.
 * @return the pointer.
 */
char* BTLeafNode::entryPtr(int eid)
{
	return buffer + eid * (sizeof(int) + sizeof(RecordId)) + sizeof(int);
}

/*
 * Write an entry to a given position in the buffer.
 * @param ptr[IN] the pointer pointing to the position to insert a new entry.
 * @param key[IN] the key to be inserted to buffer.
 * @param rid[IN] the RecordId to be inserted to buffer.
 * @return 0 if successful, return an error code if error occurs.
 */
 RC BTLeafNode::writeToPtr(char* ptr, int key, const RecordId& rid)
 {
 	//store the key
 	memcpy(ptr, &key, sizeof(int));
 	//store the RecordId
 	memcpy(ptr + sizeof(int), &rid, sizeof(RecordId));
 	return 0;
 }

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 
	RC rc;
	// get the number of keys in page
	int count = getKeyCount();
	// check if it exceeds MAX_KEY_NUM
	if(count == MAX_KEY_NUM) {
		return RC_NODE_FULL;
	} else if(count < MAX_KEY_NUM) {
		char* ptr = entryPtr(0);
		int number = 0;

		// find out where the key should be inserted
		locate(key, number);

		// how many keys are bigger than key
		int remain = count - number;
		// copy the remaining entries into buf
		size_t remainSize = (sizeof(int) + sizeof(RecordId)) * remain + sizeof(int)
		char buf[remainSize];
		memcpy(buf, ptr, remainSize);
		// write the new entry into buffer
		writeToPtr(ptr, key, rid);
		// copy back from buf to buffer
		memcpy(ptr + sizeof(int) + sizeof(RecordId), buf, remainSize);
		// increase the count of keys by 1
		setKeyCount(count + 1);
	}
	return 0; 
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
	// get the eid of half of the entries
	int halfCount = MAX_KEY_NUM / 2;
	// get the location of the half point of the old node
	char *halfPtr = entryPtr(halfCount);
	// copy the second half to the new node
	size_t secondSize = (MAX_KEY_NUM - halfCount) * (sizeof(int) + sizeof(RecordId));
	memcpy(sibling.entryPtr(0), halfPtr, secondSize);
	// copy next node pointer to half point of the old node
	memcpy(halfPtr, entryPtr(MAX_KEY_NUM), sizeof(int));
	// update # of keys of the old node
	setKeyCount(halfCount);
	// update # of keys of the new node
	sibling.setKeyCount(MAX_KEY_NUM - halfCount);
	// insert the new key into the new node
	sibling.insert(key, rid);
	// find the first key in the sibling node after split
	RecordId firstRid;
	sibling.readEntry(0, siblingKey, firstRid);
	return 0; 
}

/*
 * Find the entry whose key value is larger than or equal to searchKey
 * and output the eid (entry number) whose key value >= searchKey.
 * Remeber that all keys inside a B+tree node should be kept sorted.
 * @param searchKey[IN] the key to search for
 * @param eid[OUT] the entry number that contains a key larger than or equalty to searchKey
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{ 
	RC rc;
	int temp;
	RecordId rid;
	int number = 0;
	int keyCount = getKeyCount();
	// get the first key
	readEntry(number, temp, rid);
	while(temp < searchKey && number < keyCount) {
		number++;
		readEntry(number, temp, rid);
	}
	// set eid = number
	eid = number;
	// if no key is larger than searchKey, return 
	return 0;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{
	// get to the location in buffer
	char *ptr = entryPtr(eid);
	// read key first, then rid
	memcpy(&key, ptr, sizeof(int));
	memcpy(&rid, ptr + sizeof(int), sizeof(RecordId));
	return 0; 
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{ 
	// get the key count
	int keyCount = getKeyCount();
	// get the location at the end of last entry
	char *ptr = entryPtr(keyCount);
	// get the PageId of next node
	PageId pid;
	memcpy(&pid, ptr, sizeof(PageId));

	return pid; 
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{ 
	// get the key count
	int keyCount = getKeyCount();
	// get the location at the end of last entry
	char *ptr = entryPtr(keyCount);
	// set the new PageId of next node
	memcpy(ptr, &pid, sizeof(PageId));
	return 0; 
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ 
	RC rc;
	//read the page with PageId pid from PageFile pf
	if ((rc = pf.read(pid,buffer)) < 0)	return rc;
	return 0; 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ 
	RC rc;
	//write the content in buffer into the page with PageId pid
	if((rc = pf.write(pid, buffer)) < 0) return rc;
	return 0; 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{ 
	// the first four bytes of a page contains # keys in the page
	int count;
	memcpy(buffer, &count, sizeof(int)); 
	return count;  
}

/*
 * Set the number of keys stored in the node.
 * @param count[IN] the key count to be written in buffer.
 * @return 0 if successful. Return an error code if there is an error.
 */
 RC BTNonLeafNode::setKeyCount(int count)
 {
 	//the first four bytes of a page contains # keys in the node/page.
 	memcpy(buffer, &count, sizeof(int));
 }

/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ 
	RC rc;
	// get the number of keys in page
	int count = getKeyCount();
	// check if it exceeds MAX_KEY_NUM
	if(count == MAX_KEY_NUM) {
		return RC_NODE_FULL;
	} else if(count < MAX_KEY_NUM) {
		char* ptr = entryPtr(0);
		int number = 0;
		// find out where the key should be inserted
		locate(key, number);
		// how many keys are bigger than key
		int remain = count - number;
		// copy the remaining entries into buf
		size_t remainSize = sizeof(int) * 2 * remain + sizeof(int)
		char buf[remainSize];
		memcpy(buf, ptr, remainSize);
		// write the new entry into buffer
		writeToPtr(ptr, key, pid);
		// copy back from buf to buffer
		memcpy(ptr + sizeof(int) + sizeof(PageId), buf, remainSize);
		// increase the count of keys by 1
		setKeyCount(count + 1);
	}
	return 0; 
}

/*
 * Write an entry to a given position in the buffer.
 * @param ptr[IN] the pointer pointing to the position to insert a new entry.
 * @param key[IN] the key to be inserted to buffer.
 * @param pid[IN] the PageId to be inserted to buffer.
 * @return 0 if successful, return an error code if error occurs.
 */
 RC BTNonLeafNode::writeToPtr(char* ptr, int key, const PageId& pid)
 {
 	//store the key
 	memcpy(ptr, &key, sizeof(int));
 	//store the PageId
 	memcpy(ptr + sizeof(int), &pid, sizeof(PageId));
 	return 0;
 }

/*
 * Return the pointer to the new entry in the buffer.
 * @param eid[IN] the key count to be written in buffer.
 * @return the pointer.
 */
char* BTNonLeafNode::entryPtr(int eid)
{
	return buffer + eid * (sizeof(int) + sizeof(PageId)) + sizeof(int);
}

/**
* Read the (key, rid) pair from the eid entry.
* @param eid[IN] the entry number to read the (key, rid) pair from
* @param key[OUT] the key from the slot
* @param pid[OUT] the PageId from the slot
* @return 0 if successful. Return an error code if there is an error.
*/
RC BTNonLeafNode::readEntry(int eid, int& key, PageId& pid) 
{
	// get to the location in buffer
	char *ptr = entryPtr(eid);
	// read key first, then pid
	memcpy(&key, ptr, sizeof(int));
	memcpy(&pid, ptr + sizeof(int), sizeof(PageId));
	return 0; 
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ 
	// get the eid of half of the entries
	int halfCount = MAX_KEY_NUM / 2;
	// get the location of the half point of the old node
	char *halfPtr = entryPtr(halfCount);
	// copy the first key of second half to midKey
	memcpy(&midKey, halfPtr + sizeof(PageId), sizeof(int));
	// copy the second half to the new node except for the middle key
	size_t secondSize = (MAX_KEY_NUM - halfCount - 1) * (sizeof(int) + sizeof(PageId));
	memcpy(sibling.entryPtr(0), halfPtr + sizeof(PageId) + sizeof(int), secondSize);
	// copy next node pointer to half point of the old node
	memcpy(halfPtr, entryPtr(MAX_KEY_NUM), sizeof(int));
	// update # of keys of the old node
	setKeyCount(halfCount);
	// update # of keys of the new node (we didn't insert the middle key)
	sibling.setKeyCount(MAX_KEY_NUM - halfCount - 1);
	// insert the new key into the new node
	sibling.insert(key, pid);
	return 0; 
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ 
	int eid = 0;
	int tempKey;
	int count = getKeyCount();
	readEntry(eid, tempKey, pid);
	while()
	return 0; 
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ return 0; }
