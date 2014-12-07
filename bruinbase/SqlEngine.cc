/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <iostream>
#include <fstream>
#include <string>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

vector<SelCond> SqlEngine::getUsefulCond(const vector<SelCond>& cond) {
    SelCond smaller; //initialize with 0
    SelCond larger; // initialize with MAX_INT
    SelCond equal; //initialize -1
    SelCond inequal;// initialize with -1
    //initialize

    smaller.value = "-2147483648";
    larger.value = "2147483647";
    equal.value = "-2147483648";
    inequal.value = "-2147483648";

    // the vector we are going to return
    vector<SelCond> usefulCond;
    // get the boundary value from the query condition
    for(int i = 0; i < cond.size(); i ++ ){
        switch(cond[i].comp) {
            case SelCond::EQ:
                if (atoi(equal.value) != INT_MIN) return usefulCond;
                equal = cond[i];
                break;
            case SelCond::NE:
                usefulCond.push_back(cond[i]);
                break;
            case SelCond::GT:
            case SelCond::GE: 
                if(atoi(cond[i].value) > atoi(larger.value)) return usefulCond;
                if(atoi(cond[i].value) > atoi(smaller.value))
                    smaller = cond[i];
                break;
            case SelCond::LT:
            case SelCond::LE:
                if(atoi(cond[i].value) < atoi(smaller.value)) return usefulCond;
                if(atoi(cond[i].value) < atoi(larger.value))
                    larger = cond[i];
                break;
        }
        
    }
    if (atoi(equal.value) != INT_MIN) {
        // key = x and key < x-1
        if (atoi(larger.value) != INT_MAX) {
            if (atoi(equal.value) > atoi(larger.value)) return usefulCond;
        }
        // key = x and key > x+1
        if (atoi(smaller.value) != INT_MIN) {
            if (atoi(equal.value) < atoi(smaller.value)) return usefulCond;
        }
        // key = x and key <> x, invalid
        if (atoi(equal.value) == atoi(inequal.value)) return usefulCond;
        usefulCond.push_back(equal);
        return usefulCond;
    }
    if (cond.size() == 1) {
        if(cond[0].comp == SelCond::GT || cond[0].comp == SelCond::GE)
            usefulCond.push_back(smaller);
        else if(cond[0].comp == SelCond::LT || cond[0].comp == SelCond::LE)
            usefulCond.push_back(larger);
        return usefulCond;
    }
    
    if (atoi(larger.value) != INT_MAX){
        usefulCond.insert(usefulCond.begin(),larger);
    }
    if (atoi(smaller.value) != INT_MIN){
        usefulCond.insert(usefulCond.begin(),smaller);
    }
    return usefulCond;
}

RC SqlEngine::getStartEntry(const vector<SelCond>& usefulCond, BTreeIndex& idx, IndexCursor& cursor) {
    // if usefulCond == 0, the query is invalid
    if(usefulCond.size() == 0)
        return RC_NO_SUCH_RECORD;
    int key;
    RecordId rid;
    RC rc;
    
    if (usefulCond.size() == 1)
    {
        switch (usefulCond[0].comp) {
            case SelCond::EQ:
            case SelCond::GE:
                idx.locate(atoi(usefulCond[0].value), cursor);
                break;
            case SelCond::GT:
                rc = idx.locate(atoi(usefulCond[0].value), cursor);
                if (rc == 0) { // key exists
                    idx.readForward(cursor, key, rid);
                }
                break;
            case SelCond::NE:
            case SelCond::LE:
            case SelCond::LT:
                idx.locate(0, cursor);
                break;
        }
    }else if(usefulCond.size() > 1)
    {
        if (usefulCond[0].comp == SelCond::GE || usefulCond[0].comp == SelCond::GT)
            rc = idx.locate(atoi(usefulCond[0].value), cursor);
        else
            rc = idx.locate(0, cursor);
        if (usefulCond[0].comp == SelCond::GT && rc == 0) {
            idx.readForward(cursor,key,rid);
        }
    }
    return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
    RecordFile rf;   // RecordFile containing the table
    RecordId   rid;  // record cursor for table scanning
    BTreeIndex idx;  // index for searching in the table
    IndexCursor cursor;
    
    RC     rc;
    int    key;
    string value;
    int    count;
    int    diff=1;
    
    // when attr = 2 or attr = 3, open the table file
    if(attr == 2 || attr == 3) {
        if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
            fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
            return rc;
        }
    }

    count = 0;
    if (idx.open(table + ".idx", 'r')==0) {
        vector<SelCond> usefulCond;
        if (cond.size() == 0)
        {
            if (attr != 4)  goto scan;
            else
            {
                idx.locate(0, cursor);
                usefulCond = cond;
                goto condition_check;
            }
        }
        
        // parse the conditions and return useful conditions
        usefulCond = getUsefulCond(cond);
        // get the start entry
        rc = getStartEntry(usefulCond, idx, cursor);
        if (rc < 0) goto exit_select;
        
condition_check:
        while ((idx.readForward(cursor, key, rid)) == 0) {
            // check the conditions on the tuple
            for (unsigned i = 0; i < usefulCond.size(); i++) {
                // compute the difference between the tuple value and the condition value
                diff = key - atoi(usefulCond[i].value);
                //fprintf(stdout, "usefulCond[%d].value = %s, diff = %d\n",i,usefulCond[i].value, diff);
                // check the condition
                switch (usefulCond[i].comp) {
                    case SelCond::EQ:
                        if (diff != 0) goto end_find;
                        else goto find_match;
                    case SelCond::NE:
                        if (diff == 0) goto condition_check;
                        break;
                    case SelCond::LT:
                        if (diff >= 0) goto end_find;
                        break;
                    case SelCond::LE:
                        if (diff > 0) goto end_find;
                        break;
                    case SelCond::GT:
                        if (diff <= 0) goto end_find;
                        break;
                    case SelCond::GE:
                        if (diff < 0) goto end_find;
                        break;
                    default:
                        break;
                }
            }

find_match:
            count++;
            switch (attr) {
                case 1:  // SELECT key
                    fprintf(stdout, "%d\n", key);
                    break;
                case 2:  // SELECT value
                    // read the tuple
                    if ((rc = rf.read(rid, key, value)) < 0) {
                        goto exit_select;
                    }
                    fprintf(stdout, "%s\n", value.c_str());
                    break;
                case 3:  // SELECT *
                    // read the tuple
                    if ((rc = rf.read(rid, key, value)) < 0) {
                        goto exit_select;
                    }
                    fprintf(stdout, "%d '%s'\n", key, value.c_str());
                    break;
            }
        }
    }
    else{
    scan:
        // scan the table file from the beginning
        rid.pid = rid.sid = 0;
        
        while (rid < rf.endRid()) {
            // read the tuple
            if ((rc = rf.read(rid, key, value)) < 0) {
                fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
                goto exit_select;
            }
            
            // check the conditions on the tuple
            for (unsigned i = 0; i < cond.size(); i++) {
                // compute the difference between the tuple value and the condition value
                switch (cond[i].attr) {
                    case 1:
                        diff = key - atoi(cond[i].value);
                        break;
                    case 2:
                        diff = strcmp(value.c_str(), cond[i].value);
                        break;
                }
                
                // skip the tuple if any condition is not met
                switch (cond[i].comp) {
                    case SelCond::EQ:
                        if (diff != 0) goto next_tuple;
                        break;
                    case SelCond::NE:
                        if (diff == 0) goto next_tuple;
                        break;
                    case SelCond::GT:
                        if (diff <= 0) goto next_tuple;
                        break;
                    case SelCond::LT:
                        if (diff >= 0) goto next_tuple;
                        break;
                    case SelCond::GE:
                        if (diff < 0) goto next_tuple;
                        break;
                    case SelCond::LE:
                        if (diff > 0) goto next_tuple;
                        break;
                }
            }
            
            // the condition is met for the tuple.
            // increase matching tuple counter
            count++;
            
            // print the tuple
            switch (attr) {
                case 1:  // SELECT key
                    fprintf(stdout, "%d\n", key);
                    break;
                case 2:  // SELECT value
                    fprintf(stdout, "%s\n", value.c_str());
                    break;
                case 3:  // SELECT *
                    fprintf(stdout, "%d '%s'\n", key, value.c_str());
                    break;
            }
            
            // move to the next tuple
        next_tuple:
            ++rid;
        }
    }
    
end_find:
    // print matching tuple count if "select count(*)"
    if (attr == 4) {
        fprintf(stdout, "%d\n", count);
    }
    rc = 0;
    
    // close the table file and return
exit_select:
    rf.close();
    return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  /* your code here */
    RecordFile* rf=new RecordFile(table + ".tbl", 'w');
    ifstream in(loadfile.c_str());
    if(!in.is_open())
    {
        cout << "Error opening file"<<endl;
        exit(1);
    }
    string buffer;
    BTreeIndex btnode;
    while (in.good() && getline(in,buffer)) {
        int key;
        string value;
        RecordId id;
        IndexCursor cursor;
        
        parseLoadLine(buffer,key,value);
        //write the key,value pair into Recordfile
        rf->append(key,value,id);
        if(index == true) {
            btnode.open(table + ".idx",'w');
            btnode.insert(key,id);
            btnode.close();
        }
    }
    rf->close();
    
  return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
