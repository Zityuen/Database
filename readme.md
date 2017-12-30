## Project Framework
- File System       /rf
- Record Manager    /rbf
- Index Manager     /ix
- Query Engine      /qe

## Details
- OS(bit): CentOS Linux release 7.3.1611 (Core)
- gcc version : gcc version 4.8.5 20150623 (Red Hat 4.8.5-11) (GCC)
- Meta-data in Record File: It will create two files named Tables and Columns, each of them is the same as file format which includes a headpage and many pages. The attributes of record in the page of Tables are table-id, table-name, file-name. The attributes of record in the page of Columns are table-id, column-name, column-type, column-length, column-position. The record of Tables and Columns will be inserted in Tables file and Columns file.
- Internal Record Format: If the length of format record is no less than sizeof(RID), record format includes fieldpointers and fields. Each of fieldpointers use short int to store the offset pointing to the end position of corresponding field.
- Page Format: Put records from the beginning of the page and set the directory from the end of the page. The directory includes two structs. One is PageInfo, which includes recordofsize and numofslots used to get the freespace and the number of the slots. Another is recordoffset, which consists of slot directory, as well as includes the position of record and the length of record.
- Update: Use RID to locate the corresponding recordoffset(slot). If the recordoffset.length is -1 which means there is a TombStone, the actual data stored in current record is the new address (new rid) and we will jump to this new rid. We will do this process repeatedly until the length is not 0. When we find the record and get the difference of the length between new record and old record. If there is enough freespace for the new data, the data behind this record need to be shifted to right or left depending on the difference (the new record is longer or shorter). If there is no enough freespace for the new data, we will mark this record's recordoffset as a TombStone, find a new address and store new data in the new address.
- Delete: Similar to Update, we need to recursively find the actual position of the record whose recordoffset’s length is not tombstone mark. And the data behind this record will be shifted to the beginning of this record for overwriting it. Then the address pointer in tombstone record will be overwritten and tombstone mark in the slot will become deleted mark one by one when we complete the recursion.
- Meta-data page in Index File: Compared to previous meta-data, added a rootNodePage to indicate the pageNum of the root node.
- Index Entry Format: 
    - Internal-page entry design: <original_Key + RID> + pageid
    - Leaf-page design: <original_Key + RID>
- Catalog Information about Index: 
    - In the "Tables" table, "table-id", "table-name" and "file-name" will be recorded.
    - In the "Coloumns" table, each table's attribute will be recorded.
    - In the "Indexes" table, "tablename", "attributename" and "filename" will be recorded.
- Implemented Block Nested Loop Join and Index Nested Loop Join.
- CreateIndex: First call createfile function in ix to create index file and insert record in "Indexes" table. Call openfile function in rm to open the "tableName" table, then use scan and getNext to get each record in the table. Call insertTuple function in ix to insert entry in the index file until scanning finish.
