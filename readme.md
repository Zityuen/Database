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
- Meta-data page in Index File: Compared to previous meta-data, added a rootNodePage to indicate the pageNum of the root node.
- Index Entry Format: Internal-page entry design: <original_Key + RID> + pageid; Leaf-page design: <original_Key + RID>;
- Catalog Information about Index: 
    - In the "Tables" table, "table-id", "table-name" and "file-name" will be recorded.
    - In the "Coloumns" table, each table's attribute will be recorded.
    - In the "Indexes" table, "tablename", "attributename" and "filename" will be recorded.
