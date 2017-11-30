
#include "rm.h"

RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{
	rbfm = RecordBasedFileManager::instance();
	getTablesDescriptor(tablesDescriptor);
	getColumnsDescriptor(columnsDescriptor);
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
	const string tableName1 = "Tables";
	const string tableName2 = "Columns";
	const string tableName3 = "Indexes";


	if((rbfm -> createFile(tableName1)) == 0) {
		insertTablesRecord(1, tableName1);
		insertTablesRecord(2, tableName2);
		insertTablesRecord(3, tableName3);

		if(rbfm -> createFile(tableName2) == 0) {
			insertColumnsRecords(1, tableName1, tablesDescriptor);
			insertColumnsRecords(2, tableName2, columnsDescriptor);
			insertColumnsRecords(3, tableName3, indexesDescriptor);
			return 0;
		}
		if(rbfm -> createFile(tableName3) == 0) {
//			insertIndexesRecords();
			return 0;
		}
	}
	return -1;
}

RC RelationManager::deleteCatalog()
{
	if(rbfm->destroyFile("Tables")==0 && rbfm->destroyFile("Columns")==0) {
		return 0;
	}
	return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	int tid;
	if(rbfm -> createFile(tableName) == 0) {
		tid = getAvailableTableId();
//		cout << "creating " << tableName << ", tid = " << tid << endl;
		insertTablesRecord(tid, tableName);
		insertColumnsRecords(tid, tableName, attrs);
		return 0;
	}
	return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    if(IsSystemTable(tableName))
    		return -1;

	RID rid;
	rid.pageNum = 0;
	rid.slotNum = 0;
    int tid = getTableId(tableName, rid);
    	if(tid == -1)
    		return -1;

//    	cout << "deleting table, rid: " << rid.pageNum << ", " << rid.slotNum << endl;
    	FileHandle fileHandle_t;
    	rbfm -> openFile("Tables", fileHandle_t);
    	rbfm -> deleteRecord(fileHandle_t, tablesDescriptor, rid);
    	rbfm -> closeFile(fileHandle_t);

    	rid.pageNum = 0;
    	rid.slotNum = 0;
    	vector<string> attributeNames;
    	void *data = malloc(PAGE_SIZE);
    	FileHandle fileHandle;
    	RBFM_ScanIterator rbfm_ScanIterator;
    	rbfm-> openFile("Columns", fileHandle);

    	if(rbfm-> scan(fileHandle, columnsDescriptor, "table-id", EQ_OP, &tid, attributeNames, rbfm_ScanIterator) == 0) {
    		while(rbfm_ScanIterator.getNextRecord(rid,data) != EOF) {
    			rbfm -> deleteRecord(fileHandle, columnsDescriptor, rid);
    		}
    	}
    	free(data);
    	rbfm_ScanIterator.close();
    	rbfm-> closeFile(fileHandle);
    	rbfm-> destroyFile(tableName);
    	return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	RID rid;
	rid.pageNum = 0;
	rid.slotNum = 0;
	int tid = getTableId(tableName, rid);
//	cout << "have found tid of " << tableName << ", tid = " << tid << endl;
	if(tid == -1)
		return -1;
	int offset = 0;
	string tmpstr;
	Attribute attr;
	rid.pageNum = 0;
	rid.slotNum = 0;
	vector<string> attributeNames;
	attributeNames.push_back("table-id");
	attributeNames.push_back("column-name");
	attributeNames.push_back("column-type");
	attributeNames.push_back("column-length");
	void *data = malloc(PAGE_SIZE);
	FileHandle fileHandle;
	RBFM_ScanIterator rbfm_ScanIterator;
	rbfm-> openFile("Columns", fileHandle);
	//get attrs from "Columns" by scanning tid

	if(rbfm-> scan(fileHandle, columnsDescriptor, "table-id", EQ_OP, &tid, attributeNames, rbfm_ScanIterator) == 0) {
		while(rbfm_ScanIterator.getNextRecord(rid,data) != EOF) {
			offset = 0;
			//skip null indicator of columnsDescriptor(5 fields, 1 byte)
			offset++;
			//skip tid
			offset += sizeof(int);
			//convert original varChar data to string
			dataToString((char *)data+offset, tmpstr);
			//get attrName
			attr.name = tmpstr;
//			cout << "getAttr-name " << attr.name << endl;
			offset += (sizeof(int) + tmpstr.size());
			//get attrType
			memcpy(&attr.type, (char*)data+offset, sizeof(AttrType));
			offset += sizeof(AttrType);
			//get attrLength
			memcpy(&attr.length, (char*)data+offset, sizeof(AttrLength));
			offset += sizeof(AttrLength);
			attrs.push_back(attr);
		}
	}
	free(data);
	rbfm_ScanIterator.close();
	rbfm-> closeFile(fileHandle);
	return 0;
}

int RelationManager::getTableId(const string &tableName, RID &rid)
{
	FileHandle fileHandle;
	RBFM_ScanIterator rbfm_ScanIterator;
	void *data = malloc(PAGE_SIZE);
	vector<string> attributeNames;
	attributeNames.push_back("table-id");
	int tid = -1;
	int counter = 0;

	//get tid from "Tables" by scanning tableName
	int len = tableName.size();
	void *value = malloc(PAGE_SIZE);
	memcpy(value, &len, sizeof(int));
	memcpy((char *)value + sizeof(int), tableName.c_str(), len);

	rbfm-> openFile("Tables", fileHandle);

	if(rbfm->scan(fileHandle, tablesDescriptor, "table-name", EQ_OP, value, attributeNames, rbfm_ScanIterator) == 0) {         // formatted TypeVarChar Record!!!!!!!!!
		while(rbfm_ScanIterator.getNextRecord(rid, data) != EOF) {
			//skip null indicator (1bit in the first byte of the data)
			memcpy(&tid, (char*)data+1, sizeof(int));
			counter ++;
		}
		if (counter != 1) {
			free(value);
			free(data);
			rbfm_ScanIterator.close();
			rbfm-> closeFile(fileHandle);
			return -1;
		}
	}
	free(value);
	free(data);
	rbfm_ScanIterator.close();
	rbfm-> closeFile(fileHandle);
	return tid;
}


RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	FileHandle filehandle;
	vector<Attribute> descriptor;

	if(!IsSystemTable(tableName)){
		if(getAttributes(tableName,descriptor) == 0) {
			if(rbfm->openFile(tableName,filehandle)==0){
				if(rbfm->insertRecord(filehandle,descriptor,data,rid)==0){
					rbfm->closeFile(filehandle);
					return 0;
				}
				rbfm->closeFile(filehandle);
			}
		}
	}
	return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	FileHandle filehandle;
	vector<Attribute> descriptor;

	if(!IsSystemTable(tableName)){
		if(getAttributes(tableName,descriptor) == 0) {
			if(rbfm->openFile(tableName,filehandle) == 0){
				if(rbfm->deleteRecord(filehandle,descriptor,rid) == 0){
					rbfm->closeFile(filehandle);
					return 0;
				}
				rbfm->closeFile(filehandle);
			}
		}
	}
	return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	FileHandle filehandle;
	vector<Attribute> descriptor;

	if(!IsSystemTable(tableName)){
		if(getAttributes(tableName,descriptor) == 0) {
			if(rbfm->openFile(tableName,filehandle)==0){
				if(rbfm->updateRecord(filehandle,descriptor,data,rid)==0){
					rbfm->closeFile(filehandle);
					return 0;
				}
				rbfm->closeFile(filehandle);
			}
		}
	}
	return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	FileHandle filehandle;
	vector<Attribute> descriptor;
	if(getAttributes(tableName, descriptor) == 0) {
		if(rbfm->openFile(tableName,filehandle)==0){
			if(rbfm->readRecord(filehandle, descriptor, rid, data)==0){
				rbfm->closeFile(filehandle);
				return 0;
			}
			rbfm->closeFile(filehandle);
		}
	}
	return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{

	if(rbfm->printRecord(attrs,data)==0)
		return 0;
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    FileHandle fileHandle;
    rbfm->openFile(tableName, fileHandle);
    void* record = malloc(PAGE_SIZE);
    rbfm->readRecord(fileHandle, recordDescriptor, rid, record);
    rbfm->printRecord(recordDescriptor, record);
    free(record);
    rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
    rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    FileHandle tmpFileHandle;
	rbfm->openFile(tableName, tmpFileHandle);
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    rm_ScanIterator.prepareIterator(tmpFileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
    rbfm->closeFile(tmpFileHandle);
    return 0;
//  return rm_ScanIterator.prepareIterator(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
}

RC RM_ScanIterator::prepareIterator(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames){
//    this -> fileHandle = fileHandle;
	rbfm_Scaniterator.prepareIterator(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
    return 0;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
    return rbfm_Scaniterator.getNextRecord(rid, data);
}

RC RM_ScanIterator::close(){
//	rbfm->closeFile(fileHandle);
    return rbfm_Scaniterator.close();
}


// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

RC RelationManager::getTablesDescriptor(vector<Attribute> &tablesDescriptor)
{
    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    tablesDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tablesDescriptor.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tablesDescriptor.push_back(attr);

    return 0;
}


RC RelationManager::getColumnsDescriptor(vector<Attribute> &columnsDescriptor)
{
    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnsDescriptor.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    columnsDescriptor.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnsDescriptor.push_back(attr);

    attr.name = "column-length";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	columnsDescriptor.push_back(attr);

	attr.name = "column-position";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	columnsDescriptor.push_back(attr);

    return 0;
}

RC RelationManager::getIndexesDescriptor(vector<Attribute> &indexesDescriptor){
    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    indexesDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    indexesDescriptor.push_back(attr);

    attr.name = "attribute-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    indexesDescriptor.push_back(attr);

    return 0;
}

bool RelationManager::IsSystemTable(const string &tableName)
{
	return ((tableName == "Tables") || (tableName == "Columns"));
}

int RelationManager::getAvailableTableId()
{
	RM_ScanIterator rm_ScanIterator;
	int tid = -1;
	int tmpTid = 0;
	RID rid;
	void *data = malloc(PAGE_SIZE);
	int *pValue = new int;
	*pValue = 2;
	vector<string> attrName;
	attrName.push_back("table-id");
	rid.pageNum = 0;
	rid.slotNum = 0;

//	cout << "starting get available tid!!!" << endl;
	if(scan("Tables", "table-id", GE_OP, pValue, attrName, rm_ScanIterator) == 0) {
		while(rm_ScanIterator.getNextTuple(rid,data) != EOF) {
			//skip null indicator (1bit in the first byte of the data)
			memcpy(&tmpTid, (char*)data+1, sizeof(int));
//			cout << "In getAvailableTID, tmpTid = " << tmpTid << ", tid = " << tid << endl;
			tid = (tmpTid > tid) ? tmpTid : tid;
		}
	}
	tid++;
//	cout << "In getAvailableTID, final tid = " << tid << endl;

	delete pValue;
	free(data);
	rm_ScanIterator.close();
	return tid;
}

RC RelationManager::insertTablesRecord(const int &tid, const string &tableName)
{
	int offset = 0;
	int length;
	void *data = malloc(PAGE_SIZE);
	FileHandle fileHandle;
	RID rid;

	// generate null indicator of the record of "Tables"
	int nullFieldsIndicatorActualSize = ceil((double)tablesDescriptor.size() / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);
    memcpy(data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    // generate tid of the record of "Tables"
    memcpy((char*)data+offset, &tid, sizeof(int));
    offset += sizeof(int);

    // generate tableName of the record of "Tables"
    length = tableName.size();
    memcpy((char*)data+offset, &length, sizeof(int));
    offset += sizeof(int);
    // !!! The first byte of string in memory is '\n'. !!!
    // !!! So it should be convert to char*. Or use memcpy from (char *)(&s) + 1 !!!
    memcpy((char*)data+offset, tableName.c_str(), length);
    offset += length;

    // generate fileName of the record of "Tables"
    memcpy((char*)data+offset, &length, sizeof(int));
	offset += sizeof(int);
	memcpy((char*)data+offset, tableName.c_str(), length);
	offset += length;

    rbfm -> openFile("Tables", fileHandle);
    rbfm -> insertRecord(fileHandle, tablesDescriptor, data, rid);
    PageInfo pageInfo;
    fileHandle.readPage(rid.pageNum, data);
    memcpy(&pageInfo, (char*)data+PAGE_SIZE-sizeof(PageInfo), sizeof(PageInfo));
//    cout << "Inserting data to Tables, PageInfo.numOfSlots = " << pageInfo.numOfSlots << endl;
    rbfm -> closeFile(fileHandle);
//  printTuple(tablesDescriptor, data);
//	cout << "insert Tables RID" << rid.pageNum << ", " << rid.slotNum << endl;
    free(nullFieldsIndicator);
    free(data);
    return 0;
}

RC RelationManager::insertColumnsRecords(const int &tid, const string &tableName, const vector<Attribute> &attrs)
{
	int offset;
	int length;
	Attribute attribute;
	string attrName;
	AttrType attrType;
	AttrLength attrLength;
	void *data = malloc(PAGE_SIZE);
	FileHandle fileHandle;
	RID rid;

	rbfm -> openFile("Columns", fileHandle);

	for(int i = 0; i < attrs.size(); i++) {
		offset = 0;
		// generate null indicator of the record
		int nullFieldsIndicatorActualSize = ceil((double)columnsDescriptor.size() / CHAR_BIT);
	    unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	    memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);
	    memcpy(data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
	    offset += nullFieldsIndicatorActualSize;

	    //generate tid of the record
	    memcpy((char*)data+offset, &tid, sizeof(int));
	    offset += sizeof(int);

	    //generate attrName of the record
		attribute = attrs[i];
		attrName = attribute.name;
		length = attrName.size();
		memcpy((char*)data+offset, &length, sizeof(int));
		offset += sizeof(int);
		memcpy((char*)data+offset, attrName.c_str(), length);
		offset += length;

	    //generate attrType of the record
		attrType = attribute.type;
		length = sizeof(AttrType);
		memcpy((char*)data+offset, &attrType, length);
		offset += length;

		//generate attrLength of the record
		attrLength = attribute.length;
		length = sizeof(AttrLength);
		memcpy((char*)data+offset, &attrLength, length);
		offset += length;

		//generate attrPosition of the record
		int num = i+1;
		memcpy((char*)data+offset, &num, sizeof(int));
		offset += sizeof(int);

		rbfm -> insertRecord(fileHandle, columnsDescriptor, data, rid);
//		rbfm -> readRecord(fileHandle, columnsDescriptor, rid, data);
//		printTuple(columnsDescriptor, data);
//		cout << endl;
	    free(nullFieldsIndicator);
	}

    rbfm -> closeFile(fileHandle);
    free(data);
    return 0;
}

RC RelationManager::insertIndexesRecords(const int &tid, const string &tableName, const string &attributeName){
    int offset = 0;
    int length;
    void *data = malloc(PAGE_SIZE);
    FileHandle fileHandle;
    RID rid;

    memcpy((char *)data+offset, &tid, sizeof(int));
    offset += sizeof(int);

    length = tableName.size();
    memcpy((char *)data+offset, &length, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)data+offset, tableName.c_str(), length);
    offset += length;

    length = attributeName.size();
    memcpy((char *)data+offset, &length, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)data+offset, attributeName.c_str(), length);
    offset += length;

    rbfm->openFile("Indexes", fileHandle);
    rbfm->insertRecord(fileHandle, indexesDescriptor, data, rid);
    rbfm->closeFile(fileHandle);
    free(data);
    return 0;
}

RC RelationManager::dataToString(void *data, string &str)
{
	int size = 0;
	char *varChar = (char *)malloc(PAGE_SIZE);

	memcpy(&size, data, sizeof(int));
	memcpy(varChar, (char*)data+sizeof(int), size);

	varChar[size] = '\0';
	string tmpstr(varChar);
	str = tmpstr;

	free(varChar);
	return 0;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{

	return -1;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	return -1;
}

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
	return -1;
}



