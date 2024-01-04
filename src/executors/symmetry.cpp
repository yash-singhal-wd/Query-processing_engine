#include "global.h"
/**
 * @brief
 * SYNTAX: CHECKSYMMETRY relation_name
 */

bool syntacticParseSYMMETRY()
{
    logger.log("syntacticParseSYMMETRY");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = CHECKSYMMETRY;
    parsedQuery.loadRelationName = tokenizedQuery[1];
    return true;
}

bool semanticParseSYMMETRY()
{
    logger.log("semanticParseSYMMETRY");

    if (!tableCatalogue.isTable(parsedQuery.loadRelationName))
    {
        cout << "SEMANTIC ERROR: Matrix doesn't exist" << endl;
        return false;
    }
    return true;
}

/**************CHECK SYMMETRY*********************************/

//Returns a vector, where the convention is [block_reads,block_writes,boolean result]
bool symm_one(int block_i, int block_j){
    //LOAD page_(block_i)_(block_i) into a vector of vector. Let's say I got into dummy_page_0 for now
    Table* table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    vector<vector<int>>dummy_page_0 = table->getPageData(table->tableName, block_i, block_j);

    int sz = dummy_page_0.size();
    for(int i=0 ; i < sz ; i++){
        for(int j = i+1 ; j < sz ; j++){
            if(dummy_page_0[i][j] != dummy_page_0[j][i]){
                return false;
            }
        }
    }
    return true;
    // return {block_reads,block_writes,result};
}

bool symm_two(int block_i, int block_j){
    Table* table = tableCatalogue.getTable(parsedQuery.loadRelationName);

    //LOAD page_(block_i)_(block_j) into a vector of vector. Let's say I got into dummy_page_0 for now
    vector<vector<int>>dummy_page_0 = table->getPageData(table->tableName, block_i, block_j);

    int sz_n = dummy_page_0.size();
    int sz_m = dummy_page_0[0].size();
    
    //LOAD page_(block_j)_(block_i) into a vector of vector. Let's say I got into dummy_page_1 for now
    vector<vector<int>>dummy_page_1 = table->getPageData(table->tableName, block_j, block_i);

    for(int j = 0 ; j < sz_m ; j++){
        for(int i=0 ; i < sz_n ; i++){
            if(dummy_page_0[i][j] != dummy_page_1[j][i]){
                return false;
            }
        }
    }
    return true;
    // return {block_reads,block_writes,result};
}

void check_symmetry(){
    
    Table* table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    int n = table->rowCount;
    int block_dim = table->maxRowsPerBlock;

    //maintain disk reads and writes
    int block_reads = 0;
    int block_writes = 0;
    int block_access = 0; //at last just do reads+writes


    int blocks = ceil( (double)(n) / (double)(block_dim) );
    for(int block_i = 0 ; block_i < blocks ; block_i++){
        for(int block_j = block_i ; block_j < blocks ; block_j++){
            if(block_i == block_j){
                bool res = symm_one(block_i,block_j);
                block_reads ++;
                if(res == false){
                    cout<<"False"<<endl;
                    block_access = block_reads + block_writes;
                    cout<<"Number of blocks read: "<<block_reads<<endl;
                    cout<<"Number of blocks written: "<<block_writes<<endl;
                    cout<<"Number of blocks accessed: "<<block_access<<endl; 
                    return;               
                }
            }
            else{
                bool res = symm_two(block_i,block_j);
                block_reads += 2;
                if(res == false){
                    cout<<"False"<<endl;
                    block_access = block_reads + block_writes;
                    cout<<"Number of blocks read: "<<block_reads<<endl;
                    cout<<"Number of blocks written: "<<block_writes<<endl;
                    cout<<"Number of blocks accessed: "<<block_access<<endl;
                    return;                
                }
            }
        }
    }

    cout<<"True"<<endl;
    block_access = block_reads + block_writes;
    cout<<"Number of blocks read: "<<block_reads<<endl;
    cout<<"Number of blocks written: "<<block_writes<<endl;
    cout<<"Number of blocks accessed: "<<block_access<<endl;
    return;
}



void executeSYMMETRY()
{
    logger.log("executeSYMMETRY");
    // Table* table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    // vector<vector<int>>vec = table->getPageData(table->tableName, 0, 0);
    check_symmetry();
    return;
}