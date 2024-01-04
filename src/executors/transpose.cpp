#include "global.h"
/**
 * @brief
 * SYNTAX: TRANSPOSE MATRIX relation_name
 */

bool syntacticParseTRANSPOSEMATRIX()
{
    logger.log("syntacticParseTRANSPOSEMATRIX");
    if (tokenizedQuery.size() != 3)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = TRANSPOSE_MATRIX;
    parsedQuery.loadRelationName = tokenizedQuery[2];
    return true;
}

bool semanticParseTRANSPOSEMATRIX()
{
    logger.log("semanticParseTRANSPOSEMATRIX");

    if (!tableCatalogue.isTable(parsedQuery.loadRelationName))
    {
        cout << "SEMANTIC ERROR: Matrix doesn't exist" << endl;
        return false;
    }
    return true;
}

void write_back(int i, int j, vector<vector<int>>&vec){
    Table* table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    string wb = to_string(i) + "_" + to_string(j);  
    bufferManager.writePage(table->tableName,wb,vec,vec.size());
}
/**************TRANSPOSE*********************************/
void swap_one(int block_i, int block_j){

    //LOAD page_(block_i)_(block_i) into a vector of vector. Let's say I got into dummy_page_0 for now
    Table* table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    vector<vector<int>>dummy_page_0 = table->getPageData(table->tableName, block_i, block_j);

    int sz = dummy_page_0.size();

    for(int i=0 ; i < sz ; i++){
        for(int j = i+1 ; j < sz ; j++){
            int temp = dummy_page_0[i][j];
            dummy_page_0[i][j] = dummy_page_0[j][i];
            dummy_page_0[j][i] = temp;
        }
    }

    //write back to disk
    write_back(block_i,block_j,dummy_page_0);
    return;
    // return {block_reads,block_writes};
}

//function to in-place tranpose two blocks (i!=j)
void swap_two(int block_i, int block_j){

    Table* table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    //LOAD page_(block_i)_(block_j) into a vector of vector. Let's say I got into dummy_page_0 for now
    
    vector<vector<int>>dummy_page_0 = table->getPageData(table->tableName, block_i, block_j);
    int sz_n = dummy_page_0.size();
    int sz_m = dummy_page_0[0].size();
   
    //LOAD page_(block_j)_(block_i) into a vector of vector. Let's say I got into dummy_page_1 for now
    vector<vector<int>>dummy_page_1 = table->getPageData(table->tableName, block_j, block_i);
    
    for(int j = 0 ; j < sz_m ; j++){
        for(int i=0 ; i < sz_n ; i++){
            int temp = dummy_page_0[i][j];
            dummy_page_0[i][j] = dummy_page_1[j][i];
            dummy_page_1[j][i] = temp;
        }
    }

    //write back to disk
    write_back(block_i,block_j,dummy_page_0);
    write_back(block_j,block_i,dummy_page_1);
    return;
}

void transpose(){
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
                // pair<int,int>disk_info = swap_one(block_i,block_j);
                swap_one(block_i,block_j);
                block_reads ++;
                block_writes ++;
            }
            else{
                // pair<int,int>disk_info = swap_two(block_i,block_j);
                swap_two(block_i,block_j);
                block_reads += 2;
                block_writes += 2;
            }
        }
    }

    
    block_access = block_reads + block_writes;
    cout<<"Number of blocks read: "<<block_reads<<endl;
    cout<<"Number of blocks written: "<<block_writes<<endl;
    cout<<"Number of blocks accessed: "<<block_access<<endl;
}

void executeTRANSPOSEMATRIX()
{
    logger.log("executeTRANSPOSEMATRIX");
    // Table* table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    // vector<vector<int>>vec = table->getPageData(table->tableName, 0, 0);
    transpose();
    return;
}