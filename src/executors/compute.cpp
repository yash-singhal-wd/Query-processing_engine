#include "global.h"
/**
 * @brief
 * SYNTAX: COMPUTE relation_name
 */

bool syntacticParseCOMPUTE()
{
    logger.log("syntacticParseCOMPUTE");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = COMPUTE;
    parsedQuery.loadRelationName = tokenizedQuery[1];
    return true;
}

bool semanticParseCOMPUTE()
{
    logger.log("semanticParseCOMPUTE");

    if (!tableCatalogue.isTable(parsedQuery.loadRelationName))
    {
        cout << "SEMANTIC ERROR: Matrix doesn't exist" << endl;
        return false;
    }
    return true;
}

void compute_write_back(int i, int j, vector<vector<int>>&vec){
    Table* table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    string wb = to_string(i) + "_" + to_string(j);
    string TN = table->tableName + "_RESULT";

    bufferManager.writePage(TN,wb,vec,vec.size());
}
/**************COMPUTE*********************************/

void comp_one(int block_i, int block_j){
    
    //LOAD page_(block_i)_(block_i) into a vector of vector. Let's say I got into dummy_page_0 for now
    Table* table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    vector<vector<int>>dummy_page_0 = table->getPageData(table->tableName, block_i, block_j);

    int sz = dummy_page_0.size();
    
    //Creating new vector because we need to write in the new result and not tamper with the given page
    vector<vector<int>>new_page(sz,vector<int>(sz,0));

    for(int i=0 ; i < sz ; i++){
        for(int j = i+1 ; j < sz ; j++){
            new_page[i][j] = dummy_page_0[i][j] - dummy_page_0[j][i];
            new_page[j][i] = dummy_page_0[j][i] - dummy_page_0[i][j];    
        }
    }

    //Create page for new_page_0
    compute_write_back(block_i, block_j,new_page);

    return;
    // return {block_reads,block_writes};
}

//function to in-place tranpose two blocks (i!=j)
//returns a pair of blocks accessed and block written to
//in our case it is returning <2,2> for now
// pair<int,int> swap_two(int block_i, int block_j){
void comp_two(int block_i, int block_j){
    Table* table = tableCatalogue.getTable(parsedQuery.loadRelationName);

    //LOAD page_(block_i)_(block_j) into a vector of vector. Let's say I got into dummy_page_0 for now
    vector<vector<int>>dummy_page_0 = table->getPageData(table->tableName, block_i, block_j);
    int sz_n = dummy_page_0.size();
    int sz_m = dummy_page_0[0].size();
    
    //LOAD page_(block_j)_(block_i) into a vector of vector. Let's say I got into dummy_page_1 for now
    vector<vector<int>>dummy_page_1 = table->getPageData(table->tableName, block_j, block_i);
    //creating new vectors for both the pages as we need to create a new result and not tamper with the original
    vector<vector<int>>new_page_0(sz_n,vector<int>(sz_m,0));
    vector<vector<int>>new_page_1(sz_m,vector<int>(sz_n,0));

    for(int j = 0 ; j < sz_m ; j++){
        for(int i=0 ; i < sz_n ; i++){
            // cout<<i<<" "<<j<<endl;
            // cout<<new_page_0[i][j]<<endl;
            // cout<<new_page_1[j][i]<<endl;
            // cout<<dummy_page_0[i][j]<<endl;
            // cout<<dummy_page_1[j][i]<<endl;
            new_page_0[i][j] = dummy_page_0[i][j] - dummy_page_1[j][i];
            new_page_1[j][i] = dummy_page_1[j][i] - dummy_page_0[i][j];
        }
    }

    //Create new pages for both the matrices with corresponsing naming convention
    compute_write_back(block_i, block_j,new_page_0);
    compute_write_back(block_j, block_i,new_page_1);
    return;
    // return {block_reads,block_writes};
}

//have to store the result in A_RESULT
void compute(){
    Table* table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    
    int n = table->rowCount;
    int block_dim = table->maxRowsPerBlock;

    //Making Result table object
    string TN = table->tableName + "_RESULT";
    Table *result_table = new Table(TN);
    result_table->rowCount = n;
    result_table->columnCount = n;
    result_table->maxRowsPerBlock = block_dim;
    result_table->blockCount = table->blockCount;
    result_table->rowsPerBlockCount = table->rowsPerBlockCount;
    
    //Insertion in table catalogue
    tableCatalogue.insertTable(result_table);

    //maintain disk reads and writes
    int block_reads = 0;
    int block_writes = 0;
    int block_access = 0; //at last just do reads+writes


    int blocks = ceil( (double)(n) / (double)(block_dim) );
    for(int block_i = 0 ; block_i < blocks ; block_i++){
        for(int block_j = block_i ; block_j < blocks ; block_j++){
            if(block_i == block_j){
                // pair<int,int>disk_info = swap_one(block_i,block_j);
                comp_one(block_i,block_j);
                block_reads ++;
                block_writes ++;
            }
            else{
                // pair<int,int>disk_info = swap_two(block_i,block_j);
                comp_two(block_i,block_j);
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


void executeCOMPUTE()
{
    logger.log("executeCOMPUTE");
    // Table* table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    // vector<vector<int>>vec = table->getPageData(table->tableName, 0, 0);
    compute();
    return;
}