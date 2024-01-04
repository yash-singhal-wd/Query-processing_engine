#include "global.h"

/**
 * @brief File contains method to process SORT commands.
 *
 * syntax:
 * R <- SORT relation_name BY column_name IN sorting_order
 *
 * sorting_order = ASC | DESC
 */

bool checkForIN()
{
    int i = 3;
    int n = tokenizedQuery.size();
    while (i < n && tokenizedQuery[i] != "IN")
    {
        i++;
    }
    if (i == n)
    {

        return false;
    }
    if ((n - i - 1) != (i - 3))
    {
        return false;
    }
    if (tokenizedQuery[i] != "IN")
    {
        return false;
    }
    parsedQuery.sortQueryCount = i - 3;
    return true;
}

bool syntacticParseSORT()
{
    logger.log("syntacticParseSORT");
    if (tokenizedQuery.size() < 6)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    if (tokenizedQuery[2] != "BY")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    if (!checkForIN())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = SORT;
    // parsedQuery.sortResultRelationName = tokenizedQuery[0];
    parsedQuery.sortRelationName = tokenizedQuery[1];
    // parsedQuery.sortColumnName = tokenizedQuery[5];
    for (int ind = 3; ind < 3 + parsedQuery.sortQueryCount; ind++)
    {
        if(tokenizedQuery[ind + parsedQuery.sortQueryCount + 1] != "ASC" && tokenizedQuery[ind + parsedQuery.sortQueryCount + 1] != "DESC") 
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        pair<string, string> p = {tokenizedQuery[ind], tokenizedQuery[ind + parsedQuery.sortQueryCount + 1]};
        parsedQuery.sortColumnNames.push_back(p);
        // cout<<tokenizedQuery[ind]<<" "<< tokenizedQuery[ind + parsedQuery.sortQueryCount +1]<<endl;
    }

    // string sortingStrategy = tokenizedQuery[7];
    // if(sortingStrategy == "ASC")
    //     parsedQuery.sortingStrategy = ASC;
    // else if(sortingStrategy == "DESC")
    //     parsedQuery.sortingStrategy = DESC;
    // else{
    //     cout<<"SYNTAX ERROR"<<endl;
    //     return false;
    // }
    return true;
}

bool semanticParseSORT()
{
    logger.log("semanticParseSORT");

    // if (tableCatalogue.isTable(parsedQuery.sortResultRelationName))
    // {
    //     cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
    //     return false;
    // }

    if (!tableCatalogue.isTable(parsedQuery.sortRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    for (int ind = 3; ind < 3 + parsedQuery.sortQueryCount; ind++)
    {
        if (!tableCatalogue.isColumnFromTable(tokenizedQuery[ind], parsedQuery.sortRelationName))
        {
            cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
            return false;
        }
    }

    return true;
}

//Old Block writing function
/*
void write_back_waveRun(int waveNumber, int runNumber, int pageNumber, vector<vector<int>> &vec)
{
    cout<<"start"<<endl;
    Table *table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    string wb = "_W" + to_string(waveNumber) + "_" + "R" + to_string(runNumber) + "_" + to_string(pageNumber);
    cout<<"writing "<<wb<<endl;
    bufferManager.writePage(table->tableName, wb, vec, vec.size());
    cout<<"writing done"<<endl;
}
*/
void write_back_waveRun(int waveNumber, int runNumber, int pageNumber, vector<vector<int>> &vec, Table *table)
{
    // Table *table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    string wb = "_W" + to_string(waveNumber) + "_" + "R" + to_string(runNumber) + "_" + to_string(pageNumber);
    // bufferManager.writePage(table->tableName, wb, vec, vec.size());
    bufferManager.writePage1(table->tableName, wb, vec, vec.size());
}

void final_wave_writeback(int pageNumber, vector<vector<int>> &vec, Table *table)
{
    // Table *table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    string wb = to_string(pageNumber);
    // bufferManager.writePage(table->tableName, wb, vec, vec.size());
    bufferManager.writePage_final_wave(table->tableName, wb, vec, vec.size());
}

bool customComparator(vector<int> &a, vector<int> &b, int coli, string type) {
    if(type=="ASC"){
        if(a[coli]<b[coli]) return true;
    } else {
        if(a[coli]>b[coli]) return true;
    } 
    return false;
}


/*
struct WaveCustomComparator {
    // {value,{idx,{block_vec,{run,{order,block_seq}}}}}}
    bool operator()(pair<int,pair<int,pair<vector<vector<int>>,pair<int,int>>>> a, 
                    pair<int,pair<int,pair<vector<vector<int>>,pair<int,int>>>> b) const 
    {
        //{value,{idx,{block_vec,{run,block_seq}}}}}; 
        // Comparison logic here
        if(a.first<b.first) return true;
        else if(a.first==b.first && a.second.second.second.first>b.second.second.second.first) return true;
        else if(a.second.second.second.first==b.second.second.second.first
                && 
                a.second.second.second.second>b.second.second.second.first) return true;
        return false; 
    }
};
*/

struct WaveCustomComparator {
    // {value,{idx,{block_vec,{run,{order,block_seq}}}}}}
    bool operator()(pair<int,pair<int,pair<vector<vector<int>>,pair<int,pair<string,int>>>>> a, 
                    pair<int,pair<int,pair<vector<vector<int>>,pair<int,pair<string,int>>>>> b) const 
    {
        //{value,{idx,{block_vec,{run,block_seq}}}}}; 
        // Comparison logic here
        string order = a.second.second.second.second.first;

        if(order == "DESC"){
            if(a.first<b.first) return true;
            else if(a.first==b.first && a.second.second.second.first>b.second.second.second.first) return true;
            else false;
            // else if(a.second.second.second.first==b.second.second.second.first
            //     && 
            //     a.second.second.second.second>b.second.second.second.first) return true;
        }
        else{
            if(a.first>b.first) return true;
            else if(a.first==b.first && a.second.second.second.first>b.second.second.second.first) return true;
            else return false;
        }
        
        return false; 
    }
};

//****************** INIT INTERNAL SORT **************************
void sort_each_block(int col_index, string sort_type, Table *table){
    
    int block_count = table->blockCount;
    
    //sorting start
    for(int i=0;i<block_count; ++i){
        vector<vector<int>> sort_table = table->getPageData(table->tableName, i);
        
        // cout<<"vector loaded"<<endl;

        stable_sort(sort_table.begin(), sort_table.end(), [col_index, sort_type](vector<int> a, vector<int> b) {
                return customComparator(a, b, col_index, sort_type);
        });
        // cout<<"sort comparison done"<<endl;
        //write back
        write_back_waveRun(0, i, 0, sort_table,table); 
    }
    //all blocks sorting done 

}

//****************** Merge Algorithm *****************************

void merge(int col_index, string sort_type, Table *table){

    // cout<<"sort merge called by "<<col_index<<endl;
    // 10 hard coded buffers
    // vector<vector<int>>buff_0;
    // vector<vector<int>>buff_1;
    // vector<vector<int>>buff_2;
    // vector<vector<int>>buff_3;
    // vector<vector<int>>buff_4;
    // vector<vector<int>>buff_5;
    // vector<vector<int>>buff_6;
    // vector<vector<int>>buff_7;
    // vector<vector<int>>buff_8;

    int buffer_block_count = 9; //default 9

    vector<vector<vector<int>>>buff_list(9); //9 Buffers, I have not used this
    
    vector<vector<int>>buff_wb; // Buffer used to write back

    
    int columnCount = table->columnCount;
    int maxRows = 250/columnCount; //Hard coded for now
    
    
    int block_count = table->blockCount;
    int total_w = ceil(log(block_count)/log(9)); // Using 9 blocks to merge
    
    // cout<<"total w : "<<total_w<<endl;
    // int lf;
    int num_pages = block_count;

    //Max heap for DESC sort
    // priority_queue<pair<int,pair<int,pair<vector<vector<int>>,pair<int,int>>>>>pq; //{value,{idx,{block_vec,{run,block_seq}}}}};
    
    // priority_queue< pair<int,pair<int,pair<vector<vector<int>>,pair<int,int>>>> , vector <pair<int,pair<int,pair<vector<vector<int>>,pair<int,int>>>>> , WaveCustomComparator>pq;
    
    // pair<int,pair<int,pair<vector<vector<int>>,pair<int,pair<string,int>>>>> //{value,{idx,{block_vec,{run,{order,block_seq}}}}}}

    priority_queue< pair<int,pair<int,pair<vector<vector<int>>,pair<int,pair<string,int>>>>> , 
    vector < pair<int,pair<int,pair<vector<vector<int>>,pair<int,pair<string,int>>>>> > , 
    WaveCustomComparator >pq;
    
    if(total_w == 0){
        vector<vector<int>>buff_wb = table->getPageData(table->tableName, 0, 0, 0);
        final_wave_writeback(0,buff_wb,table);
        return;
    }

    for(int w = 0 ; w < total_w ; w++){
        int load_total = ceil((double)num_pages/9.0);
        num_pages = load_total;

        // cout<<"First loop num pages = "<<num_pages<<endl;
        
        int block_fetch = 0;
        //here the loads is referring to the current run i.e in this loop, 1 sorted file is the output
        for(int loads=0; loads < load_total ; loads++){

            int write_back_sequence = 0; // in order to maintain the sequence of this specific run when writing back

            //loading the vectors and pushing in the priority queue
            for(int i = 0 ; i < 9 ; i++){
                // block_fetch += i;
                // buff_list[i] = table->getPageData(table->tableName, w, block_fetch, 0);
                vector<vector<int>>buffer_vec = table->getPageData(table->tableName, w, block_fetch, 0);
                // cout<<"fetched block_"<<block_fetch<<endl;
                // cout<<"rows : "<<buffer_vec.size()<<endl;
                if(buffer_vec.size() == 0){
                    break;
                }
                // pq.push({buffer_vec[0][col_index],{0,{buffer_vec,{block_fetch,0}}}});
                pq.push({buffer_vec[0][col_index],{0,{buffer_vec,{block_fetch,{sort_type,0}}}}});
                block_fetch++;
            }
            
            //Now my Priority queue has the top element of all the loaded vectors
            //I will fetch the top which is the min, then push this vector in the write back vector ; case wb vector full
            // increment the index to the buffer vector ; case: buffer vector gets over
            //push the new vector in the pq
            
            // cout<<"loaded all vectors. entering pq"<<endl;
            
            while(!pq.empty()){
                // {value,{idx,{block_vec,{run,{order,block_seq}}}}}}
                int top_idx = pq.top().second.first;
                vector<vector<int>>top_vec = pq.top().second.second.first;
                // int top_block_idx = pq.top().second.second.second.second;
                int top_block_idx = pq.top().second.second.second.second.second;
                int run = pq.top().second.second.second.first;
                // cout<<"pq element : "<<pq.top().first<<endl;
                pq.pop();
            
                //put in write back buffer vector
                buff_wb.push_back(top_vec[top_idx]);
                
                top_idx++;

                // cout<<"element is popped. i = "<<top_idx<<" vec size() = "<<top_vec.size()<<endl;
                //if the buffer becomes emptied, we need to load its corresponding next block in sequence, and if it
                //does not exist, not put anything in the pq
                if(top_idx == top_vec.size()){
                    //load the corresponding next block into this buffer
                    // cout<<"Block is over, loading new one\n";
                    // cout<<"loading page w: "<<w<<" r: "<<run<<" :"<<top_block_idx+1<<endl;
                    // top_vec = table->getPageData(table->tableName, w, block_fetch, top_block_idx+1);
                    top_vec = table->getPageData(table->tableName, w, run, top_block_idx+1);

                    if(top_vec.size() != 0){
                        // pq.push({top_vec[0][col_index],{0,{top_vec,{run,top_block_idx+1}}}});
                        pq.push({top_vec[0][col_index],{0,{top_vec,{run,{sort_type,top_block_idx+1}}}}});
                    }
                    else{
                        // cout<<"No page for this run left\n";
                    }
                }
                else{
                    pq.push({top_vec[top_idx][col_index],{top_idx,{top_vec,{run,{sort_type,top_block_idx}}}}});
                }

                //if the write back vector is full, write back this vector, then flush it
                if(buff_wb.size() == maxRows){
                    // cout<<"writing back new wb page wirth the name\n";
                    // cout<<"w: "<<w+1<<" r: "<<loads<<" :"<<write_back_sequence<<endl;
                    if(w == total_w-1){
                        final_wave_writeback(write_back_sequence,buff_wb,table);
                    }
                    else{
                        write_back_waveRun(w+1, loads, write_back_sequence, buff_wb, table);
                    }
                    buff_wb.clear();
                    write_back_sequence++;
                }
            }
            // cout<<"run completed\nRemaining buffer elements : "<<buff_wb.size()<<endl;
            //If the buffer is incomplete, write it back
            if(buff_wb.size() != 0){
                if(w == total_w-1){
                    final_wave_writeback(write_back_sequence,buff_wb,table);
                }
                else{
                    write_back_waveRun(w+1, loads, write_back_sequence, buff_wb, table);
                }
                buff_wb.clear();
            }
            //run completed
        }
        //wave completed

        //Another task is to rename all the pages with the names wx_ry_z to the original page name

    }
    // cout<<"merge done\n";
    return;
    
}

/*
void merge_asc(int col_index, string sort_type, Table *table){

    // 10 hard coded buffers
    // vector<vector<int>>buff_0;
    // vector<vector<int>>buff_1;
    // vector<vector<int>>buff_2;
    // vector<vector<int>>buff_3;
    // vector<vector<int>>buff_4;
    // vector<vector<int>>buff_5;
    // vector<vector<int>>buff_6;
    // vector<vector<int>>buff_7;
    // vector<vector<int>>buff_8;

    int buffer_block_count = 9; //default 9

    vector<vector<vector<int>>>buff_list(9); //9 Buffers, I have not used this
    
    vector<vector<int>>buff_wb; // Buffer used to write back

    
    int columnCount = table->columnCount;
    int maxRows = 250/columnCount; //Hard coded for now
    
    
    int block_count = table->blockCount;
    int total_w = ceil(log(block_count)/log(9)); // Using 9 blocks to merge
    
    // cout<<"total w : "<<total_w<<endl;
    // int lf;
    int num_pages = block_count;

    //Min heap for ASC sort
    priority_queue< pair<int,pair<int,pair<vector<vector<int>>,pair<int,int>>>> , vector<pair<int,pair<int,pair<vector<vector<int>>,pair<int,int>>>> , greater< pair<int,pair<int,pair<vector<vector<int>>,pair<int,int>>>> >>>pq; //{value,{idx,{block_vec,{run,block_seq}}}}};
    
    // cout<<"Initial num_pages = "<<num_pages<<endl;

    for(int w = 0 ; w < total_w ; w++){
        int load_total = ceil((double)num_pages/9.0);
        num_pages = load_total;

        // cout<<"First loop num pages = "<<num_pages<<endl;
        
        int block_fetch = 0;
        //here the loads is referring to the current run i.e in this loop, 1 sorted file is the output
        for(int loads=0; loads < load_total ; loads++){

            int write_back_sequence = 0; // in order to maintain the sequence of this specific run when writing back

            //loading the vectors and pushing in the priority queue
            for(int i = 0 ; i < 9 ; i++){
                // block_fetch += i;
                // buff_list[i] = table->getPageData(table->tableName, w, block_fetch, 0);
                vector<vector<int>>buffer_vec = table->getPageData(table->tableName, w, block_fetch, 0);
                // cout<<"fetched block_"<<block_fetch<<endl;
                // cout<<"rows : "<<buffer_vec.size()<<endl;
                if(buffer_vec.size() == 0){
                    break;
                }
                pq.push({buffer_vec[0][col_index],{0,{buffer_vec,{block_fetch,0}}}});
                block_fetch++;
            }
            
            //Now my Priority queue has the top element of all the loaded vectors
            //I will fetch the top which is the min, then push this vector in the write back vector ; case wb vector full
            // increment the index to the buffer vector ; case: buffer vector gets over
            //push the new vector in the pq
            
            // cout<<"loaded all vectors. entering pq"<<endl;
            
            while(!pq.empty()){
                int top_idx = pq.top().second.first;
                vector<vector<int>>top_vec = pq.top().second.second.first;
                int top_block_idx = pq.top().second.second.second.second;
                int run = pq.top().second.second.second.first;
                // cout<<"pq element : "<<pq.top().first<<endl;
                pq.pop();
            
                //put in write back buffer vector
                buff_wb.push_back(top_vec[top_idx]);
                
                top_idx++;

                // cout<<"element is popped. i = "<<top_idx<<" vec size() = "<<top_vec.size()<<endl;
                //if the buffer becomes emptied, we need to load its corresponding next block in sequence, and if it
                //does not exist, not put anything in the pq
                if(top_idx == top_vec.size()){
                    //load the corresponding next block into this buffer
                    // cout<<"Block is over, loading new one\n";
                    // cout<<"loading page w: "<<w<<" r: "<<run<<" :"<<top_block_idx+1<<endl;
                    // top_vec = table->getPageData(table->tableName, w, block_fetch, top_block_idx+1);
                    top_vec = table->getPageData(table->tableName, w, run, top_block_idx+1);

                    if(top_vec.size() != 0){
                        pq.push({top_vec[0][col_index],{0,{top_vec,{run,top_block_idx+1}}}});
                    }
                    else{
                        // cout<<"No page for this run left\n";
                    }
                }
                else{
                    pq.push({top_vec[top_idx][col_index],{top_idx,{top_vec,{run,top_block_idx}}}});
                }

                //if the write back vector is full, write back this vector, then flush it
                if(buff_wb.size() == maxRows){
                    // cout<<"writing back new wb page wirth the name\n";
                    // cout<<"w: "<<w+1<<" r: "<<loads<<" :"<<write_back_sequence<<endl;
                    write_back_waveRun(w+1, loads, write_back_sequence, buff_wb, table);
                    buff_wb.clear();
                    write_back_sequence++;
                }
            }
            // cout<<"run completed\nRemaining buffer elements : "<<buff_wb.size()<<endl;
            //If the buffer is incomplete, write it back
            if(buff_wb.size() != 0){
                write_back_waveRun(w+1, loads, write_back_sequence, buff_wb, table);
                buff_wb.clear();
            }
            //run completed
        }
        //wave completed

        //Another task is to rename all the pages with the names wx_ry_z to the original page name

    }
    // cout<<"merge done\n";
    return;
    
}
*/ 

void executeSORT()
{

    logger.log("executeSORT");
    Table *table = tableCatalogue.getTable(parsedQuery.sortRelationName);
    // system(("mkdir ../data/temp/" + parsedQuery.sortRelationName).c_str());

    // All the columns on which we need to sort and in which order are stored in vector<pair<string,string>> "parsedQuery.sortColumnNames"
    // For getting column index use syntax : "int index = table->getColumnIndex(<columnName>);"
    // remove below comments Anurag its just for your reffernce for merging phase
    // write_back_waveRun() function is used to write your merged blocks you have to pass waveNumber, runNumber and Block Number.
    // table->getPageData(table->tableName, waveNumber, runNumber, blockNumber) for getting page data;

    //for singhal for srting phase
    // you can get table data using table->getPageData(table->tableName, blockNumber)in vec of vec.



    //ANURAG ACTIVITY//
    vector<pair<string,string>>query = parsedQuery.sortColumnNames; //cols to be sorted in order of priority ; format {col_name,ASC/DESCV}
    parsedQuery.sortColumnNames.clear();
    //we will sort on all these cols in reverse order
    for(int i = query.size()-1 ; i >= 0; i-- ){
        system(("mkdir ../data/temp/" + parsedQuery.sortRelationName).c_str());
        string col_name = query[i].first;
        string order = query[i].second;

        int col_idx = table->getColumnIndex(col_name); //get col index using it's name

        // cout<<"worked till here\n";
        //################# - sorting each block - ###################

        //Initial internal sort. This will create the initial pages, individually sorted, for the merge phase
        sort_each_block(col_idx,order,table);
        // cout<<"sort done\n";

        //Merge
       merge(col_idx, order, table);
       //delete temp folder
    system(("rm -r ../data/temp/" + parsedQuery.sortRelationName).c_str());

    }

    //HAVE TO RENAME ALL THE TEMP FILES TO BE THE ORIGINAL FILE NAME
    return;
}


// void executeSORT(){
//     Table *table = tableCatalogue.getTable(parsedQuery.sortRelationName);
//     vector<vector<int>> vec ={{0}};
    
//     system(("mkdir ../data/temp/" + parsedQuery.sortRelationName).c_str());
//     write_back_waveRun(0,0,0,vec,table);
// }