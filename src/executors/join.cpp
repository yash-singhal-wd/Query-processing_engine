#include "global.h"
/**
 * @brief 
 * SYNTAX: R <- JOIN relation_name1, relation_name2 ON column_name1 bin_op column_name2
 */
bool syntacticParseJOIN()
{
    logger.log("syntacticParseJOIN");
    if (tokenizedQuery.size() != 9 || tokenizedQuery[5] != "ON")
    {
        cout << "SYNTAC ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = JOIN;
    parsedQuery.joinResultRelationName = tokenizedQuery[0];
    parsedQuery.joinFirstRelationName = tokenizedQuery[3];
    parsedQuery.joinSecondRelationName = tokenizedQuery[4];
    parsedQuery.joinFirstColumnName = tokenizedQuery[6];
    parsedQuery.joinSecondColumnName = tokenizedQuery[8];

    string binaryOperator = tokenizedQuery[7];
    if (binaryOperator == "<")
        parsedQuery.joinBinaryOperator = LESS_THAN;
    else if (binaryOperator == ">")
        parsedQuery.joinBinaryOperator = GREATER_THAN;
    else if (binaryOperator == ">=" || binaryOperator == "=>")
        parsedQuery.joinBinaryOperator = GEQ;
    else if (binaryOperator == "<=" || binaryOperator == "=<")
        parsedQuery.joinBinaryOperator = LEQ;
    else if (binaryOperator == "==")
        parsedQuery.joinBinaryOperator = EQUAL;
    else if (binaryOperator == "!=")
        parsedQuery.joinBinaryOperator = NOT_EQUAL;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseJOIN()
{
    logger.log("semanticParseJOIN");

    if (tableCatalogue.isTable(parsedQuery.joinResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.joinFirstRelationName) || !tableCatalogue.isTable(parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.joinFirstColumnName, parsedQuery.joinFirstRelationName) || !tableCatalogue.isColumnFromTable(parsedQuery.joinSecondColumnName, parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    return true;
}

void write_back_waveRun_join(int waveNumber, int runNumber, int pageNumber, vector<vector<int>> &vec, Table *table)
{
    // Table *table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    string wb = "_W" + to_string(waveNumber) + "_" + "R" + to_string(runNumber) + "_" + to_string(pageNumber);
    // bufferManager.writePage(table->tableName, wb, vec, vec.size());
    bufferManager.writePage1(table->tableName, wb, vec, vec.size());
}

void final_wave_writeback_join(string folder, int pageNumber, vector<vector<int>> &vec, Table *table, string tablename)
{
    // Table *table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    string wb = to_string(pageNumber);
    // bufferManager.writePage(table->tableName, wb, vec, vec.size());
    bufferManager.writePage_customFolder(folder,tablename, wb, vec, vec.size());
}

bool customComparator_join(vector<int> &a, vector<int> &b, int coli, string type) {
    if(type=="ASC"){
        if(a[coli]<b[coli]) return true;
    } else {
        if(a[coli]>b[coli]) return true;
    } 
    return false;
}

struct WavecustomComparator_join {
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

void sort_each_block_join(int col_index, string sort_type, Table *table){
    
    int block_count = table->blockCount;
    
    //sorting start
    for(int i=0;i<block_count; ++i){
        vector<vector<int>> sort_table = table->getPageData(table->tableName, i);
        
        // cout<<"vector loaded"<<endl;

        stable_sort(sort_table.begin(), sort_table.end(), [col_index, sort_type](vector<int> a, vector<int> b) {
                return customComparator_join(a, b, col_index, sort_type);
        });
        // cout<<"sort comparison done"<<endl;
        //write back
        write_back_waveRun_join(0, i, 0, sort_table,table); 
    }
    //all blocks sorting done 

}

void merge_join(int col_index, string sort_type, Table *table, Table *result, string tablename){

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
    
    // priority_queue< pair<int,pair<int,pair<vector<vector<int>>,pair<int,int>>>> , vector <pair<int,pair<int,pair<vector<vector<int>>,pair<int,int>>>>> , WavecustomComparator_join>pq;
    
    // pair<int,pair<int,pair<vector<vector<int>>,pair<int,pair<string,int>>>>> //{value,{idx,{block_vec,{run,{order,block_seq}}}}}}

    priority_queue< pair<int,pair<int,pair<vector<vector<int>>,pair<int,pair<string,int>>>>> , 
    vector < pair<int,pair<int,pair<vector<vector<int>>,pair<int,pair<string,int>>>>> > , 
    WavecustomComparator_join >pq;
    
    if(total_w == 0){
        vector<vector<int>>buff_wb = table->getPageData(table->tableName, 0, 0, 0);
        final_wave_writeback_join("temp_join",0,buff_wb,result, tablename);
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
                        final_wave_writeback_join("temp_join",write_back_sequence,buff_wb,result, tablename);
                    }
                    else{
                        write_back_waveRun_join(w+1, loads, write_back_sequence, buff_wb, table);
                    }
                    buff_wb.clear();
                    write_back_sequence++;
                }
            }
            // cout<<"run completed\nRemaining buffer elements : "<<buff_wb.size()<<endl;
            //If the buffer is incomplete, write it back
            if(buff_wb.size() != 0){
                if(w == total_w-1){
                    final_wave_writeback_join("temp_join",write_back_sequence,buff_wb,result,tablename);
                }
                else{
                    write_back_waveRun_join(w+1, loads, write_back_sequence, buff_wb, table);
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

bool compare(int a, int b){
    // cout<<parsedQuery.joinBinaryOperator<<endl;
    if(parsedQuery.joinBinaryOperator == LESS_THAN || parsedQuery.joinBinaryOperator == GREATER_THAN){
        // cout<<"a : "<<a<<" b : "<<b<<endl;
        if(a > b){
            return true;
        }
        return false;
    }
    // else if(parsedQuery.joinBinaryOperator == GREATER_THAN){
    //     if(a > b){
    //         return true;
    //     }
    //     return false;
    // }
    else if(parsedQuery.joinBinaryOperator == LEQ || parsedQuery.joinBinaryOperator == GEQ){
        if(a >= b){
            return true;
        }
        return false;
    }
    // else if(parsedQuery.joinBinaryOperator == GEQ){
    //     if(a >= b){
    //         return true;
    //     }
    //     return false;
    // }
}

// void join_equal()

void join_main(Table *table_1, Table *table_2, Table *result, int col_1, int col_2, int rows_1, int rows_2){

    //logic is we will keep one table as anchor, and keep iterating the whole second table again and again
    //for all the values of the first table
    vector<vector<int>>vec_1;
    vector<vector<int>>vec_2;

    int anchor = 0;
    // int last_match = 0;
    int i=0,j=0;
    int it=0,jt=0;
    int pg_i=0, pg_j=0;

    vec_1 = table_1->getPageDataJoin(table_1->tableName, pg_i);
    logger.log(to_string(vec_1.size()));
    logger.log(to_string(rows_1));
    vec_2 = table_2->getPageDataJoin("temp_table_2", pg_j);
    while(i<rows_1){
        if(it == vec_1.size()){
            pg_i++;
            it = 0;
            vec_1 = table_1->getPageDataJoin(table_1->tableName, pg_i);
        }
        anchor = vec_1[it][col_1];
        // cout<<"now anchor is : "<<anchor<<endl;
        jt=0;
        j=0;
        vec_2 = table_2->getPageDataJoin(table_2->tableName, 0);
        // cout<<"init size : "<<vec_2.size()<<endl;
        // cout<<"rows : "<<rows_2<<endl;
        while(j<rows_2){
            if(jt == vec_2.size()){
                // cout<<"block over\n";
                pg_j++;
                vec_2 = table_2->getPageDataJoin(table_2->tableName, pg_j);
                // cout<<"new size : "<<vec_2.size();
                jt=0;
                if(vec_2.size() == 0){
                    break;
                }
            }
            // cout<<"j : "<<j<<" jt : "<<jt<<endl;
            int cmp_val = vec_2[jt][col_2];
            // cout<<anchor<<" > "<<cmp_val<<endl;
            if(compare(anchor,cmp_val)){
                // cout<<"yes"<<endl;
                vector<int>wb;
                if(parsedQuery.joinBinaryOperator == GREATER_THAN || parsedQuery.joinBinaryOperator == GEQ){
                    wb = vec_1[it];
                    wb.insert(wb.end(),vec_2[jt].begin(),vec_2[jt].end());
                }
                else{
                    wb = vec_2[jt];
                    wb.insert(wb.end(),vec_1[it].begin(),vec_1[it].end());                
                }
                result -> writeRow<int>(wb);
            }
            else{
                break;
            }
            j++;
            jt++;
        }   
        
        it++; 
        i++;
    }
}

void join_equal(Table *table_1, Table *table_2, Table *result, int col_1, int col_2, int rows_1, int rows_2){

    //logic is we will keep one table as anchor, and keep iterating the whole second table again and again
    //for all the values of the first table
    vector<vector<int>>vec_1;
    vector<vector<int>>vec_2;

    int anchor = 0;
    int last_match_j = 0;
    int last_match_jt = 0;
    int last_match_page = 0;
    int i=0,j=0;
    int it=0,jt=0;
    int pg_i=0, pg_j=0;

    vec_1 = table_1->getPageDataJoin("temp_table_1", pg_i);
    // vec_2 = table_2->getPageDataJoin("temp_table_2", pg_j);
    while(i<rows_1){
        if(it == vec_1.size()){
            pg_i++;
            it = 0;
            vec_1 = table_1->getPageDataJoin(table_1->tableName, pg_i);
        }
        if(vec_1[it][col_1] != anchor){
            jt=jt;
            j=j;
            last_match_page = pg_j;
            last_match_j = j;
            last_match_jt = jt;
        }
        else{
            jt=last_match_jt;
            j=last_match_j;
        }
        anchor = vec_1[it][col_1];
        // cout<<"now anchor is : "<<anchor<<endl;
        
        vec_2 = table_2->getPageDataJoin(table_2->tableName, last_match_page);
        // cout<<"init size : "<<vec_2.size()<<endl;
        // cout<<"rows : "<<rows_2<<endl;
        // bool matching = 0;
        while(j<rows_2){
            if(jt == vec_2.size()){
                // cout<<"block over\n";
                pg_j++;
                vec_2 = table_2->getPageDataJoin(table_2->tableName, pg_j);
                // cout<<"new size : "<<vec_2.size();
                jt=0;
                if(vec_2.size() == 0){
                    break;
                }
            }
            // cout<<"j : "<<j<<" jt : "<<jt<<endl;
            int cmp_val = vec_2[jt][col_2];
            // cout<<anchor<<" > "<<cmp_val<<endl;
            if(anchor == cmp_val){
                // matching = 1;
                // cout<<"yes"<<endl;
                vector<int>wb;
                wb = vec_1[it];
                wb.insert(wb.end(),vec_2[jt].begin(),vec_2[jt].end());                
                result -> writeRow<int>(wb);
            }
            else{
                if(anchor < cmp_val){
                    break;
                }
                jt++;
                j++;
                continue;
            }
            j++;
            jt++;
        }   
        it++; 
        i++;
    }
}

void executeJOIN()
{
    logger.log("executeJOIN");

    Table *table_1 = tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
    Table *table_2 = tableCatalogue.getTable(parsedQuery.joinSecondRelationName);

    vector<string>cols = table_1->columns;
    cols.insert(cols.end(), table_2->columns.begin(), table_2->columns.end());
    Table *res_table = new Table(parsedQuery.joinResultRelationName,cols);
    // Table *res_table = new Table(parsedQuery.GROUPBYResultRelationName,vec);
    
    Table *temp_table_1 = new Table("temp_table_1");
    Table *temp_table_2 = new Table("temp_table_2");
    // string table_1_name = parsedQuery.joinFirstRelationName;
    // string table_2_name = parsedQuery.joinSecondRelationName;

    string col_name_1 = parsedQuery.joinFirstColumnName;
    string col_name_2 = parsedQuery.joinSecondColumnName;

    int col_idx_1 = table_1->getColumnIndex(col_name_1);
    int col_idx_2 = table_2->getColumnIndex(col_name_2);

    string order = "ASC";

    //sorting the first table on its corresponding column
    system("mkdir ../data/temp/temp_join");

    system(("mkdir ../data/temp/" + parsedQuery.joinFirstRelationName).c_str());
    sort_each_block_join(col_idx_1,order,table_1);
    merge_join(col_idx_1, order, table_1,temp_table_1,"temp_table_1");
    //delete temp folder
    system(("rm -r ../data/temp/" + parsedQuery.joinFirstRelationName).c_str());

    //sorting the second table on its corresponding column
    system(("mkdir ../data/temp/" + parsedQuery.joinSecondRelationName).c_str());
    sort_each_block_join(col_idx_2,order,table_2);
    merge_join(col_idx_2, order, table_2,temp_table_2,"temp_table_2");
    //delete temp folder
    system(("rm -r ../data/temp/" + parsedQuery.joinSecondRelationName).c_str());

    int rows_1 = table_1->rowCount;
    int rows_2 = table_2->rowCount;

    if(parsedQuery.joinBinaryOperator == GREATER_THAN || parsedQuery.joinBinaryOperator == GEQ){
        join_main(temp_table_1,temp_table_2, res_table, col_idx_1, col_idx_2, rows_1, rows_2);
    }
    else if(parsedQuery.joinBinaryOperator == LESS_THAN || parsedQuery.joinBinaryOperator == LEQ){
        join_main(temp_table_2,temp_table_1, res_table, col_idx_2, col_idx_1, rows_2, rows_1);
    }
    else if(parsedQuery.joinBinaryOperator == EQUAL){
        join_equal(temp_table_1,temp_table_2, res_table, col_idx_1, col_idx_2, rows_1, rows_2);
    }

    system("rm -r ../data/temp/temp_join"); //delete the temp folder
    res_table->blockify();
    tableCatalogue.insertTable(res_table);
    
    return;
}