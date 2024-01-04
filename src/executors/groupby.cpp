#include "global.h"
/**
 * @brief
 * SYNTAX: <new_table> <- GROUP BY <grouping_attribute> FROM <table_name> HAVING
            <aggregate(attribute)> <bin_op> <attribute_value> RETURN
            <aggregate_func(attribute)>
 */
bool syntacticParseGROUPBY()
{
    logger.log("syntacticParseJOIN");
    if (tokenizedQuery.size() != 13 || tokenizedQuery[2] != "GROUP" || tokenizedQuery[3] != "BY" || tokenizedQuery[5] != "FROM" || tokenizedQuery[7] != "HAVING" || tokenizedQuery[11] != "RETURN")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = GROUPBY;
    parsedQuery.GROUPBYResultRelationName = tokenizedQuery[0];
    parsedQuery.GROUPBYRelationName = tokenizedQuery[6];
    parsedQuery.GROUPBYGroupingAttribute = tokenizedQuery[4];
    parsedQuery.GROUPBYAggregateAttribute = tokenizedQuery[8];
    parsedQuery.GROUPBYAggregateFuncAttribute = tokenizedQuery[12];
    parsedQuery.GROUPBYAggregateValue = tokenizedQuery[10];

    string binaryOperator = tokenizedQuery[9];
    if (binaryOperator == "<")
        parsedQuery.GROUPBYBinaryOperator = LESS_THAN;
    else if (binaryOperator == ">")
        parsedQuery.GROUPBYBinaryOperator = GREATER_THAN;
    else if (binaryOperator == ">=" || binaryOperator == "=>")
        parsedQuery.GROUPBYBinaryOperator = GEQ;
    else if (binaryOperator == "<=" || binaryOperator == "=<")
        parsedQuery.GROUPBYBinaryOperator = LEQ;
    else if (binaryOperator == "==")
        parsedQuery.GROUPBYBinaryOperator = EQUAL;
    else if (binaryOperator == "!=")
        parsedQuery.GROUPBYBinaryOperator = NOT_EQUAL;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseGROUPBY()
{
    logger.log("semanticParseJOIN");

    if (tableCatalogue.isTable(parsedQuery.GROUPBYResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.GROUPBYRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.GROUPBYGroupingAttribute, parsedQuery.GROUPBYRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    return true;
}

void write_back_waveRun_gb(int waveNumber, int runNumber, int pageNumber, vector<vector<int>> &vec, Table *table)
{
    // Table *table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    string wb = "_W" + to_string(waveNumber) + "_" + "R" + to_string(runNumber) + "_" + to_string(pageNumber);
    // bufferManager.writePage(table->tableName, wb, vec, vec.size());
    bufferManager.writePage1(table->tableName, wb, vec, vec.size());
}

void final_wave_writeback_gb(string folder, int pageNumber, vector<vector<int>> &vec, Table *table, string tablename)
{
    // Table *table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    string wb = to_string(pageNumber);
    // bufferManager.writePage(table->tableName, wb, vec, vec.size());
    bufferManager.writePage_customFolder(folder,tablename, wb, vec, vec.size());
}

void final_writeback_gb(int pageNumber, vector<vector<int>> &vec, Table *table, string tablename)
{
    // Table *table = tableCatalogue.getTable(parsedQuery.loadRelationName);
    string wb = to_string(pageNumber);
    // bufferManager.writePage(table->tableName, wb, vec, vec.size());
    bufferManager.writePage_final_wave(tablename, wb, vec, vec.size());
}

bool customComparator_gb(vector<int> &a, vector<int> &b, int coli, string type) {
    if(type=="ASC"){
        if(a[coli]<b[coli]) return true;
    } else {
        if(a[coli]>b[coli]) return true;
    } 
    return false;
}

struct WavecustomComparator_gb {
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

void sort_each_block_gb(int col_index, string sort_type, Table *table){
    
    int block_count = table->blockCount;
    
    //sorting start
    for(int i=0;i<block_count; ++i){
        vector<vector<int>> sort_table = table->getPageData(table->tableName, i);
        
        // cout<<"vector loaded"<<endl;

        stable_sort(sort_table.begin(), sort_table.end(), [col_index, sort_type](vector<int> a, vector<int> b) {
                return customComparator_gb(a, b, col_index, sort_type);
        });
        // cout<<"sort comparison done"<<endl;
        //write back
        write_back_waveRun_gb(0, i, 0, sort_table,table); 
    }
    //all blocks sorting done 

}

void merge_gb(int col_index, string sort_type, Table *table, Table *result, string tablename){

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

    
    // pair<int,pair<int,pair<vector<vector<int>>,pair<int,pair<string,int>>>>> //{value,{idx,{block_vec,{run,{order,block_seq}}}}}}

    priority_queue< pair<int,pair<int,pair<vector<vector<int>>,pair<int,pair<string,int>>>>> , 
    vector < pair<int,pair<int,pair<vector<vector<int>>,pair<int,pair<string,int>>>>> > , 
    WavecustomComparator_gb >pq;
    
    if(total_w == 0){
        vector<vector<int>>buff_wb = table->getPageData(table->tableName, 0, 0, 0);
        final_wave_writeback_gb("temp_gb",0,buff_wb,result, tablename);
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
                        final_wave_writeback_gb("temp_gb",write_back_sequence,buff_wb,result, tablename);
                    }
                    else{
                        write_back_waveRun_gb(w+1, loads, write_back_sequence, buff_wb, table);
                    }
                    buff_wb.clear();
                    write_back_sequence++;
                }
            }
            // cout<<"run completed\nRemaining buffer elements : "<<buff_wb.size()<<endl;
            //If the buffer is incomplete, write it back
            if(buff_wb.size() != 0){
                if(w == total_w-1){
                    final_wave_writeback_gb("temp_gb",write_back_sequence,buff_wb,result,tablename);
                }
                else{
                    write_back_waveRun_gb(w+1, loads, write_back_sequence, buff_wb, table);
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

bool qualifier(int maxi, int mini, double avg, long long sum, int count, string having_func,double having_value){

    double func;
    if(having_func == "MAX"){
        func = maxi;
    }
    else if(having_func == "MIN"){
        func = mini;
    }
    else if(having_func == "AVG"){
        func = avg;
    }
    else if(having_func == "SUM"){
        func = sum;
    }
    else if(having_func == "COUNT"){
        func = count;
    }


    if(parsedQuery.GROUPBYBinaryOperator == LESS_THAN){
        if(func < having_value){
            return true;
        }    
        return false;
    }
    else if(parsedQuery.GROUPBYBinaryOperator == GREATER_THAN){
        if(func > having_value){
            return true;
        }
        return false;
    }
    else if(parsedQuery.GROUPBYBinaryOperator == LEQ){
        if(func <= having_value){
            return true;
        }
        return false;
    }
    else if(parsedQuery.GROUPBYBinaryOperator == GEQ){
        if(func >= having_value){
            return true;
        }
        return false;
    }
    else if(parsedQuery.GROUPBYBinaryOperator == EQUAL){
        if(func == having_value){
            return true;
        }
        return false;
    }
    else if(parsedQuery.GROUPBYBinaryOperator == NOT_EQUAL){
        if(func != having_value){
            return true;
        }
        return false;
    }
}

int return_entry(int maxi, int mini, double avg, long long sum, int count, string return_func){
    if(return_func == "MAX"){
        return maxi;
    }
    else if(return_func == "MIN"){
        return mini;
    }
    else if(return_func == "AVG"){
        return avg;
    }
    else if(return_func == "SUM"){
        return sum;
    }
    else if(return_func == "COUNT"){
        return count;
    }
}

int group_having(int maxRows, int pages, int sort_col, int having_col,string having_col_name, string having_func, string return_func, double having_value, Table *temp_gb, Table *result){
    // vector<vector<int>>vec = temp_gb->getPageData_custom("temp_gb",0);
    int maxi = -1e9;
    int mini = 1e9;
    int avg = 0;
    int sum = 0;
    int count = 0;
    int row_count = 0;
    //Logic is that I'll save the first entry of every distinct element and then keep iterating till I dont
    //get some other value or the file gets over
    //While i'm iterating i'll keep calculating all the params mentioned above
    //Then when this entry gets over, qualify this attribute, if yes then store it in my writeback block
    vector<vector<int>>buff;
    vector<vector<int>>write_back;
    int i=0;
    int entry;
    bool start = true;
    int page_count = 0;
    while(i<pages){
        buff = temp_gb->getPageData_custom("temp_gb",i);
    
        if(start){
            entry = buff[0][sort_col];
            start = !start;
        }

        for(int j=0;j<buff.size();j++){
            // cout<<entry<<" "<<buff[j][sort_col]<<endl;
            // cout<<"temp val: "<<buff[j][sort_col]<<" entry : "<<entry<<endl;
            if(buff[j][sort_col] != entry){
                // cout<<"entered"<<endl;
                //calculate average
                avg = sum/count;
                //new entry has come up. First we qualify this entry
                // cout<<"before going into qualifier : entry : "<<entry<<endl;
                // cout<<"MIN : "<<mini<<"\nMAX : "<<maxi<<"\nAVG : "<<avg<<"\nSUM : "<<sum<<"\ncount : "<<count<<endl;
                if(qualifier(maxi,mini,avg,sum,count,having_func,having_value)){
                    // string res_col = return_func + having_col_name;
                    vector<int>temp_wb;
                    temp_wb.push_back(entry);
                    int ans = return_entry(maxi,mini,avg,sum,count,return_func);
                    temp_wb.push_back(ans);
                    result -> writeRow<int>(temp_wb);
                    // write_back.push_back(temp_wb);

                    // if(write_back.size() == maxRows){
                    //     row_count += maxRows;
                    //     final_writeback_gb(page_count, write_back,result,parsedQuery.GROUPBYResultRelationName);
                    //     page_count++;
                    //     write_back.clear();
                    // }
                }
                // else{
                //     cout<<"qualifier failed at "<<entry<<endl;
                // }

                //reset aggregates
                maxi = -1e9;
                mini = 1e9;
                avg = 0;
                sum = 0;
                count = 0;

                entry = buff[i][sort_col];
                int hc = buff[j][having_col];
                sum += hc;
                maxi = max(maxi,hc);
                mini = min(mini,hc);
                count++;

                entry = buff[j][sort_col];
            }
            else{
                int hc = buff[j][having_col];
                // cout<<"hc : "<<hc<<endl;
                sum += hc;
                maxi = max(maxi,hc);
                mini = min(mini,hc);
                count++;
            }
        }
        i++;

    }
    
    //remaining entry
    avg = sum/count;
    if(qualifier(maxi,mini,avg,sum,count,having_func,having_value)){
                    // string res_col = return_func + having_col_name;
                    vector<int>temp_wb;
                    temp_wb.push_back(entry);
                    int ans = return_entry(maxi,mini,avg,sum,count,return_func);
                    temp_wb.push_back(ans);
                    result -> writeRow<int>(temp_wb);
    }
    return row_count;
}


void executeGROUPBY()
{
    logger.log("executeGROUPBY");

    
    Table *table = tableCatalogue.getTable(parsedQuery.GROUPBYRelationName);
    

    // int columnCount = table->columnCount;
    // int maxRows = 250/columnCount;
    int maxRows = 250/2;
    int pages = table->blockCount;

    Table *temp_gb = new Table("temp_gb");

    string sort_col_name = parsedQuery.GROUPBYGroupingAttribute;
    int sort_col_idx = table->getColumnIndex(sort_col_name);
    string order = "ASC";

    string having_string = parsedQuery.GROUPBYAggregateAttribute;
    
    string having_func = ""; // HAVING AVG(Salary) so this is the AVG, first aggregate func
    string return_func = ""; // RETURN MAX(Salary) so this is the MAX, return aggregate func
    string having_col_name = ""; // This is Salary, the attribute on which the functions are applied
    
    bool flag = true;
    
    int i=0;
    while(i<having_string.size()){
        if(having_string[i] == '('){
            flag = !flag;
            i++;
            continue;
        }
        if(having_string[i] == ')'){
            break;
        }
        if(flag){
            having_func += having_string[i];
        }
        else{
            having_col_name += having_string[i];
        }
        i++;
    }

    string ret_string = parsedQuery.GROUPBYAggregateFuncAttribute;
    double having_value = (double)stoi(parsedQuery.GROUPBYAggregateValue);
    
    i=0;
    while(i<ret_string.size()){
        if(ret_string[i] == '('){
            break;
        }
        else{
            return_func += ret_string[i];
        }
        i++;
    }

    // cout<<having_func<<endl<<return_func<<endl<<having_col<<endl<<having_value<<endl;


    int having_col_idx = table->getColumnIndex(having_col_name); //index of the column
    // cout<<having_col_name<<" "<<having_col_idx<<endl;
    
    string hvc = return_func + having_col_name;
    vector<string> vec = {sort_col_name, hvc};
    Table *res_table = new Table(parsedQuery.GROUPBYResultRelationName,vec);

    //First sort the grouping attribute in asc
    system(("mkdir ../data/temp/" + parsedQuery.GROUPBYRelationName).c_str());
    system("mkdir ../data/temp/temp_gb");
    // system(("mkdir ../data/temp/tempgb").c_str()); //folder to store the sorted pages
    sort_each_block_gb(sort_col_idx,order,table);
    merge_gb(sort_col_idx, order, table,temp_gb,"temp_gb");
    //delete temp folder
    system(("rm -r ../data/temp/" + parsedQuery.GROUPBYRelationName).c_str());
    
    
    int row_count = group_having(maxRows,pages,sort_col_idx,having_col_idx,having_col_name,having_func,return_func,having_value,temp_gb,res_table);
    int blocks_total = ceil((double)row_count/(double)maxRows);

    // cout<<row_count<<" "<<blocks_total<<endl;
    // registerTable(res_table,sort_col_name,having_col_name, row_count, blocks_total,return_func);

    system("rm -r ../data/temp/temp_gb"); //delete the temp folder

    res_table->blockify();
    tableCatalogue.insertTable(res_table);
    

    return;
}