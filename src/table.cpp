#include "global.h"

/**
 * @brief Construct a new Table:: Table object
 *
 */
Table::Table()
{
    logger.log("Table::Table");
}

/**
 * @brief Construct a new Table:: Table object used in the case where the data
 * file is available and LOAD command has been called. This command should be
 * followed by calling the load function;
 *
 * @param tableName
 */
Table::Table(string tableName)
{
    logger.log("Table::Table");
    this->sourceFileName = "../data/" + tableName + ".csv";
    this->tableName = tableName;
    // this->firstName = tableName;
}

/**
 * @brief Construct a new Table:: Table object used when an assignment command
 * is encountered. To create the table object both the table name and the
 * columns the table holds should be specified.
 *
 * @param tableName
 * @param columns
 */
Table::Table(string tableName, vector<string> columns)
{
    logger.log("Table::Table");
    this->sourceFileName = "../data/temp/" + tableName + ".csv";
    this->tableName = tableName;
    this->columns = columns;
    this->columnCount = columns.size();
    this->maxRowsPerBlock = (unsigned int)((BLOCK_SIZE * 1000) / (sizeof(int) * columnCount));
    this->writeRow<string>(columns);
}

/**
 * @brief The load function is used when the LOAD command is encountered. It
 * reads data from the source file, splits it into blocks and updates table
 * statistics.
 *
 * @return true if the table has been successfully loaded
 * @return false if an error occurred
 */
bool Table::load()
{
    logger.log("Table::load");
    fstream fin(this->sourceFileName, ios::in);
    string line;
    if (getline(fin, line))
    {
        fin.close();
        if (this->extractColumnNames(line))
            if (this->blockify())
                return true;
    }
    fin.close();
    return false;
}

bool Table::loadMatrix()
{
    logger.log("Matrix::load");
    fstream fin(this->sourceFileName, ios::in);
    string line;
    if (getline(fin, line))
    {
        fin.close();
        if (this->extractColumnNamesMatrix(line))
            if (this->blockifyMatrix())
                return true;
    }
    fin.close();
    return false;
}

/**
 * @brief Function extracts column names from the header line of the .csv data
 * file.
 *
 * @param line
 * @return true if column names successfully extracted (i.e. no column name
 * repeats)
 * @return false otherwise
 */
bool Table::extractColumnNames(string firstLine)
{
    logger.log("Table::extractColumnNames");
    unordered_set<string> columnNames;
    string word;
    stringstream s(firstLine);
    while (getline(s, word, ','))
    {
        word.erase(std::remove_if(word.begin(), word.end(), ::isspace), word.end());
        if (columnNames.count(word))
            return false;
        columnNames.insert(word);
        this->columns.emplace_back(word);
    }
    this->columnCount = this->columns.size();
    this->maxRowsPerBlock = (unsigned int)((BLOCK_SIZE * 1000) / (sizeof(int) * this->columnCount));
    return true;
}

bool Table::extractColumnNamesMatrix(string firstLine)
{
    logger.log("Table::extractColumnNamesMatrix");
    unordered_set<string> columnNames;
    string word;
    stringstream s(firstLine);

    while (getline(s, word, ','))
    {

        word.erase(std::remove_if(word.begin(), word.end(), ::isspace), word.end());
        this->columnCount++;
        this->rowCount++;
        // this->columns.emplace_back(word);
    }
    logger.log(to_string(this->columnCount));

    // this->columnCount = this->columns.size();
    // this->rowCount = this->columns.size();
    this->maxRowsPerBlock = 15;

    return true;
}

/**
 * @brief This function splits all the rows and stores them in multiple files of
 * one block size.
 *
 * @return true if successfully blockified
 * @return false otherwise
 */
bool Table::blockify()
{
    logger.log("Table::blockify");
    ifstream fin(this->sourceFileName, ios::in);
    string line, word;
    vector<int> row(this->columnCount, 0);
    vector<vector<int>> rowsInPage(this->maxRowsPerBlock, row);
    int pageCounter = 0;
    unordered_set<int> dummy;
    dummy.clear();
    this->distinctValuesInColumns.assign(this->columnCount, dummy);
    this->distinctValuesPerColumnCount.assign(this->columnCount, 0);
    getline(fin, line);
    while (getline(fin, line))
    {
        stringstream s(line);
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (!getline(s, word, ','))
                return false;
            row[columnCounter] = stoi(word);
            rowsInPage[pageCounter][columnCounter] = row[columnCounter];
        }
        pageCounter++;
        this->updateStatistics(row);
        if (pageCounter == this->maxRowsPerBlock)
        {
            bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
            this->blockCount++;
            this->rowsPerBlockCount.emplace_back(pageCounter);
            pageCounter = 0;
        }
    }
    if (pageCounter)
    {
        bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
        this->blockCount++;
        this->rowsPerBlockCount.emplace_back(pageCounter);
        pageCounter = 0;
    }

    if (this->rowCount == 0)
        return false;
    this->distinctValuesInColumns.clear();
    return true;
}

bool Table::blockifyMatrix()
{
    logger.log("Table::blockifyMatrix");
    ifstream fin(this->sourceFileName, ios::in);
    string line, word;

    vector<int> row;
    vector<vector<int>> col;
    vector<vector<vector<int>>> rowBlock;

    int pageColCounter = 0;
    int pageRowCounter = 0;
    int rowCounter = 0;
    int pageCounter = 0;
    int blockRead = 0, blockWritten = 0;
    bool flag = true;
    int rows = 0;
    while (getline(fin, line))
    {

        stringstream s(line);
        vector<int> temp;
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (!getline(s, word, ','))
                return false;
            if (flag)
            {

                temp.push_back(stoi(word));

                if ((columnCounter + 1) % this->maxRowsPerBlock == 0)
                {
                    col.push_back(temp);
                    rowBlock.push_back(col);
                    col.clear();
                    temp.clear();
                }
                else if (columnCounter == this->columnCount - 1)
                {

                    col.push_back(temp);
                    rowBlock.push_back(col);
                    col.clear();
                    temp.clear();
                }
            }
            else
            {

                temp.push_back(stoi(word));
                if ((columnCounter + 1) % this->maxRowsPerBlock == 0)
                {
                    rowBlock[pageColCounter].push_back(temp);
                    temp.clear();
                    pageColCounter++;
                }
                else if (columnCounter == this->columnCount - 1)
                {

                    rowBlock[pageColCounter].push_back(temp);
                    temp.clear();
                    pageColCounter++;
                }
            }
        }

        pageCounter++;
        pageColCounter = 0;
        pageRowCounter++;
        flag = false;
        rows++;

        if (pageRowCounter == this->maxRowsPerBlock || rows == this->rowCount)
        {
            for (int i = 0; i < rowBlock.size(); i++)
            {
                string pageIndex = (to_string(rowCounter) + "_" + to_string(i));
                bufferManager.writePage(this->tableName, pageIndex, rowBlock[i], pageCounter);
                blockWritten++;
            }
            rowCounter++;
            this->rowsPerBlockCount.emplace_back(pageCounter);
            pageCounter = 0;
            pageRowCounter = 0;
            flag = true;
            rowBlock.clear();
        }
    }
    this->blockCount = blockWritten;
    cout << "Number of blocks read: " << blockRead << endl;
    cout << "Number of blocks written: " << blockWritten << endl;
    cout << "Number of blocks accessed: " << blockRead + blockWritten << endl;
    return true;
}

/**
 * @brief Given a row of values, this function will update the statistics it
 * stores i.e. it updates the number of rows that are present in the column and
 * the number of distinct values present in each column. These statistics are to
 * be used during optimisation.
 *
 * @param row
 */
void Table::updateStatistics(vector<int> row)
{
    this->rowCount++;
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (!this->distinctValuesInColumns[columnCounter].count(row[columnCounter]))
        {
            this->distinctValuesInColumns[columnCounter].insert(row[columnCounter]);
            this->distinctValuesPerColumnCount[columnCounter]++;
        }
    }
}

/**
 * @brief Checks if the given column is present in this table.
 *
 * @param columnName
 * @return true
 * @return false
 */
bool Table::isColumn(string columnName)
{
    logger.log("Table::isColumn");
    for (auto col : this->columns)
    {
        if (col == columnName)
        {
            return true;
        }
    }
    return false;
}

/**
 * @brief Renames the column indicated by fromColumnName to toColumnName. It is
 * assumed that checks such as the existence of fromColumnName and the non prior
 * existence of toColumnName are done.
 *
 * @param fromColumnName
 * @param toColumnName
 */
void Table::renameColumn(string fromColumnName, string toColumnName)
{
    logger.log("Table::renameColumn");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (columns[columnCounter] == fromColumnName)
        {
            columns[columnCounter] = toColumnName;
            break;
        }
    }
    return;
}

void Table::renameMatrix(string matrixName, string newMatrixName)
{
    logger.log("Matrix::renameMatrix");
    this->tableName = newMatrixName;
    this->sourceFileName = "../data/" + newMatrixName + ".csv";
    return;
}

/**
 * @brief Function prints the first few rows of the table. If the table contains
 * more rows than PRINT_COUNT, exactly PRINT_COUNT rows are printed, else all
 * the rows are printed.
 *
 */
// void Table::print()
// {
//     logger.log("Table::print");
//     unsigned int count = min((long long)PRINT_COUNT, this->rowCount);

//     // print headings
//     this->writeRow(this->columns, cout);

//     Cursor cursor(this->tableName, 0);
//     vector<int> row;
//     for (int rowCounter = 0; rowCounter < count; rowCounter++)
//     {
//         row = cursor.getNext();
//         this->writeRow(row, cout);
//     }
//     printRowCount(this->rowCount);
// }

void Table::print()
{
    logger.log("Table::print");
    string fileName = "../data/temp/" + this->tableName + "_Page";
    unsigned int count = this->blockCount;
    int rowCounter = 0;
    // print headings
    this->writeRow(this->columns, cout);
    vector<vector<int>> vec;
    for (int blockCounter = 0; blockCounter < count; blockCounter++)
    {
        fileName += to_string(blockCounter);
        std::ifstream inputFile(fileName);
        std::string line;

        while (std::getline(inputFile, line))
        {
            std::vector<int> row;
            std::istringstream iss(line);

            int value;
            while (iss >> value)
            {
                row.push_back(value);
            }
           
            rowCounter++;
            vec.push_back(row);
        }

        inputFile.close();
        if (rowCounter >= min((long long)20, this->rowCount))
            break;
    }
    for (int i = 0; i < vec.size(); i++)
    {
        for (int j = 0; j < vec[i].size(); j++)
        {
            printf("%d,", vec[i][j]);
        }
        printf("\n");
    }
}

void Table::printMatrix()
{
    logger.log("Matrix::print");
    int blockRead = 0, blockWritten = 0;

    int colCounter = 0, rowCounter = 0;
    for (int i = 0; i < min(20, int(this->columnCount)); i++)
    {
        colCounter = 0;
        int j = 0;
        while (j < min(20, int(this->columnCount)))
        {
            vector<vector<int>> vec;
            std::ifstream inputFile("../data/temp/" + this->tableName + "_Page" + to_string(rowCounter) + "_" + to_string(colCounter));
            blockRead++;
            std::string line;

            while (std::getline(inputFile, line))
            {

                std::vector<int> row;
                std::istringstream iss(line);

                int value;
                while (iss >> value)
                {
                    row.push_back(value);
                }

                vec.push_back(row);
            }
            inputFile.close();

            for (int k = 0; k < vec[0].size() && j < 20; k++)
            {
                if (j != this->columnCount - 1 && j != 19)
                    std::cout << vec[i % 15][k] << ",";
                else
                    std::cout << vec[i % 15][k];
                j++;
            }
            colCounter++;
        }
        std::cout << endl;
        if ((i + 1) % 15 == 0)
            rowCounter++;
    }

    // unsigned int count = min((long long)PRINT_COUNT, this->rowCount);

    // Cursor cursor(this->tableName, 0);
    // vector<int> row;
    // for (int rowCounter = 0; rowCounter < count; rowCounter++)
    // {
    //     row = cursor.getNext();
    //     this->writeRow(row, std::cout);
    // }
    std::cout << "Number of blocks read: " << blockRead << endl;
    std::cout << "Number of blocks written: " << blockWritten << endl;
    std::cout << "Number of blocks accessed: " << blockRead + blockWritten << endl;
    printRowCount(min(20, int(this->rowCount)));
}

vector<vector<int>> Table::getPageData(string tablename, int i, int j)
{
    logger.log("Matrix::getPageData");

    vector<vector<int>> vec;
    std::ifstream inputFile("../data/temp/" + tableName + "_Page" + to_string(i) + "_" + to_string(j));
    std::string line;

    while (std::getline(inputFile, line))
    {
        std::vector<int> row;
        std::istringstream iss(line);

        int value;
        while (iss >> value)
        {
            row.push_back(value);
        }

        vec.push_back(row);
    }
    inputFile.close();
    return vec;
}

vector<vector<int>> Table::getPageData(string tablename, int i)
{
    logger.log("Matrix::getPageData");

    vector<vector<int>> vec;
    std::ifstream inputFile("../data/temp/" + tableName + "_Page" + to_string(i));
    std::string line;

    while (std::getline(inputFile, line))
    {
        std::vector<int> row;
        std::istringstream iss(line);

        int value;
        while (iss >> value)
        {
            row.push_back(value);
        }

        vec.push_back(row);
    }
    inputFile.close();
    return vec;
}

vector<vector<int>> Table::getPageDataJoin(string tablename, int i)
{
    logger.log("Matrix::getPageData");

    vector<vector<int>> vec;
    std::ifstream inputFile("../data/temp/temp_join/" + tableName + "_Page" + to_string(i));
    std::string line;

    while (std::getline(inputFile, line))
    {
        std::vector<int> row;
        std::istringstream iss(line);

        int value;
        while (iss >> value)
        {
            row.push_back(value);
        }

        vec.push_back(row);
    }
    inputFile.close();
    return vec;
}

vector<vector<int>> Table::getPageData(string tablename, int waveNumber, int runNumber, int pageNumber)
{
    logger.log("Matrix::getPageData");

    vector<vector<int>> vec;
    std::ifstream inputFile("../data/temp/" + tableName + "/" + tableName + "_Page_W" + to_string(waveNumber) + "_R" + to_string(runNumber) + "_" + to_string(pageNumber));
    std::string line;
    if (inputFile.is_open())
    {
        while (std::getline(inputFile, line))
        {
            std::vector<int> row;
            std::istringstream iss(line);

            int value;
            while (iss >> value)
            {
                row.push_back(value);
            }

            vec.push_back(row);
        }
        inputFile.close();
    }

    return vec;
}

vector<vector<int>> Table::getPageData_custom(string tablename, int block)
{
    logger.log("Matrix::getPageData");

    vector<vector<int>> vec;
    std::ifstream inputFile("../data/temp/" + tableName + "/" + tableName + "_Page" + to_string(block));
    std::string line;
    if (inputFile.is_open())
    {
        while (std::getline(inputFile, line))
        {
            std::vector<int> row;
            std::istringstream iss(line);

            int value;
            while (iss >> value)
            {
                row.push_back(value);
            }

            vec.push_back(row);
        }
        inputFile.close();
    }

    return vec;
}

/**
 * @brief This function returns one row of the table using the cursor object. It
 * returns an empty row is all rows have been read.
 *
 * @param cursor
 * @return vector<int>
 */
void Table::getNextPage(Cursor *cursor)
{
    logger.log("Table::getNext");

    if (cursor->pageIndex < this->blockCount - 1)
    {
        cursor->nextPage(cursor->pageIndex + 1);
    }
}

/**
 * @brief called when EXPORT command is invoked to move source file to "data"
 * folder.
 *
 */
void Table::makePermanent()
{
    logger.log("Table::makePermanent");
    if (!this->isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
    string newSourceFile = "../data/" + this->tableName + ".csv";
    ofstream fout(newSourceFile, ios::out);

    // print headings
    this->writeRow(this->columns, fout);

    Cursor cursor(this->tableName, 0);
    vector<int> row;
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        row = cursor.getNext();
        this->writeRow(row, fout);
    }
    fout.close();
}

void Table::makePermanentMatrix()
{
    int blockRead = 0, blockWritten = 0;

    logger.log("Matrix::makePermanent");
    if (!this->isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
    string newSourceFile = "../data/" + this->tableName + ".csv";
    ofstream fout(newSourceFile, ios::out);
    int colCounter = 0, rowCounter = 0;
    for (int i = 0; i < this->columnCount; i++)
    {
        colCounter = 0;
        int j = 0;
        while (j < this->columnCount)
        {
            vector<vector<int>> vec;
            std::ifstream inputFile("../data/temp/" + this->tableName + "_Page" + to_string(rowCounter) + "_" + to_string(colCounter));
            blockRead++;
            std::string line;

            while (std::getline(inputFile, line))
            {

                std::vector<int> row;
                std::istringstream iss(line);

                int value;
                while (iss >> value)
                {
                    row.push_back(value);
                }

                vec.push_back(row);
            }
            inputFile.close();

            for (int k = 0; k < vec[0].size(); k++)
            {
                if (j != this->columnCount - 1)
                    fout << vec[i % 15][k] << ",";
                else
                    fout << vec[i % 15][k];
                j++;
            }
            colCounter++;
        }
        fout << endl;
        if ((i + 1) % 15 == 0)
            rowCounter++;
    }

    cout << "Number of blocks read: " << blockRead << endl;
    cout << "Number of blocks written: " << blockWritten << endl;
    cout << "Number of blocks accessed: " << blockRead + blockWritten << endl;
    return;
}
// void Table::makePermanentMatrix1()
// {
//     logger.log("Matrix::makePermanent");
//     if (!this->isPermanent())
//         bufferManager.deleteFile(this->sourceFileName);
//     string newSourceFile = "../data/" + this->tableName + ".csv";
//     ofstream fout(newSourceFile, ios::out);

//     Cursor cursor(this->tableName, 0);
//     vector<int> row;
//     for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
//     {
//         row = cursor.getNext();
//         this->writeRow(row, fout);
//     }
//     fout.close();
// }

/**
 * @brief Function to check if table is already exported
 *
 * @return true if exported
 * @return false otherwise
 */
bool Table::isPermanent()
{
    logger.log("Table::isPermanent");
    if (this->sourceFileName == "../data/" + this->tableName + ".csv")
        return true;
    return false;
}

/**
 * @brief The unload function removes the table from the database by deleting
 * all temporary files created as part of this table
 *
 */
void Table::unload()
{
    logger.log("Table::~unload");
    for (int pageCounter = 0; pageCounter < this->blockCount; pageCounter++)
        bufferManager.deleteFile(this->tableName, pageCounter);
    if (!isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
}

/**
 * @brief Function that returns a cursor that reads rows from this table
 *
 * @return Cursor
 */
Cursor Table::getCursor()
{
    logger.log("Table::getCursor");
    Cursor cursor(this->tableName, 0);
    return cursor;
}
/**
 * @brief Function that returns the index of column indicated by columnName
 *
 * @param columnName
 * @return int
 */
int Table::getColumnIndex(string columnName)
{
    logger.log("Table::getColumnIndex");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (this->columns[columnCounter] == columnName)
            return columnCounter;
    }
    return -1;
}