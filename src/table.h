#include "cursor.h"

enum IndexingStrategy
{
    BTREE,
    HASH,
    NOTHING
};

/**
 * @brief The Table class holds all information related to a loaded table. It
 * also implements methods that interact with the parsers, executors, cursors
 * and the buffer manager. There are typically 2 ways a table object gets
 * created through the course of the workflow - the first is by using the LOAD
 * command and the second is to use assignment statements (SELECT, PROJECT,
 * JOIN, SORT, CROSS and DISTINCT).
 *
 */
class Table
{
    vector<unordered_set<int>> distinctValuesInColumns;

public:
    // string matrixName = "";
    long long int matrixRowCount = 0;
    unsigned int matrixBlockCount = 0;


    // string firstName = "";
    string sourceFileName = "";
    string tableName = "";
    vector<string> columns;
    vector<unsigned int> distinctValuesPerColumnCount;
    unsigned int columnCount = 0;
    long long int rowCount = 0;
    unsigned int blockCount = 0;

    unsigned int maxRowsPerBlock = 0;

    vector<unsigned int> rowsPerBlockCount;
    bool indexed = false;
    string indexedColumn = "";
    IndexingStrategy indexingStrategy = NOTHING;

    bool extractColumnNames(string firstLine);
    bool extractColumnNamesMatrix(string firstLine);
    bool blockify();
    bool blockifyMatrix();
    void updateStatistics(vector<int> row);
    Table();
    Table(string tableName);
    Table(string tableName, vector<string> columns);
    bool load();
    bool loadMatrix();
    bool isColumn(string columnName);
    void renameColumn(string fromColumnName, string toColumnName);
    void renameMatrix(string matrixName, string newMatrixName);
    void print();
    void printMatrix();
    vector<vector<int> >getPageData(string tablename,int i, int j);
    vector<vector<int> >getPageData(string tablename,int pageNumber);
    vector<vector<int> > getPageData(string tablename, int waveNumber, int runNumber, int pageNumber);
    vector<vector<int> > getPageData_custom(string tablename, int block);
    vector<vector<int> > getPageDataJoin(string tablename, int block);
    
    void makePermanent();
    void makePermanentMatrix();
    bool isPermanent();
    void getNextPage(Cursor *cursor);
    Cursor getCursor();
    int getColumnIndex(string columnName);
    void unload();

    /**
     * @brief Static function that takes a vector of valued and prints them out in a
     * comma seperated format.
     *
     * @tparam T current usaages include int and string
     * @param row
     */
    template <typename T>
    void writeRow(vector<T> row, ostream &fout)
    {
        logger.log("Table::printRow");
        for (int columnCounter = 0; columnCounter < row.size(); columnCounter++)
        {
            if (columnCounter != 0)
                fout << ", ";
            fout << row[columnCounter];
        }
        fout << endl;
    }

    /**
     * @brief Static function that takes a vector of valued and prints them out in a
     * comma seperated format.
     *
     * @tparam T current usaages include int and string
     * @param row
     */
    template <typename T>
    void writeRow(vector<T> row)
    {
        logger.log("Table::printRow");
        ofstream fout(this->sourceFileName, ios::app);
        this->writeRow(row, fout);
        fout.close();
    }
};