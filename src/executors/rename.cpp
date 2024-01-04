#include <iostream>
#include <fstream>
#include <cstdio>
#include "global.h"
/**
 * @brief
 * SYNTAX: RENAME column_name TO column_name FROM relation_name
 */
bool syntacticParseRENAME()
{
    logger.log("syntacticParseRENAME");
    if (tokenizedQuery.size() != 6 || tokenizedQuery[2] != "TO" || tokenizedQuery[4] != "FROM")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = RENAME;
    parsedQuery.renameFromColumnName = tokenizedQuery[1];
    parsedQuery.renameToColumnName = tokenizedQuery[3];
    parsedQuery.renameRelationName = tokenizedQuery[5];
    return true;
}

bool syntacticParseRENAMEMATRIX()
{
    logger.log("syntacticParseRENAMEMATRIX");
    if (tokenizedQuery.size() != 4)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = RENAME_MATRIX;
    parsedQuery.newRenameRelationName = tokenizedQuery[3];
    parsedQuery.renameRelationName = tokenizedQuery[2];
    return true;
}

bool semanticParseRENAME()
{
    logger.log("semanticParseRENAME");

    if (!tableCatalogue.isTable(parsedQuery.renameRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.renameFromColumnName, parsedQuery.renameRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    if (tableCatalogue.isColumnFromTable(parsedQuery.renameToColumnName, parsedQuery.renameRelationName))
    {
        cout << "SEMANTIC ERROR: Column with name already exists" << endl;
        return false;
    }
    return true;
}

bool semanticParseRENAMEMATRIX()
{
    logger.log("semanticParseRENAMEMATRIX");

    if (!tableCatalogue.isTable(parsedQuery.renameRelationName))
    {
        cout << "SEMANTIC ERROR: Matrix doesn't exist" << endl;
        return false;
    }
    return true;
}

void executeRENAME()
{
    logger.log("executeRENAME");
    Table *table = tableCatalogue.getTable(parsedQuery.renameRelationName);
    table->renameColumn(parsedQuery.renameFromColumnName, parsedQuery.renameToColumnName);
    return;
}
// void executeRENAMEMATRIX1()
// {
//     logger.log("executeRENAMEMATRIX");
//     Table *table = tableCatalogue.getTable(parsedQuery.renameRelationName);

//     Cursor cursor(parsedQuery.renameRelationName, 0);

//     while (cursor.pageIndex != table->blockCount)
//     {
//         string oldFileName = "../data/temp/" + parsedQuery.renameRelationName + "_Page" + to_string(cursor.pageIndex);
//         string newFileName = "../data/temp/" + parsedQuery.newRenameRelationName + "_Page" + to_string(cursor.pageIndex);

//         std::ifstream oldFile(oldFileName.c_str());

//         if (!oldFile.is_open())
//         {
//             std::cerr << "Error opening the old file." << std::endl;
//             return;
//         }

//         std::ofstream newFile(newFileName.c_str());

//         if (!newFile.is_open())
//         {
//             std::cerr << "Error creating the new file." << std::endl;
//             oldFile.close();
//             return;
//         }

//         char ch;
//         while (oldFile.get(ch))
//         {
//             newFile.put(ch);
//         }
//         oldFile.close();
//         newFile.close();
//         if (std::remove((oldFileName.c_str())) != 0)
//         {
//             std::cerr << "Error deleting the old file." << std::endl;
//             return;
//         }
//         else
//         {
//             std::cout << "File renamed successfully." << std::endl;
//         }
//         if (cursor.pageIndex + 1 != table->blockCount)
//             cursor.nextPage(cursor.pageIndex + 1);
//         else
//             break;
//     }
//     tableCatalogue.renameTable(parsedQuery.renameRelationName, parsedQuery.newRenameRelationName);
//     // table->renameMatrix(parsedQuery.renameRelationName, parsedQuery.newRenameRelationName);

//     return;
// }

void executeRENAMEMATRIX()
{

    int blockRead = 0, blockWritten = 0;
    logger.log("executeRENAMEMATRIX");
    Table *table = tableCatalogue.getTable(parsedQuery.renameRelationName);

    int c = ceil(float(table->rowCount) / float(table->maxRowsPerBlock));
    // cout<<c<<endl;
    for (int i = 0; i < c; i++)
    {
        for (int j = 0; j < c; j++)
        {
            string oldFileName = "../data/temp/" + parsedQuery.renameRelationName + "_Page" + to_string(i) + "_" + to_string(j);
            std::ifstream oldFile(oldFileName.c_str());
            blockRead++;
            if (!oldFile.is_open())
            {
                std::cerr << "Error opening the old file." << std::endl;
                return;
            }

            vector<vector<int>> vec;
            std::string line;

            while (std::getline(oldFile, line))
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
            oldFile.close();

            bufferManager.writePage(parsedQuery.newRenameRelationName, to_string(i) + "_" + to_string(j), vec, vec.size());
            blockWritten++;
            if (std::remove((oldFileName.c_str())) != 0)
            {
                std::cerr << "Error deleting the old file." << std::endl;
                return;
            }
        }
    }
    tableCatalogue.renameTable(parsedQuery.renameRelationName, parsedQuery.newRenameRelationName);
    cout << "Number of blocks read: " << blockRead << endl;
    cout << "Number of blocks written: " << blockWritten << endl;
    cout << "Number of blocks accessed: " << blockRead + blockWritten << endl;
    return;
}