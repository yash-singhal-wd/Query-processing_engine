#include "global.h"
/**
 * @brief 
 * SYNTAX: SOURCE filename
 */
bool syntacticParseSOURCE()
{
    logger.log("syntacticParseSOURCE");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = SOURCE;
    parsedQuery.sourceFileName = tokenizedQuery[1];
    return true;
}

bool semanticParseSOURCE()
{
    logger.log("semanticParseSOURCE");
    if (!isQueryFile(parsedQuery.sourceFileName))
    {
        cout << "SEMANTIC ERROR: File doesn't exist" << endl;
        return false;
    }
    return true;
}

void doCommand1()
{
    logger.log("doCommand1");
    if (syntacticParse() && semanticParse())
        executeCommand();
    return;
}

void executeSOURCE()
{
    logger.log("executeSOURCE");

    regex delim("[^\\s,]+");
    string command;
    // system("rm -rf ../data/temp");
    // system("mkdir ../data/temp");

    fstream new_file;
    new_file.open("../data/" + parsedQuery.sourceFileName + ".ra", ios::in);
    cout << parsedQuery.sourceFileName << endl;
    // string command;
    if(!new_file.is_open())
	{
		cout<<"Unable to open the file."<<endl;
		return;
	}
    while (getline(new_file, command))
    {
        cout << "\n> ";
        cout << command << endl;
        tokenizedQuery.clear();
        parsedQuery.clear();
        logger.log("\nReading New Command: ");
        // getline(cin, command);
        logger.log(command);

        auto words_begin = std::sregex_iterator(command.begin(), command.end(), delim);
        auto words_end = std::sregex_iterator();
        for (std::sregex_iterator i = words_begin; i != words_end; ++i)
            tokenizedQuery.emplace_back((*i).str());

        if (tokenizedQuery.size() == 1 && tokenizedQuery.front() == "QUIT")
        {
            break;
        }

        if (tokenizedQuery.empty())
        {
            continue;
        }

        if (tokenizedQuery.size() == 1)
        {
            cout << "SYNTAX ERROR" << endl;
            continue;
        }

        doCommand1();
    }
    return;
}
