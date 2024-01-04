#include "global.h"

// comment to test github

void executeCommand()
{

    switch (parsedQuery.queryType)
    {
    case GROUPBY:
        executeGROUPBY();
        break;
    case ORDERBY:
        executeORDERBY();
        break;
    case CLEAR:
        executeCLEAR();
        break;
    case CROSS:
        executeCROSS();
        break;
    case DISTINCT:
        executeDISTINCT();
        break;
    case EXPORT:
        executeEXPORT();
        break;
    case EXPORT_MATRIX:
        executeEXPORTMATRIX();
        break;
    case INDEX:
        executeINDEX();
        break;
    case JOIN:
        executeJOIN();
        break;
    case CHECKSYMMETRY:
        executeSYMMETRY();
        break;
    case COMPUTE:
        executeCOMPUTE();
        break;
    case LIST:
        executeLIST();
        break;
    case LOAD:
        executeLOAD();
        break;
    case LOAD_MATRIX:
        return executeLOADMATRIX();
    case PRINT:
        executePRINT();
        break;
    case PRINT_MATRIX:
        executePRINTMATRIX();
        break;
    case PROJECTION:
        executePROJECTION();
        break;
    case RENAME:
        executeRENAME();
        break;
    case TRANSPOSE_MATRIX:
        executeTRANSPOSEMATRIX();
        break;
    case RENAME_MATRIX:
        executeRENAMEMATRIX();
        break;
    case SELECTION:
        executeSELECTION();
        break;
    case SORT:
        executeSORT();
        break;
    case SOURCE:
        executeSOURCE();
        break;
    default:
        cout << "PARSING ERROR" << endl;
    }

    return;
}

void printRowCount(int rowCount)
{
    cout << "\n\nRow Count: " << rowCount << endl;
    return;
}