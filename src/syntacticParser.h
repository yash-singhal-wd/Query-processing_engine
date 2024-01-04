#include "tableCatalogue.h"
// #include "matrixCatalogue.h"
using namespace std;

enum QueryType
{
    CLEAR,
    ORDERBY,
    GROUPBY,
    CROSS,
    DISTINCT,
    EXPORT,
    EXPORT_MATRIX,
    INDEX,
    JOIN,
    LIST,
    LOAD,
    LOAD_MATRIX,
    PRINT,
    PRINT_MATRIX,
    TRANSPOSE_MATRIX,
    PROJECTION,
    RENAME,
    RENAME_MATRIX,
    SELECTION,
    SORT,
    SOURCE,
    UNDETERMINED,
    CHECKSYMMETRY,
    COMPUTE,
};

enum BinaryOperator
{
    LESS_THAN,
    GREATER_THAN,
    LEQ,
    GEQ,
    EQUAL,
    NOT_EQUAL,
    NO_BINOP_CLAUSE
};

enum SortingStrategy
{
    ASC,
    DESC,
    NO_SORT_CLAUSE
};

enum SelectType
{
    COLUMN,
    INT_LITERAL,
    NO_SELECT_CLAUSE
};

class ParsedQuery
{

public:
    QueryType queryType = UNDETERMINED;
    string orderByRelationName="";
    string orderByAttribute="";
    string orderByResultRelationName="";

    string GROUPBYResultRelationName="";
    string GROUPBYRelationName="";
    string GROUPBYGroupingAttribute="";
    string GROUPBYAggregateAttribute="";
    string GROUPBYAggregateFuncAttribute="";
    string GROUPBYAggregateValue="";
    BinaryOperator GROUPBYBinaryOperator = NO_BINOP_CLAUSE;

    string clearRelationName = "";
    int sortQueryCount = 0;
    string crossResultRelationName = "";
    string crossFirstRelationName = "";
    string crossSecondRelationName = "";

    string distinctResultRelationName = "";
    string distinctRelationName = "";

    string exportRelationName = "";

    IndexingStrategy indexingStrategy = NOTHING;
    // IndexingStrategy1 indexingStrategy1 = NOTHING1;
    string indexColumnName = "";
    string indexRelationName = "";

    BinaryOperator joinBinaryOperator = NO_BINOP_CLAUSE;
    string joinResultRelationName = "";
    string joinFirstRelationName = "";
    string joinSecondRelationName = "";
    string joinFirstColumnName = "";
    string joinSecondColumnName = "";

    string loadRelationName = "";
    string loadMatrixName = "";
    string printRelationName = "";

    string projectionResultRelationName = "";
    vector<string> projectionColumnList;
    string projectionRelationName = "";

    string renameFromColumnName = "";
    string renameToColumnName = "";
    string renameRelationName = "";
    string newRenameRelationName = "";

    SelectType selectType = NO_SELECT_CLAUSE;
    BinaryOperator selectionBinaryOperator = NO_BINOP_CLAUSE;
    string selectionResultRelationName = "";
    string selectionRelationName = "";
    string selectionFirstColumnName = "";
    string selectionSecondColumnName = "";
    int selectionIntLiteral = 0;

    SortingStrategy orderByStrategy = NO_SORT_CLAUSE;

    SortingStrategy sortingStrategy = NO_SORT_CLAUSE;
    string sortResultRelationName = "";
    string sortColumnName = "";
    vector<pair<string,string>> sortColumnNames;
    string sortRelationName = "";

    string sourceFileName = "";

    ParsedQuery();
    void clear();
};

bool syntacticParse();
bool syntacticParseORDERBY();
bool syntacticParseGROUPBY();
bool syntacticParseCLEAR();
bool syntacticParseCROSS();
bool syntacticParseDISTINCT();
bool syntacticParseEXPORT();
bool syntacticParseEXPORTMATRIX();
bool syntacticParseINDEX();
bool syntacticParseJOIN();
bool syntacticParseLIST();
bool syntacticParseLOAD();
bool syntacticParseLOADMATRIX();
bool syntacticParseTRANSPOSEMATRIX();
bool syntacticParsePRINT();
bool syntacticParsePRINTMATRIX();
bool syntacticParsePROJECTION();
bool syntacticParseRENAME();
bool syntacticParseRENAMEMATRIX();
bool syntacticParseSELECTION();
bool syntacticParseSORT();
bool syntacticParseSOURCE();
bool syntacticParseSYMMETRY();
bool syntacticParseCOMPUTE();

bool isFileExists(string tableName);
bool isQueryFile(string fileName);
