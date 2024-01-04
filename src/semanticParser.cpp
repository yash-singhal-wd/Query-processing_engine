#include"global.h"

bool semanticParse(){
    logger.log("semanticParse");
    switch(parsedQuery.queryType){
        case ORDERBY: return semanticParseORDERBY();
        case GROUPBY: return semanticParseGROUPBY();
        case CLEAR: return semanticParseCLEAR();
        case CROSS: return semanticParseCROSS();
        case DISTINCT: return semanticParseDISTINCT();
        case EXPORT: return semanticParseEXPORT();
        case EXPORT_MATRIX: return semanticParseEXPORTMATRIX();
        case INDEX: return semanticParseINDEX();
        case JOIN: return semanticParseJOIN();
        case LIST: return semanticParseLIST();
        case LOAD: return semanticParseLOAD();
        case LOAD_MATRIX: return semanticParseLOADMATRIX();
        case PRINT: return semanticParsePRINT();
        case TRANSPOSE_MATRIX: return semanticParseTRANSPOSEMATRIX();
        case PRINT_MATRIX: return semanticParsePRINTMATRIX();
        case PROJECTION: return semanticParsePROJECTION();
        case RENAME: return semanticParseRENAME();
        case RENAME_MATRIX: return semanticParseRENAMEMATRIX();
        case SELECTION: return semanticParseSELECTION();
        case SORT: return semanticParseSORT();
        case SOURCE: return semanticParseSOURCE();
        case CHECKSYMMETRY: return semanticParseSYMMETRY();
        case COMPUTE: return semanticParseCOMPUTE();
        default: cout<<"SEMANTIC ERROR"<<endl;
    }

    return false;
}