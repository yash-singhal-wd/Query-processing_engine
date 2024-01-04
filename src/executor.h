#include"semanticParser.h"

void executeCommand();

void executeCLEAR();
void executeCROSS();
void executeDISTINCT();
void executeEXPORT();
void executeEXPORTMATRIX();
void executeINDEX();
void executeJOIN();
void executeORDERBY();
void executeGROUPBY();
void executeLIST();
void executeLOAD();
void executeLOADMATRIX();
void executePRINT();
void executePRINTMATRIX();
void executePROJECTION();
void executeRENAME();
void executeRENAMEMATRIX();
void executeSELECTION();
void executeSORT();
void executeSOURCE();
void executeTRANSPOSEMATRIX();
void executeSYMMETRY();
void executeCOMPUTE();

bool evaluateBinOp(int value1, int value2, BinaryOperator binaryOperator);
void printRowCount(int rowCount);