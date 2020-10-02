import pandas as pd
dataTableName1 = ''
dfTableDict = dict()
dataTableIsCustomIndex = {dataTableName1:False}
dataTableNamesList = [dataTableName1]
dataTableColumnsDict = {dataTableName1:("column1")}
globalPath = "./dataTables/"



def tableSaveTable(dataTableName):
    dfTableDict[dataTableName].to_csv(globalPath+dataTableName+".csv")
def tableGetDataTables(dataTableNames):
    for dataTableName in dataTableNames:
        df = pd.read_csv(dataTableName)
        dfTableDict[dataTableName] = df
def tableInsertCustomindex(tableName,newRows):
    tempRows = [newRows[i][1:] for i in range(len(newRows))]
    tempCustomIndex = [newRows[i][0] for i in range(len(newRows))]
    tempDF = pd.DataFrame(tempRows,columns = dfTableDict[tableName].columns,index=tempCustomIndex)
    dfTableDict[tableName] = pd.concat([dfTableDict[tableName],tempDF])
def tableInsertEntry(tableName,newRows):
    id = dfTableDict[tableName].shape[0]
    tempCustomIndex = [i+id for i in range(len(newRows))]
    tempDF = pd.DataFrame(newRows, columns=dfTableDict[tableName].columns, index=tempCustomIndex)
    dfTableDict[tableName] = pd.concat([dfTableDict[tableName], tempDF])
def tableTestValidity(functionName, tableName):
    if not tableName in dfTableDict:
        raise ("invalid table name query to ",functionName, "! invalid name:", tableName )
def tableQuery(tableName,queires, returnColValue="id"):
    '''
    :param tableName:
    :param queires:
    :param returnColValue:
    :return: return the columns specified by returnColValue
             for rows that satisfy queires
    '''
    tableTestValidity("tableQueryByColumnValue",tableName)
    entries = dfTableDict[tableName].query(" & ".join(queires))
    if returnColValue == "id": return entries.index
    else: return entries[returnColValue]
def tableQueryByRowId(tableName,rowID, colName=None):
    '''
    :param tableName:
    :param rowID:
    :param colName:
    :return: column values of rowID from tableName.
            If column name is none, then return the entire row
    '''
    tableTestValidity("tableQueryByRowId",tableName)
    entries = dfTableDict[tableName].iloc[rowID]
    if colName == None: return entries
    else: return entries[colName]
def tableGetDataTable():
    '''
    Load data tables to a dictionary dfTableDict
    :return:
    '''
    global dataTableNames,dfTableDict
    for dataTableName in dataTableNamesList:
        dfTableDict[dataTableName] = pd.read_csv(globalPath+dataTableName+".csv",index_col=0)
def tableBuildDataTableCSV():
    '''
    build a panda data table
    :param dataTableName:
    :param dataTableColumns:
    :return: None
    :side effect: create CSV files according to data table name with columns specified by dataTableColumns
    '''
    global dataTableNames, dfTableDict, dataTableColumnsDict
    for dataTableName in dataTableNamesList:
        if dataTableIsCustomIndex[dataTableName]:
            dfTableDict[dataTableName] = pd.DataFrame([],columns = dataTableColumnsDict[dataTableName][1:])
        else: dfTableDict[dataTableName] = pd.DataFrame([],columns = dataTableColumnsDict[dataTableName])
        tableSaveTable(dataTableName)

tableGetDataTable()
