from collections import defaultdict
from typing import List, Set, Dict, Tuple
import csv
import os
import json
import time
import datetime

def readTable(tableName:str) -> List[str]:
    filename = "C:/Arcology/AeonDB/%s/table.txt" % tableName
    if not os.path.isfile(filename):
        print("Table does not exist.")
        return
    return json.load(open(filename))

def writeTable(tableName:str, data:List[str]) -> None:
    filename = "C:/Arcology/AeonDB/%s/table.txt" % tableName
    dir = "C:/Arcology/AeonDB/%s/" % tableName
    if not os.path.exists(dir):
        os.makedirs(dir)
    json.dump(data, open(filename, 'w+'))
    return

def readIndex(tableName:str) -> Dict[str, List[int]]:
    filename = "C:/Arcology/AeonDB/%s/index.txt" % tableName
    if not os.path.isfile(filename):
        print("Index does not exist.")
        return
    return json.load(open(filename))

def writeIndex(tableName:str, data:Dict[str, List[int]]) -> None:
    filename = "C:/Arcology/AeonDB/%s/index.txt" % tableName
    dir = "C:/Arcology/AeonDB/%s/" % tableName
    if not os.path.exists(dir):
        os.makedirs(dir)
    json.dump(data, open(filename, 'w+'))
    return

def readFuzzy(tableName:str) -> Dict[str, List[str]]:
    filename = "C:/Arcology/AeonDB/%s/fuzzy.txt" % tableName
    if not os.path.isfile(filename):
        print("Fuzzy dictionary does not exist.")
        return
    return json.load(open(filename))

def writeFuzzy(tableName:str, data:Dict[str, List[str]]) -> None:
    filename = "C:/Arcology/AeonDB/%s/fuzzy.txt" % tableName
    dir = "C:/Arcology/AeonDB/%s/" % tableName
    if not os.path.exists(dir):
        os.makedirs(dir)
    json.dump(data, open(filename, 'w+'))
    return

def listTables() -> List[str]:
    filename = "C:/Arcology/AeonDB/"
    if not os.path.exists(filename):
        os.makedirs(filename)
    return os.listdir(filename)

def timestamp() -> str:
    return datetime.datetime.fromtimestamp(time.time()).strftime("%m/%d/%Y %H:%M:%S")

cmdHelpMap = {
    "buildfuzzy"  : "buildFuzzy {tableName}",
    "createtable" : "createTable {tableDesc}",
    "getrows"     : "getRows {tableName} {key} {count}",
    "importtable" : "importTable {tableName} {CSV filespec}",
    "listtables"  : "listTables",
    "indextable"  : "indexTable {tableName}",
    "find"        : "find {tableName} {term1 term2 term3...}",
    "fuzzysearch" : "fuzzySearch {tableName} {term1 term2 term3...}",
    "quit"        : "quit"
    }

def printHelp() -> None:
    for help in cmdHelpMap.values():
        print(help)
    return

def bigrams(s:str) -> Set[str]:
    ngrams = set()
    if len(s) < 2:
        ngrams.add(s)
        return ngrams
    for i in range(len(s) - 1):
        sub = s[i:i+2]
        ngrams.add(sub)
    return ngrams

def mapNgrams(indexObj:Dict[str, List[int]]) -> Dict[str, Set[str]]:
    ngrams = dict()
    for term in indexObj.keys():
        ngrams.update({term : bigrams(term)})
    return ngrams

def dicesCoefficient(str1:str, str2:str, map:Dict[str, Set[str]]) -> float:
    a = map.get(str1)
    b = map.get(str2)
    c = a.intersection(b)
    sim = float(2 * len(c)) / float(len(a) + len(b))
    print("===============================================================")
    print(str1 + " " + str(a))
    print(str2 + " " + str(b))
    print("shared: " + str(c))
    print("sim: " + str(sim))
    return sim

def removeSymbols(s:str) -> str:
    s = s.replace("~", " ")
    s = s.replace("`", " ")
    s = s.replace("!", " ")
    s = s.replace("@", " ")
    s = s.replace("#", " ")
    s = s.replace("$", " ")
    s = s.replace("%", " ")
    s = s.replace("^", " ")
    s = s.replace("&", " ")
    s = s.replace("*", " ")
    s = s.replace("(", " ")
    s = s.replace(")", " ")
    s = s.replace("-", " ")
    s = s.replace("_", " ")
    s = s.replace("+", " ")
    s = s.replace("=", " ")
    s = s.replace("{", " ")
    s = s.replace("}", " ")
    s = s.replace("[", " ")
    s = s.replace("]", " ")
    s = s.replace("|", " ")
    s = s.replace("\\", " ")
    s = s.replace(";", " ")
    s = s.replace(":", " ")
    s = s.replace('"', " ")
    s = s.replace("'", " ")
    s = s.replace("<", " ")
    s = s.replace(">", " ")
    s = s.replace(",", " ")
    s = s.replace(".", " ")
    s = s.replace("/", " ")
    s = s.replace("?", " ")
    s = s.replace("1", " ")
    s = s.replace("2", " ")
    s = s.replace("3", " ")
    s = s.replace("4", " ")
    s = s.replace("5", " ")
    s = s.replace("6", " ")
    s = s.replace("7", " ")
    s = s.replace("8", " ")
    s = s.replace("9", " ")
    s = s.replace("0", " ")
    return s

def indexTable(tableObj:List[str]) -> Dict[str, List[int]]:
    index = dict()
    for rowId in range(len(tableObj)):
        row = tableObj[rowId]
        row = removeSymbols(row).lower()
        tokens = set(row.split())
        if "" in tokens:
            tokens.remove("")
        for token in tokens:
            rowIds = index.get(token)
            if rowIds == None:
                rowIds = set()
            rowIds = set(rowIds)
            rowIds.add(rowId)
            index.update({token: list(rowIds)})
        print("Indexed row %d." % rowId)
    return index

def buildFuzzy(indexObj:Dict[str, List[int]]) -> Dict[str, List[str]]:
    fuzzy = dict()
    map = mapNgrams(indexObj)
    terms = list(indexObj.keys())
    i = 1
    for token1 in terms:
        related = set()
        for token2 in terms:
            print("Progress: " + str(i) + " of " + str(len(terms)**2))
            if dicesCoefficient(token1, token2, map) > 0.7:
                related.add(token2)
            i += 1
        fuzzy.update({token1: list(related)})
    return fuzzy

def importCsv(filename:str) -> List[str]:
    if not os.path.exists(filename):
        print("CSV does not exist.")
        return
    a = list()
    with open(filename) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            print(row)
            a.append(row[0])
    return a

def expandQuery(query:List[str], fuzzyObj:Dict[str, List[str]]) -> List[str]:
    expandedQuery = set()
    for word in query:
        related = fuzzyObj.get(word)
        expandedQuery = expandedQuery.union(related)
    return List(expandedQuery)

def find(tableObj:List[str], indexObj:Dict[str, List[int]], keyTerms:List[str]) -> List[str]:
    rowIds = set()
    results = list()
    for word in keyTerms:
        rowIds = rowIds.union(indexObj.get(word))
    for i in rowIds:
        results.append(tableObj[i])
    return results


tableDb = defaultdict(list)
indices = dict()
fuzzyDict = dict()

def loadAllTables() -> None:
    tableNames = listTables()
    for tableName in tableNames:
        print("%s Log.info: Table %s: Backup volume offline. Waiting for new volume." % (timestamp(), tableName))

        tableObj = readTable(tableName)
        if tableObj != None:
            tableDb.update({tableName: tableObj})
            print("%s Log.info: Table %s: Recovered %d rows." % (timestamp(), tableName, len(tableObj)))

        indexObj = readIndex(tableName)
        if indexObj != None:
            indices.update({tableName: indexObj})

        fuzzyObj = readFuzzy(tableName)
        if fuzzyObj != None:
            fuzzyDict.update({tableName: fuzzyObj})

    print("AeonDB ready. %d tables available." % len(tableNames))
    return

def main():
    print("%s AeonDB 1.0 beta 65" % timestamp())
    print("%s Copyright © 2011-2018 by Kronosaur Productions LLC. All Rights Reserved." % timestamp())
    loadAllTables()
    args = input(" : ").split()
    args[0] = args[0].lower()
    while args[0] != "quit":
        if args[0] == "createtable":
            if len(args) < 2:
                print(cmdHelpMap.get(args[0]))
            else:
                print("Not implemented for demo.")
        elif args[0] == "getrows":
            if len(args) < 4:
                print(cmdHelpMap.get(args[0]))
            else:
                print("Not implemented for demo.")
        elif args[0] == "importtable":
            if len(args) < 3:
                print(cmdHelpMap.get(args[0]))
            else:
                csvName = args[2]
                csvName = csvName.replace('"', "")
                csvName = csvName.replace("'", "")
                csvName = csvName.replace("/", "\\")
                tableObj = importCsv(csvName)
                if tableObj != None:
                    print("Imported %d rows to table %s." % (len(tableObj), args[1]))
                    tableDb.update({args[1]: tableObj})
                    writeTable(args[1], tableObj)
        elif args[0] == "listtables":
            if len(args) < 1:
                print(cmdHelpMap.get(args[0]))
            else:
                for x in listTables():
                    print(x)
        elif args[0] == "indextable":
            if len(args) < 2:
                print(cmdHelpMap.get(args[0]))
            else:
                if args[1] in tableDb:
                    tableIndex = indexTable(tableDb.get(args[1]))
                    indices.update({args[1]: tableIndex})
                    writeIndex(args[1], tableIndex);
                else:
                    print("Table %s does not exist." % args[1])
        elif args[0] == "find":
            if len(args) < 3:
                print(cmdHelpMap.get(args[0]))
            else:
                if args[1] not in tableDb:
                    print("Table %s does not exist." % args[1])
                else:
                    tableObj = tableDb.get(args[1])
                    indexObj = indices.get(args[1])
                    if indexObj == None:
                        print("Build index for table %s first." % args[1])
                        continue
                    results = find(tableObj, indexObj, args[2:])
                    for row in results:
                        print(row)
        elif args[0] == "fuzzysearch":
            if len(args) < 3:
                print(cmdHelpMap.get(args[0]))
            else:
                if args[1] not in tableDb:
                    print("Table %s does not exist." % args[1])
                else:
                    tableObj = tableDb.get(args[1])
                    indexObj = indices.get(args[1])
                    if indexObj == None:
                        print("Build index for table %s first." % args[1])
                        continue
                    fuzzyObj = fuzzyDict.get(args[1])
                    if fuzzyObj == None:
                        print("Build fuzzy dictionary for table %s first." % args[1])
                        continue
                    something = expandQuery(args[2:], fuzzyObj)
                    results = find(tableObj, indexObj, something)
                    for row in results:
                        print(row)
        elif args[0] == "buildfuzzy":
            if len(args) < 2:
                print(cmdHelpMap.get(args[0]))
            else:
                if args[1] not in tableDb:
                    print("Table %s does not exist." % args[1])
                else:
                    tableFuzzy = buildFuzzy(indices.get(args[1]))
                    fuzzyDict.update({args[1]: tableFuzzy})
                    writeFuzzy(args[1], tableFuzzy);
        else:
            printHelp()
        args = input(" : ").split()
        args[0] = args[0].lower()
    return

main()