from typing import List, Set, Dict, Tuple
import csv
import os
import json
import time
import datetime

Table = List[str]
Index = Dict[str, List[int]]
Fuzzy = Dict[str, List[str]]

ROOT_PATH = "C:/Arcology/AeonDB"
TABLE_DIR = "C:/Arcology/AeonDB/%s"
TABLE_PATH = "C:/Arcology/AeonDB/%s/table.txt"
INDEX_PATH = "C:/Arcology/AeonDB/%s/index.txt"
FUZZY_PATH = "C:/Arcology/AeonDB/%s/fuzzy.txt"
FUZZY2_PATH = "C:/Arcology/AeonDB/%s/fuzzy2.txt"

g_tables: Dict[str, Table] = dict()
g_indices: Dict[str, Index] = dict()
g_fuzzyDict: Dict[str, Fuzzy] = dict()
g_fuzzyDict2: Dict[str, Fuzzy] = dict()

def readTable(tableName: str) -> Table:
    os.makedirs(TABLE_DIR % tableName, exist_ok=True)
    return json.load(open(TABLE_PATH % tableName))

def writeTable(tableName: str, table: Table) -> None:
    os.makedirs(TABLE_DIR % tableName, exist_ok=True)
    json.dump(table, open(TABLE_PATH % tableName, 'w+'))
    return None

def readIndex(tableName: str) -> Index:
    os.makedirs(TABLE_DIR % tableName, exist_ok=True)
    return json.load(open(INDEX_PATH % tableName))

def writeIndex(tableName: str, index: Index) -> None:
    os.makedirs(TABLE_DIR % tableName, exist_ok=True)
    json.dump(index, open(INDEX_PATH % tableName, 'w+'))
    return None

def readFuzzy(tableName: str) -> Fuzzy:
    os.makedirs(TABLE_DIR % tableName, exist_ok=True)
    return json.load(open(FUZZY_PATH % tableName))

def writeFuzzy(tableName: str, fuzzy: Fuzzy) -> None:
    os.makedirs(TABLE_DIR % tableName, exist_ok=True)
    json.dump(fuzzy, open(FUZZY_PATH % tableName, 'w+'))
    return None

def readFuzzy2(tableName: str) -> Fuzzy:
    os.makedirs(TABLE_DIR % tableName, exist_ok=True)
    return json.load(open(FUZZY2_PATH % tableName))

def writeFuzzy2(tableName: str, fuzzy: Fuzzy) -> None:
    os.makedirs(TABLE_DIR % tableName, exist_ok=True)
    json.dump(fuzzy, open(FUZZY2_PATH % tableName, 'w+'))
    return None

def listTables() -> List[str]:
    os.makedirs(ROOT_PATH, exist_ok=True)
    return os.listdir(ROOT_PATH)

def timestamp() -> str:
    return datetime.datetime.fromtimestamp(time.time()).strftime("%m/%d/%Y %H:%M:%S")

g_cmdHelpMap = {
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
    for help in g_cmdHelpMap.values():
        print(help)
    return

def toBigrams(s: str) -> Set[str]:
    ngrams = set()
    if len(s) < 2:
        ngrams.add(s)
        return ngrams
    for i in range(len(s) - 1):
        ngrams.add(s[i:i+2])
    return ngrams

def dicesCoefficient(a: Set[str], b: Set[str]) -> float:
    return float(2 * len(a.intersection(b))) / float(len(a) + len(b))

def preprocess(s: str) -> str:
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

def createIndex(table: Table) -> Tuple[Index, Fuzzy, Fuzzy]:
    startTime = time.time()
    index: Index = dict()
    fuzzy1: Fuzzy = dict()
    fuzzy2: Fuzzy = dict()
    fuzzy3: Dict[str, Set[str]] = dict()
    for rowId in range(len(table)):
        row = table[rowId]
        row = preprocess(row).lower()
        terms = set(row.split())
        if "" in terms:
            terms.remove("")
        for term in terms:
            if term not in index:
                index.update({term: list()})
            rowIds = index.get(term)
            if rowId not in rowIds:
                rowIds.append(rowId)
            if term not in fuzzy3:
                atLeastOneBigram = set()
                bigrams = toBigrams(term)
                fuzzy3.update({term: bigrams})
                for bigram in bigrams:
                    if bigram not in fuzzy2:
                        fuzzy2.update({bigram: list()})
                    bigramList = fuzzy2.get(bigram)
                    bigramList.append(term)
                    atLeastOneBigram.update(bigramList)
                related = list()
                for term2 in atLeastOneBigram:
                    if term == term2:
                        related.append(term2)
                    elif dicesCoefficient(fuzzy3.get(term), fuzzy3.get(term2)) > 0.6:
                        related.append(term2)
                        fuzzy1.get(term2).append(term)
                fuzzy1.update({term: related})
        print("Indexed row %d of %d." % (rowId, len(table)))
    print("Indexing Time: " + str(time.time() - startTime))
    return index, fuzzy1, fuzzy2

def importCsv(filename: str) -> Table:
    table = [" ".join(row) for row in csv.reader(open(filename))]
    table.pop(0)
    return table

def expandQuery(term: str, index: Index, fuzzy: Fuzzy, fuzzy2: Fuzzy) -> Set[int]:
    rowIds = set()
    relateds = set()
    if term not in fuzzy:
        possiblyRelateds = set()
        bigrams = toBigrams(term)
        for bigram in bigrams:
            if bigram in fuzzy2:
                possiblyRelateds.update(fuzzy2.get(bigram))
        for pRelated in possiblyRelateds:
            if dicesCoefficient(toBigrams(pRelated), bigrams) > 0.6:
                relateds.add(pRelated)
    else:
        relateds = fuzzy.get(term)
    for related in relateds:
        rowIds.update(index.get(related))
    return rowIds

def find(keyTerms: Set[str], table: Table, index: Index, fuzzy: Fuzzy, fuzzy2: Fuzzy, isFuzzy: bool) -> Table:
    lowKeyTerms = {term.lower() for term in keyTerms}
    rowIds = set()
    results = list()
    first = lowKeyTerms.pop()
    if isFuzzy:
        rowIds.update(expandQuery(first, index, fuzzy, fuzzy2))
    elif first in index:
        rowIds.update(index.get(first))
    else:
        return results
    for word in lowKeyTerms:
        if isFuzzy:
            rowIds.intersection_update(expandQuery(word, index, fuzzy, fuzzy2))
        elif word in index:
            rowIds.intersection_update(index.get(word))
        else:
            return results
    for i in rowIds:
        results.append(table[i])
    return results

def loadAllTables() -> None:
    tableNames = listTables()
    for tableName in tableNames:
        print("%s Log.info: Table %s: Backup volume offline. Waiting for new volume." % (timestamp(), tableName))

        try:
            table = readTable(tableName)
            g_tables.update({tableName: table})
            print("%s Log.info: Table %s: Recovered %d rows." % (timestamp(), tableName, len(table)))
        except OSError:
            print("%s Log.info: Table %s: Could not read file." % (timestamp(), tableName))
        except json.JSONDecodeError:
            print("%s Log.info: Table %s: File is corrupted." % (timestamp(), tableName))

        try:
            index = readIndex(tableName)
            g_indices.update({tableName: index})
            print("%s Log.info: Index %s: Recovered %d terms." % (timestamp(), tableName, len(index)))
        except OSError:
            print("%s Log.info: Index %s: Could not read file." % (timestamp(), tableName))
        except json.JSONDecodeError:
            print("%s Log.info: Index %s: File is corrupted." % (timestamp(), tableName))

        try:
            fuzzy = readFuzzy(tableName)
            g_fuzzyDict.update({tableName: fuzzy})
            print("%s Log.info: Fuzzy %s: Recovered %d terms." % (timestamp(), tableName, len(fuzzy)))
        except OSError:
            print("%s Log.info: Fuzzy %s: Could not read file." % (timestamp(), tableName))
        except json.JSONDecodeError:
            print("%s Log.info: Fuzzy %s: File is corrupted." % (timestamp(), tableName))

        try:
            fuzzy2 = readFuzzy2(tableName)
            g_fuzzyDict2.update({tableName: fuzzy2})
            print("%s Log.info: Fuzzy2 %s: Recovered %d terms." % (timestamp(), tableName, len(fuzzy2)))
        except OSError:
            print("%s Log.info: Fuzzy2 %s: Could not read file." % (timestamp(), tableName))
        except json.JSONDecodeError:
            print("%s Log.info: Fuzzy2 %s: File is corrupted." % (timestamp(), tableName))

    print("AeonDB ready. %d tables available." % len(tableNames))
    return None

def prompt() -> List[str]:
    args = input(" : ").split()
    args[0] = args[0].lower()
    return args

def main() -> None:
    print("%s AeonDB 1.0 beta 65" % timestamp())
    print(u"%s Copyright © 2011-2018 by Kronosaur Productions LLC. All Rights Reserved." % timestamp())
    loadAllTables()
    args = prompt()
    while args[0] != "quit":
        # createtable
        if args[0] == "createtable":
            if len(args) < 2:
                print(g_cmdHelpMap.get(args[0]))
            else:
                print("Not implemented for demo.")
        # getrows
        elif args[0] == "getrows":
            if len(args) < 4:
                print(g_cmdHelpMap.get(args[0]))
            else:
                print("Not implemented for demo.")
        # importtable
        elif args[0] == "importtable":
            if len(args) < 3:
                print(g_cmdHelpMap.get(args[0]))
            else:
                csvName = args[2]
                csvName = csvName.replace('"', "")
                csvName = csvName.replace("'", "")
                csvName = csvName.replace("/", "\\")
                try:
                    tableObj = importCsv(csvName)
                    print("Imported %d rows to table %s." % (len(tableObj), args[1]))
                    g_tables.update({args[1] : tableObj})
                    print("Saving table %s to file." % args[1])
                    writeTable(args[1], tableObj)
                except:
                    print("Failed to import table. Check URI.")
        # listtables
        elif args[0] == "listtables":
            if len(args) < 1:
                print(g_cmdHelpMap.get(args[0]))
            else:
                for x in listTables():
                    print(x)
        # indextable
        elif args[0] == "indextable":
            if len(args) < 2:
                print(g_cmdHelpMap.get(args[0]))
            else:
                if args[1] in g_tables:
                    tableIndex, tableFuzzy1, tableFuzzy2 = createIndex(g_tables.get(args[1]))
                    g_indices.update({args[1] : tableIndex})
                    g_fuzzyDict.update({args[1] : tableFuzzy1})
                    g_fuzzyDict2.update({args[1] : tableFuzzy2})
                    try:
                        print("Saving index %s." % args[1])
                        writeIndex(args[1], tableIndex)
                        print("Saving fuzzy %s." % args[1])
                        writeFuzzy(args[1], tableFuzzy1)
                        print("Saving fuzzy2 %s." % args[1])
                        writeFuzzy2(args[1], tableFuzzy2)
                    except:
                        print("Failed to write index to file.")
                else:
                    print("Table %s does not exist." % args[1])
        # find
        elif args[0] == "find":
            if len(args) < 3:
                print(g_cmdHelpMap.get(args[0]))
            else:
                if args[1] not in g_tables:
                    print("Table %s does not exist." % args[1])
                elif args[1] not in g_indices:
                    print("Index %s does not exist." % args[1])
                elif args[1] not in g_fuzzyDict:
                    print("Fuzzy1 %s does not exist." % args[1])
                elif args[1] not in g_fuzzyDict2:
                    print("Fuzzy2 %s does not exist." % args[1])
                else:
                    results = find(set(args[2:]), g_tables.get(args[1]), g_indices.get(args[1]), g_fuzzyDict.get(args[1]), g_fuzzyDict2.get(args[1]), False)
                    for row in results:
                        print(row)
                    print("Found %d rows." % len(results))
        # fuzzysearch
        elif args[0] == "fuzzysearch":
            if len(args) < 3:
                print(g_cmdHelpMap.get(args[0]))
            else:
                if args[1] not in g_tables:
                    print("Table %s does not exist." % args[1])
                elif args[1] not in g_indices:
                    print("Index %s does not exist." % args[1])
                elif args[1] not in g_fuzzyDict:
                    print("Fuzzy1 %s does not exist." % args[1])
                elif args[1] not in g_fuzzyDict2:
                    print("Fuzzy2 %s does not exist." % args[1])
                else:
                    results = find(set(args[2:]), g_tables.get(args[1]), g_indices.get(args[1]), g_fuzzyDict.get(args[1]), g_fuzzyDict2.get(args[1]), True)
                    for row in results:
                        print(row)
                    print("Found %d rows." % len(results))
        # Bad commands
        else:
            printHelp()
        # Next loop
        args = prompt()
    return None

main()