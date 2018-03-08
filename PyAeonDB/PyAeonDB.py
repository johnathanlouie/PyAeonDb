from collections import defaultdict
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
    "buildfuzzy"  : "buildFuzzy {tableName}",
    "createfuzzy" : "createFuzzy {tableName}",
    "createtable" : "createTable {tableDesc}",
    "getrows"     : "getRows {tableName} {key} {count}",
    "importtable" : "importTable {tableName} {CSV filespec}",
    "listtables"  : "listTables",
    "indextable"  : "indexTable {tableName}",
    "find"        : "find {tableName} {term1 term2 term3...}",
    "fuzzysearch" : "fuzzySearch {tableName} {term1 term2 term3...}",
    "fuzzyfind"   : "fuzzyFind {tableName} {term1 term2 term3...} {number_of_results_to_return}",
    "quit"        : "quit"
    }

def printHelp() -> None:
    for help in g_cmdHelpMap.values():
        print(help)
    return

def bigrams(s: str) -> Set[str]:
    ngrams = set()
    if len(s) < 2:
        ngrams.add(s)
        return ngrams
    for i in range(len(s) - 1):
        ngrams.add(s[i:i+2])
    return ngrams

def mapNgrams(index: Index) -> Dict[str, Set[str]]:
    ngrams = dict()
    for term in index.keys():
        ngrams.update({term : bigrams(term)})
    return ngrams

def dicesCoefficient(str1: str, str2: str, map: Dict[str, Set[str]]) -> float:
    a = map.get(str1)
    if a == None:
        a = bigrams(str1)
    b = map.get(str2)
    if b == None:
        b = bigrams(str2)
    c = a.intersection(b)
    sim = float(2 * len(c)) / float(len(a) + len(b))
    print("===============================================================")
    print(str1 + " " + str(a))
    print(str2 + " " + str(b))
    print("shared: " + str(c))
    print("sim: " + str(sim))
    return sim

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

def createIndex(table: Table) -> Index:
    index = dict()
    for rowId in range(len(table)):
        row = table[rowId]
        row = preprocess(row).lower()
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
        print("Indexed row %d of %d." % (rowId, len(table)))
    return index

#def buildFuzzy(index: Index) -> Fuzzy:
#    termBigrams = mapNgrams(index)
#    uniqueBigrams = set()
#    for termBigram in termBigrams.values():
#        uniqueBigrams.update(termBigram)
#    fuzzy = dict()
#    for bigram in uniqueBigrams:
#        fuzzy.update({bigram : list()})
#    for term, bigramSet in termBigrams.items():
#        for b in bigramSet:
#            fuzzy.get(b).append(term)
#    return fuzzy

def buildFuzzy(indexObj:Dict[str, List[int]]) -> Dict[str, List[str]]:
    fuzzy = dict()
    for key, value in indexObj.items():
        #This is a dictionary so indexing n-grams on value is wasteful - LOTS OF STRINGS -- going to do it anyways
        ngrams = bigrams(key)
        for ngram in ngrams:
            ngram_tokens = fuzzy.get(ngram)
            if ngram_tokens == None:
                ngram_tokens = set()
            ngram_tokens = set(ngram_tokens)
            for index_ngram_relation_value in value:
                ngram_tokens.add(index_ngram_relation_value)
            fuzzy.update({ngram: list(ngram_tokens)})
        print("Created fuzzy mapping for %s" % key)
    return fuzzy

def fuzzyFind(tableObj:List[str], indexObj:Dict[str, List[int]], fuzzyObj:Dict[str, List[int]], keyTerms:List[str],nClosestMatches:int=20) -> List[str]:
    rowIds = set()
    results = list()
    ngrams_list = list()
    for word in keyTerms:
        ngrams = bigrams(word)
        for ngram in ngrams:
            ngrams_list.append(ngram)
    votes_dict = dict()
    for ngram in ngrams_list:
        ngramRowMatches = fuzzyObj[ngram]
        if ngramRowMatches == None:
            continue
        for ngramRowMatch in ngramRowMatches:
            current_votes = votes_dict.get(ngramRowMatch)
            if current_votes == None:
                current_votes = 0
            current_votes = int(current_votes)
            current_votes +=1
            votes_dict[ngramRowMatch] = current_votes
    sorted_votes = sorted(votes_dict.items(),key=lambda x:x[1],reverse=True)
    resultsAdded = 0
    for highest_voted_result in sorted_votes:
        results.append(tableObj[highest_voted_result[0]])
        resultsAdded += 1
        if resultsAdded >= nClosestMatches:
            break
    return results

def createFuzzy(index: Index) -> Fuzzy:
    fuzzy = dict()
    map = mapNgrams(index)
    terms = list(index.keys())
    i = 1
    total = 0
    counter = len(terms)
    while counter > 1:
        total += counter - 1
        counter -= 1
    for term in terms:
        fuzzy.update({term : list()})
    for x in range(len(terms)):
        token1 = terms[x]
        related1 = fuzzy.get(token1)
        related1.append(token1)
        for y in range(x + 1, len(terms)):
            token2 = terms[y]
            print("Progress: " + str(i) + " of " + str(total))
            if dicesCoefficient(token1, token2, map) > 0.6:
                related2 = fuzzy.get(token2)
                related1.append(token2)
                related2.append(token1)
            i += 1
    return fuzzy

def importCsv(filename: str) -> Table:
    table = [" ".join(row) for row in csv.reader(open(filename))]
    table.pop(0)
    return table

def expandQuery(query: Set[str], fuzzy: Fuzzy) -> Set[str]:
    expandedQuery = set()
    for word in query:
        related = fuzzy.get(word)
        if related != None:
            expandedQuery.update(related)
    return expandedQuery

def find(table: Table, index: Index, keyTerms: Set[str]) -> Table:
    rowIds = set()
    results = list()
    first = True
    for word in keyTerms:
        word = word.lower()
        termRowIds = index.get(word)
        if termRowIds == None and first:
            rowIds = set()
            first = False
        elif termRowIds != None and first:
            rowIds = set(termRowIds)
            first = False
        elif termRowIds != None:
            rowIds.intersection_update(termRowIds)
    for i in rowIds:
        results.append(table[i])
    return results

g_tables = defaultdict(list)
g_indices = dict()
g_fuzzyDict = dict()
g_fuzzyDict2 = dict()

def loadAllTables() -> None:
    tableNames = listTables()
    for tableName in tableNames:
        print("%s Log.info: Table %s: Backup volume offline. Waiting for new volume." % (timestamp(), tableName))

        try:
            table = readTable(tableName)
            g_tables.update({tableName : table})
            print("%s Log.info: Table %s: Recovered %d rows." % (timestamp(), tableName, len(table)))
        except OSError:
            print("%s Log.info: Table %s: Could not read file." % (timestamp(), tableName))
        except json.JSONDecodeError:
            print("%s Log.info: Table %s: File is corrupted." % (timestamp(), tableName))

        try:
            index = readIndex(tableName)
            g_indices.update({tableName : index})
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
                    tableIndex = createIndex(g_tables.get(args[1]))
                    g_indices.update({args[1] : tableIndex})
                    try:
                        print("Saving index %s." % args[1])
                        writeIndex(args[1], tableIndex)
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
                else:
                    results = find(g_tables.get(args[1]), g_indices.get(args[1]), set(args[2:]))
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
                    print("Fuzzy Dictionary %s does not exist." % args[1])
                else:
                    expanded = expandQuery(set(args[2:]), g_fuzzyDict.get(args[1]))
                    results = find(g_tables.get(args[1]), g_indices.get(args[1]), expanded)
                    for row in results:
                        print(row)
                    print("Found %d rows." % len(results))
        # createfuzzy
        elif args[0] == "createfuzzy":
            if len(args) < 2:
                print(g_cmdHelpMap.get(args[0]))
            else:
                if args[1] not in g_indices:
                    print("Index %s does not exist." % args[1])
                else:
                    tableFuzzy = createFuzzy(g_indices.get(args[1]))
                    g_fuzzyDict.update({args[1] : tableFuzzy})
                    try:
                        print("Saving fuzzy dictionary %s." % args[1])
                        writeFuzzy(args[1], tableFuzzy)
                    except:
                        print("Failed to write fuzzy dictionary to file.")
        # fuzzyfind
        elif args[0] == "fuzzyfind":
            if len(args) < 3:
                print(g_cmdHelpMap.get(args[0]))
            else:
                if args[1] not in g_tables:
                    print("Table %s does not exist." % args[1])
                elif args[1] not in g_indices:
                    print("Index %s does not exist." % args[1])
                elif args[1] not in g_fuzzyDict2:
                    print("Fuzzy Dictionary %s does not exist." % args[1])
                else:
                    table = g_tables.get(args[1])
                    index = g_indices.get(args[1])
                    fuzzy = g_fuzzyDict2.get(args[1])
                    results = fuzzyFind(table, index, fuzzy, args[2:-1], int(args[-1]))
                    for row in results:
                        print(row)
                    print("Found %d rows." % len(results))
        # buildfuzzy
        elif args[0] == "buildfuzzy":
            if len(args) < 2:
                print(g_cmdHelpMap.get(args[0]))
            else:
                if args[1] not in g_indices:
                    print("Index %s does not exist." % args[1])
                else:
                    tableFuzzy = buildFuzzy(g_indices.get(args[1]))
                    g_fuzzyDict2.update({args[1] : tableFuzzy})
                    try:
                        print("Saving fuzzy dictionary %s." % args[1])
                        writeFuzzy2(args[1], tableFuzzy)
                    except:
                        print("Failed to write fuzzy dictionary to file.")
        # Bad commands
        else:
            printHelp()
        # Next loop
        args = prompt()
    return None

main()