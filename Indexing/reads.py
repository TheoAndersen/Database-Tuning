#!/usr/bin/python
# encoding: utf-8
"""
DB2/LogIO/experiment.py

A database must exist and be initialized before the experiment can be run 
(table and index creation in init.sql), data should be loaded (load.sql), 
and the database should be cleant up afterwards (cleanup.sql).

The database parameters are obtained from ../db2.py

Copyright (c) Philippe Bonnet 2010 . All rights reserved.
"""

import sys
import getopt
import timeit
import random
import os
import re
import ibm_db
import time
from string import maketrans

### Experiment parameters (default values)
NBRUNS         = 1    # Number of runs 
QUERY_FILE_PATH = "./query_range.sql"  # Complete filename (including path) of query file 
NBQUERIES       = 1                 # Nb of queries per run
NBTUPLES       = 1000000
SPECFILE       = 'employeesspec'
NBKEYS         = 1
ATTLIST         = []

### Database parameters (DATABASE; HOSTNAME; PORT; USERNAME; PASSWORD)
sys.path.append("..")
from db2 import DATABASE
from db2 import HOSTNAME
from db2 import PORT
from db2 import USERNAME
from db2 import PASSWORD

### Timed function parameter
query_str = None
g = None
    
def experiment(query_str,g):
    # generate nb of parameters for query
    matchList  = re.findall('\?', query_str)
    nbParams   = len(matchList)
    if (len(ATTLIST) != nbParams): raise Usage("Attribute missing (add appropriate -a option)")
    # Connect to DB
    conn = ibm_db.pconnect('DRIVER={IBM DB2 ODBC DRIVER};DATABASE='+DATABASE+';HOSTNAME='+HOSTNAME+';PORT='+str(PORT)+'; PROTOCOL=TCPIP;UID='+USERNAME+';PWD='+PASSWORD+';','','')
    if conn is None: raise Usage(ibm_db.conn_errormsg())
    # Prepare statement
    query_stmt   = ibm_db.prepare(conn, query_str)
    if (query_stmt == False): raise Usage("Failed to prepare query")
    # Execute statement
    for i in range(NBQUERIES): 
        if (nbParams == 0): 
            if ibm_db.execute(query_stmt) == False:
                raise Usage("Failed to execute the query")
        else:
            t = g.getWrite(i)
            l = list(t)
            u = [l[j] for j in range(len(l)) if j in ATTLIST]
            if ibm_db.execute(query_stmt, tuple(u)) == False:
                raise Usage("Failed to execute the query") 
        nbtuples = 0
        while (ibm_db.fetch_tuple(query_stmt) != False):
            nbtuples += 1
        print "Query"+str(i)+": "+str(nbtuples)+" fetched."
    # Disconnect from DB
    status = ibm_db.close(conn)
    if status == False: raise Usage("Failed to close db connection.\n") 
 

"""
Gentable class
"""
class GenWrites(object):
    def __init__(self, numrows, numkeys, numwrites, specfile):
        self.numrows   = numrows
        self.numkeys   = numkeys
        self.numwrites = numwrites
        # Extract columns specifications from specfile
        print "reading specfile..."
        f = open(specfile)
        filelines = f.readlines()
        alllines = [(l.strip()).translate(maketrans("",""),"\n") for l in filelines if len(l.translate(maketrans("",""),"\n")) >1] # remove empty lines
        lines = [ll for ll in [l for l in alllines if l[0]!='/'] if ll[0]!=' ']   # remove comments
        colspecs = [l.split(' ') for l in lines]
        numcols = len(colspecs) 
        # Generate Table based on columns specifications
        print "generating rows..."
        self.writes = self.gentable(numcols, colspecs)
        self.counter = 0

    def getWrite(self,cursor):
        if cursor < 0: return None
        elif cursor >= len(self.writes) : return None
        else:
            return self.writes[cursor]
            
    def getWrites(self,count):
        s = self.counter
        self.counter += count
        return [self.getWrite(c) for c in range(s,s+count)]

    def sample_wr(self,population, k):
        # Chooses k random elements (with replacement) from a population
        n = len(population)
        _random, _int = random.random, int  # speed hack 
        result = [None] * k
        for i in xrange(k):
            j = _int(_random() * n)
            result[i] = population[j]
        return result

    def gentable(self,numcols, colspecs):
        attvalues     = [[]]*numcols
        schema   = [[]]*numcols
        i = 0
        while i < numcols:
            # spec is of the form -- attribute draw basevalue distribution
            # for now we ignore distribution
            spec = colspecs[i]
            attribute = spec[0]
            draw = float(spec[1])
            basevalue = 0
            if len(spec)>2:
                basevalue = int(spec[2])
            # generate the numbers used to generate attribute values
            if draw == 1:
                numlist = random.sample(range(self.numrows),self.numwrites)
            # for now we assume uniform distribution        
            elif draw < 1:
                numvalues = int(round(self.numrows*draw, 0))
                numlist   = self.sample_wr(range(numvalues), self.numwrites)
            elif draw > 1:
                numvalues = int(round(draw, 0))
                if numvalues >= self.numwrites:
                    numlist = random.sample(range(numvalues),self.numwrites)
                else:
                    numlist = self.sample_wr(range(numvalues),self.numwrites)
            # adjust list of numbers to account for basevalue   
            numlist = [num+basevalue for num in numlist]        
            # distinction between numerical attributes 'n' and alphanumerical attributes ATT_NAME
            if attribute == 'n':
                att = ''
                s = 'numeric'
                attvalues[i] = [num for num in numlist] 
            else:
                att = attribute
                s = 'varchar('+str(max(numlist))+')'   
                attvalues[i] = [att+str(num) for num in numlist]    
            # record schema
            schema[i] = s
            i += 1
        # remove duplicates for the key values
        if self.numkeys == numcols:
            # - use sets to eliminate duplicates
            tuples = zip(*attvalues)
            writes = set(tuples)
        elif self.numkeys < numcols:
            # - use dictionary to eliminate duplicates
            keys = zip(*attvalues[:self.numkeys])
            rest = zip(*attvalues[self.numkeys:])
            dico = dict(zip(keys,rest))
            writes = set([k+v for k,v in dico.items()])
        else:
            print "error in specfile: numkeys > numcols\n"
            system.exit(2)
        print "done generating rows..."
        return list(writes)


    
help_message = '''
python reads.py [options]
options:
-h, --help       : this help message
-r, --runs=      : number of runs (< 100) -- should be 1 if cold buffer
-q, --queries=   : number of queries per run
-p, --path=      : complete path to query file
-s, --specfile=  : specification file (gentable format)
-k, --numkeys=   : number of keys in specification file
-m, --numtuples= : max number of tuples in specification file (should be greater than -n)
-a, --attribute= : position of the attribute referenced in update file (multiple -a considered in order)
Executes reads against the database described in ../db2.py and prints timing 

The default values are:
-r 1                    # Number of runs 
-p "./query_range.sql"  # Complete filename (including path) of query file 
-q 1                    # Nb of queries per run
-m 1000000              # Nb of potential employees tuple
-s 'employeesspec'      # Employees table
-k 1                    # 1 key in Employees table

Example: python reads.py -r1 -q1000 -p./query_point.sql -a0
         python reads.py -r5 -q100 -p./query_multipoint.sql -a5
         python reads.py -r10 -q1 -p./query_scan.sql 
         python reads.py -r1 -q5 -p./query_range.sql

'''

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    global NBRUNS, NBQUERIES
    global NBTUPLES, SPECFILE, NBKEYS, ATTLIST
    global QUERY_FILE_PATH, query_str
    global g

    try:
        if argv is None:
            argv = sys.argv
        try:
             opts, args = getopt.getopt(argv[1:], 
              "hvr:q:p:s:k:m:a:", 
             ["help", "runs=", "queries=", "path=", "specfile=", "numkeys=", "numtuples=", "attribute="])
        except getopt.error, msg:
            raise Usage(msg)
    
        # Option processing
        try:
            for option, value in opts:
                if option == "-v":
                    verbose = True
                if option in ("-h", "--help"):
                    raise Usage(help_message)
                if option in ("-r", "--runs"):
                    v = int(value)
                    if not (v < 100): raise Usage("Runs out of bounds")
                    NBRUNS = v
                if option in ("-q", "--queries"):
                    v = int(value)
                    if not (v < 1000): raise Usage("NbQueries out of bounds")
                    NBQUERIES = v
                if option in ("-p", "--path"):
                    if not os.path.exists(value): raise Usage("Query file path does not exist")
                    QUERY_FILE_PATH= value
                if option in ("-s", "--specfile"):
                    if not os.path.exists(value): raise Usage("Spec file does not exist")
                    SPECFILE = value
                if option in ("-k", "--numkeys"):
                    v = int(value)
                    NBKEYS = v
                if option in ("-m", "--numtuples"):
                    v = int(value)
                    NBTUPLES = v
                if option in ("-a","--attribute"):
                    v = int(value)
                    ATTLIST.append(v)
                
        except ValueError, e:
            raise Usage("Invalid parameter:" + e)
    
    
        # Verify preconditions: modes are compatible, required sql files exist
        read_str = None
        try:
            f = open(QUERY_FILE_PATH, 'r')
            query_str = f.readline()
            f.close()
        except IOError, e:
            raise Usage("Failed to open sql file.\n")
        
        if (query_str == None): raise Usage("Failed to read from SQL file")         
        
        g = GenWrites(NBTUPLES, NBKEYS, NBQUERIES, SPECFILE)
    
        print ('run (query:'+ QUERY_FILE_PATH +')')

        # Timed experiment 
        t = timeit.Timer("experiment(query_str,g)", "from __main__ import experiment, query_str,g")
        timings = []
        try:
            # repeat 1 experiment NBRUNS time - output is a list of timing
            timings = t.repeat(NBRUNS,1)    
            # Log timing
            for timing in timings:
                s = str(timing)
                outputKey = re.search('(?<=./)\w+(?=.sql)',QUERY_FILE_PATH)
                if (outputKey == None):
                    print QUERY_FILE_PATH + ':=:' + s 
                else:
                    print outputKey.group(0) + ':=:' + s 
        except:
            raise Usage(t.print_exc())
            
    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
        print >> sys.stderr, "For help use --help"
        return 2

if __name__ == "__main__":
    sys.exit(main())

