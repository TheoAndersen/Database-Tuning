#!/usr/bin/env python
# encoding: utf-8
"""
DB2/ValueOfSerializability/experiment.py

A database must exist and be initialized before the experiment can be run 
(table and index creation in init.sql), data should be loaded (load.sql), 
and the database should be cleant up afterwards (cleanup.sql).

The database parameters are obtained from ../db2.py

Copyright (c) Philippe Bonnet 2010 . All rights reserved.
"""

import sys
import getopt
import timeit
import multiprocessing
import random
import os
import ibm_db
import time


### Experiment parameters (default values)
NBRUNS         = 5    # Number of runs (-r:, --runs=)
NBSWAPS        = 100   # Number of swaps (-s:, --swaps=)
NBSWAPTHREADS  = 10   # Number of swap threads (-t:, --threads=)
RANGE_LOW	   = 1    # Lower bound of the range for account number
RANGE_UP 	   = 1000000  # Upper bound of the range for account number
ISOL_LEVEL     = 'RR'

### Output parameters (default values)
OUTPUT_FILE_PATH  = '.'   # Path of the output file output.txt (append)

### Database parameters (DATABASE; HOSTNAME; PORT; USERNAME; PASSWORD)
sys.path.append("..")
from db2 import DATABASE
from db2 import HOSTNAME
from db2 import PORT
from db2 import USERNAME
from db2 import PASSWORD

# Process Manager data structure
q = None

""""
Swapping of balance values.
read balance for account number X into valX and for account number Y into valY.
update account number X with balance set to valY
update account number Y with balance set to valX
X < Y 
We avoid deadlocks because of the clustered index on
account number that garantees that account numbers 
are accessed in acending order.
"""
def swap(q):
    swap1_str= q[0]; swap2_str = q[1]
    # Connect to DB
    conn = ibm_db.pconnect(DATABASE, USERNAME, PASSWORD)
    if conn is None: raise Usage(ibm_db.conn_errormsg())
    ibm_db.autocommit(ibm_db.SQL_AUTOCOMMIT_OFF)
    # Set isolation level
    ret = ibm_db.exec_immediate(conn, "SET CURRENT ISOLATION = "+ISOL_LEVEL)
    # Prepare Statements
    swap1_stmt = ibm_db.prepare(conn, swap1_str)
    if (swap1_stmt == False): raise Usage("Failed to prepare swap1 query")
    swap2_stmt = ibm_db.prepare(conn, swap2_str)
    if (swap2_stmt == False): raise Usage("Failed to prepare swap2 update")
    # Execute Statements
    nbrep = int(round(NBSWAPS / NBSWAPTHREADS))
    for i in range(nbrep):
        x = random.randint(RANGE_LOW, RANGE_UP/2)
		y = random.randint(x,RANGE_UP)
		if ibm_db.execute(swap1_stmt, (x,)) == False:
	            raise Usage("Failed to execute the swap1 query (x)")
		valX = ibm_db.fetch_tuple(swap1_stmt)
		if valX == False:
		    raise Usage("Failed to iterate over the swap1 result set (x)")
		if ibm_db.execute(swap1_stmt, (y,)) == False:
		    raise Usage("Failed to execute the swap1 query (y)")
		valY = ibm_db.fetch_tuple(swap1_stmt)
		if valY == False:
		    raise Usage("Failed to iterate over the swap1 result set (y)")
	        time.sleep(1)
		if ibm_db.execute(swap2_stmt, (valY[0],x)) == False:
		    raise Usage("Failed to execute the swap2 query (x, valY)")
		if ibm_db.execute(swap2_stmt, (valX[0],y)) == False:
		    raise Usage("Failed to execute the swap1 query (y, valX)")
		ibm_db.commit(conn)
	# Disconnect from DB
	status = ibm_db.close(conn)
	if status == False: raise Usage("Failed to close db connection.\n") 


def summation(q):
    sum_str = q[2]
    # Connect to DB
    conn = ibm_db.pconnect(DATABASE, USERNAME, PASSWORD)
    if conn is None: raise Usage(ibm_db.conn_errormsg())
    ibm_db.autocommit(ibm_db.SQL_AUTOCOMMIT_OFF)
    # Set isolation level
    ret = ibm_db.exec_immediate(conn, "SET CURRENT ISOLATION = "+ISOL_LEVEL)
    # Prepare statement
    sum_stmt   = ibm_db.prepare(conn, sum_str)
    if (sum_stmt == False): raise Usage("Failed to prepare sum query")
    # Execute statement
    if ibm_db.execute(sum_stmt) == False:
        raise Usage("Failed to execute the sum query")
    sum= ibm_db.fetch_tuple(sum_stmt)
    ibm_db.commit(conn)
    # Print result set to output file
    try:
	  f = open(OUTPUT_FILE_PATH+'/output.txt', 'a')
  	except IOError, e:
	  raise Usage("Failed to open output.txt. Check the output file path.\n")
	try:
	  f.write(str(sum)+'\n')
 	  f.close()
    except IOError, e:
	  raise Usage("Failed to write to output.txt.\n")
    finally:
	  f.close()
    # Disconnect from DB
    status = ibm_db.close(conn)
    if status == False: raise Usage("Failed to close db connection.\n") 

"""
Thread wrapper class
"""
class Thread(multiprocessing.Process):
	def __init__(self, target, *args):
            multiprocessing.Process.__init__(self, target=target, args=args)
	        self.start()
	
def experiment(q):
    ThreadL = []
	# Launch swap threads
	for n in range(NBSWAPTHREADS):
		ThreadL.append(Thread(swap,q))
	# Launch Summation thread
        ThreadL.append(Thread(summation, q))
	# Barrier
        for t in ThreadL:
            t.join()
	
help_message = '''
python sumNswap.py [options]
options:
-h, --help       : this help message
-t, --threads=   : number of swap threads (1..59)
-s, --swaps=     : total number of swaps (< 1000) 
-r, --runs=      : number of repetitions (< 100)
-i, --isol=      : isolation level ('UR', 'CS', 'RS','RR')
-o, --output=    : path to output file (result.txt)

Executes value of serializability experiment against the database described in ../db2.py
and prints timing 

Example: python sumNswap.py -t10 -s1000 -r5 -iCS
'''

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    global NBRUNS, NBSWAPS, NBSWAPTHREADS, RANGE_LOW, RANGE_UP, ISOL_LEVEL
    global OUTPUT_FILE_PATH
    global q

    if argv is None:
        argv = sys.argv
    try:
	try:
         opts, args = getopt.getopt(argv[1:], 
		  "ho:vr:s:t:g:i:", 
		 ["help", "output=", "runs=","swaps=", "threads=", "isol="])
	except getopt.error, msg:
		raise Usage(msg)
	
	# Option processing
	for option, value in opts:
		if option == "-v":
	        verbose = True
		if option in ("-h", "--help"):
		    raise Usage(help_message)
		if option in ("-r", "--runs"):
			v = int(value)
			if not (v < 100): raise Usage("Runs out of bounds")
			NBRUNS = v
		if option in ("-s", "--swaps"):
            v = int(value)
			if not (v < 10000): raise Usage("Swaps out of bounds")
			NBSWAPS = v
		if option in ("-t", "--threads"):
            v = int(value)
            if (v < 0 or v>60): raise Usage("Threads out of bounds")
			NBSWAPTHREADS = v
		if option in ("-i", "--isol"):
			if not value in ['UR', 'CS', 'RS', 'RR']: raise Usage("Isolation level not supported")
			ISOL_LEVEL = value
		if option in ("-o", "--output"):
			if not os.path.exists(value): raise Usage("Result file path does not exist")
			OUTPUT_FILE_PATH= value

	# Verify preconditions: required sql files exist
	try:
		f = open('sum.sql', 'r')
		sum_str = f.readline()
		f.close()
	except IOError, e:
		raise Usage("Failed to manipulate sum.sql.\n")
		
	try:
		f = open('swap1.sql', 'r')
		swap1_str = f.readline()
		f.close()
	except IOError, e:
		raise Usage("Failed to manipulate swap1.sql.\n")		
		
	try:
		f = open('swap2.sql', 'r')
		swap2_str = f.readline()
		f.close()
	except IOError, e:
		raise Usage("Failed to manipulate swap2.sql.\n")		
		
    print 'run (isol: '+ISOL_LEVEL+', threads: '+str(NBSWAPTHREADS)+', swaps:'+str(NBSWAPS)+')'
    # Queue Initialization
    manager = multiprocessing.Manager()
    q = manager.list([swap1_str, swap2_str, sum_str])

	# Timed experiment 
	t = timeit.Timer("experiment(q)", "from __main__ import experiment,q")
	timings = []
	try:
	    # repeat 1 experiment NBRUNS time - output is a list of timing
	    timings = t.repeat(NBRUNS,1)	
	    # Log timing
	    for timing in timings:
		s = str(timing)
		print s	
        except:
            raise Usage(t.print_exc())		

    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
        print >> sys.stderr, "\t for help use --help"
        return 2

if __name__ == "__main__":
    sys.exit(main())

