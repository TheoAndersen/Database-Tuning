#!/usr/bin/env python
# encoding: utf-8
"""
gentable.py

This file generates tables in file format for loading.
Vertical bar delimited

Input as follows:
python gentable numrows outfile specificationfile numkeyfields
For example, suppose we want to produce data to go into
roomtype(hotelid, roomtypeid, description)
where the first two fields are the keys.

If the file roomtypespec has the data
hotel 0.25
n 4.0
desc 16.0

The first field of each spec is either n in which case it
is a numeric attribute, d in which case it is a date attribute,
otherwise the attribute is a string composed of
the first field postfixed with a generated integer.
The second field of each spec is either 1 in which case
the number of different values equals the number of rows
or less than 1 in which case it is a fraction
or it is a fixed number in which case the choice is from
that number.
In the above example, there are 1/4 as many distinct
values of hotel as there are rows, there are only 4 room
types and only 16 descriptions. For dates, these ranges
are mapped on number of days from which the generated date
is drawn.

Then we can produce the roomtype data as follows:
python gentable.py 30 roomtype roomtypespec 2

and we will produce a 30 row, 3 column output
with the first two fields constituting a key.

(c) Philippe Bonnet, Dennis Shasha 2009, 2010.

"""

import sys
import getopt
import random
from datetime import date
from string import maketrans

help_message = '''
python gentable.py numrows outfile specificationfile numkeyfields
generates numrows vertical bar delimited rows in outfile
based on the fields specified in specificationfile. The first 
numkeyfields fields of specificationfile are key.
'''


class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg
        
def spitvert(row):
    # row is a tuple
    if (row[0:] == ()):
        return '\n'
    elif (row[1:] == ()):
        return str(*row[:1])+"\n"
    else:
        s= str(*row[:1]) + str("|") + spitvert(row[1:]) 
        return s
    
        
def sample_wr(population, k):
    # Chooses k random elements (with replacement) from a population¨
    if population == []: population = [0]
    n = len(population)
    #_random, _int = random.random, int  # speed hack 
    result = [None] * k
    for i in range(k):
        j = int(random.random() * (n-1))
        result[i] = population[j]
    return result
    
def gentable(numrows, numcols, colspecs, numkeys):
    attvalues = [[]]*numcols
    attdesc   = [[]]*numcols
    i = 0
    while i < numcols:
        # spec is of the form -- attribute draw basevalue distribution
        # for now we ignore distribution
        spec = colspecs[i]
        attribute = spec[0]
        draw = float(spec[1])
        basevalue = 0
        if (len(spec)>2):
            basevalue = int(float(spec[2]))
                
        # generate the numbers used to generate attribute values
        if draw == 1:
            numlist = random.sample(range(numrows),numrows)
        # for now we assume uniform distribution        
        elif draw < 1:
            numvalues = int(round(numrows*draw, 0))
            numlist= sample_wr(range(numvalues),numrows)
        elif draw > 1:
            numvalues = int(round(draw, 0))
            if numvalues >= numrows:
                numlist = random.sample(range(numvalues),numrows)
            else:
                numlist = sample_wr(range(numvalues), numrows)
        # adjust list of numbers to account for basevalue   
        numlist = [num+basevalue for num in numlist]
        
        # distinction between numerical attributes 'n', date 'd' and alphanumerical attributes ATT_NAME
        if attribute == 'n':
            attvalues[i] = [str(num) for num in numlist]
            attdesc[i]= 'numeric'
        elif attribute == 'd':
            now = date.today()
            now_ordinal = date.toordinal(now)
            attvalues[i] = [date.isoformat(date.fromordinal(now_ordinal+num)) for num in numlist]
            attdesc[i]= 'date'
        else:
            attvalues[i] = [attribute+str(num) for num in numlist]
            attdesc[i]= 'varchar('+str(max(numlist))+')'
            
        i += 1
        
    # remove duplicates for the key values
    if numkeys == numcols:
        # - use sets to eliminate duplicates
        tuples = zip(*attvalues)
        table = set(tuples)
    elif numkeys < numcols:
        # - use dictionary to eliminate duplicates
        keys = zip(*attvalues[:numkeys])
        rest = zip(*attvalues[numkeys:])
        dico = dict(zip(keys,rest))
        table = set([k+v for k,v in dico.items()])
    else:
        print "error in specfile: numkeys > numcols\n"
        system.exit(2)
    
    table = list(table)
    table.insert(0,tuple(attdesc))
    
    # pretty print table on output file
    listofrowstrings = [spitvert(row) for row in table]
        
    return listofrowstrings


def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "h", ["help"])
        except getopt.error, msg:
            raise Usage(msg)
    
        for option, value in opts:
            if option in ("-h", "--help"):
                raise Usage(help_message)
                sys.exit(1)
            # other options silently ignored
            
        # main 
        if len(args) == 4:
            numrows = int(argv[1])
            targfile = argv[2]
            specfile = argv[3]
            numkeys = int(argv[4])
            
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
            table = gentable(numrows, numcols, colspecs, numkeys)
            
            # write table to output file
            print "writing outpt file..."
            o = open(targfile, "w")
            o.writelines(table) 
            
            print "done."
            
            
            
            
    
    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
        print >> sys.stderr, "\t for help use --help"
        return 2


if __name__ == "__main__":
    sys.exit(main())
