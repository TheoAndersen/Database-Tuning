// Database Tuning: Principles, Experimentation and Design
// @ Dennis Shasha, Philippe Bonnet - 2010

Script:		experiment.py    -- timed experiment (sum/swap transactions)
Schema:		init.sql  -- accounts table
Indexes:	index.sql -- clustered index on accounts number
Scripts:	sum.sql   -- summation query
	  		swap1.sql -- select balance for account number X)
	 		swap2.sql -- update balance for account number Y)

GOAL
----
The goal of this experiment is to study the trade-off between
different isolation levels. Serializable execution trades decreased
throughput for improved correctness. But what is the value of serializability? 
How incorrect is read committed? How inefficient is serializable?

The experiment consist in running concurrently a summation query that
scans the entire accounts table (sum.sql) concurrently with
transactions that swap the balance between accounts (swap1.sql +
swap2.sql).

CONFIGURATION
-------------

MEASUREMENTS
------------

Vary the number of threads (NBSWAPTHREADS) and the isolation level
(isolation: 1 for read committed and 3 for serializable) in the
configuration file ValueOfSerializability.conf. Measure the response
time for different number of jobs for the swap transactions. Trace
throughput (nb statements/response time) as a function of the number
of concurrent jobs for the swap transactions.







