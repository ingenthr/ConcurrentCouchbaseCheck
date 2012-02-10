This ConcurrentCouchbaseCheck is a really simple, silly way
to be able to generate workload with the Couchbase Java client
library.

When run, it will spawn a set of ten threads, each of which
will whack the server with a bomb of gets and sets 
asyncronously.  It will count the successes and failures and 
will calculate rough latency in a very unintelligent way.

Building
shell> ant jar

Usage:

First, change directory to dist, then,

shell> java -jar ConcurrentCouchbaseCheck.jar

Connect to the cluster using http://localhost:8091/pools as the
bootstrap URI and do 200 iterations (the default).

Advanced usage (needs enhancement):

First, change directory to dist, then,

shell> java -jar ConcurrentCouchbaseCheck.jar 1000 2000 myserver

Connect to the cluster using http://myserver:8091/pools as the
bootstrap URI and start up five threads each doing 1000 iterations.  Wait
up to 2000 seconds for the threads to complete their work.
