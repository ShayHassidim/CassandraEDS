[![Build Status](https://secure.travis-ci.org/OpenSpaces/CassandraEDS.png)](http://travis-ci.org/OpenSpaces/CassandraEDS)

<h1>Cassandra OpenSpaces External Data Source</h1>
The Cassandra OpenSpaces EDS implementation allows applications to use push the long term data into
Cassandra database in an asynchronous manner without impacting the application response time and also
load data from the Cassandra database once the GigaSpaces IMDG is started or in a lazy manner once 
there is a cache hit when reading data from OpenSpaces IMDG.

The OpenSpaces Cassandra EDS leveraging the Cassandra CQL and the Cassandra JDBC Driver.
Every application write or take operation against the IMDG is delegated into the Mirror 
service that is using the Cassandra Mirror implementation to execute the CQL statement and push
the changes into the Cassandra database.



