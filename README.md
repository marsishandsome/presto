# TiPresto: TiDB Hackathon 2019

TiPresto = Presto + TiDB

## Compile
```
./mvnw clean install -DskipTests
```

## Compile TiDB Connector
```
./mvnw clean install -DskipTests -pl presto-tidb
```

## Running Presto in IntellijIDEA

After building Presto for the first time, you can load the project into your IDE and run the server. We recommend using [IntelliJ IDEA](http://www.jetbrains.com/idea/). Because Presto is a standard Maven project, you can import it into your IDE using the root `pom.xml` file. In IntelliJ, choose Open Project from the Quick Start box or choose Open from the File menu and select the root `pom.xml` file.

After opening the project in IntelliJ, double check that the Java SDK is properly configured for the project:

* Open the File menu and select Project Structure
* In the SDKs section, ensure that a 1.8 JDK is selected (create one if none exist)
* In the Project section, ensure the Project language level is set to 8.0 as Presto makes use of several Java 8 language features

Presto comes with sample configuration that should work out-of-the-box for development. Use the following options to create a run configuration:

* Main Class: `com.facebook.presto.server.PrestoServer`
* VM Options: `-ea -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -Xmx2G -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties`
* Working directory: `$MODULE_DIR$`
* Use classpath of module: `presto-main`

The working directory should be the `presto-main` subdirectory. In IntelliJ, using `$MODULE_DIR$` accomplishes this automatically.

## Debug in CLI

```
./presto-cli-0.227-executable.jar --server localhost:8080
```

```
presto> show catalogs;
 Catalog
---------
 system  
 tidb    
(2 rows)

Query 20191021_163747_00001_jdrew, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:01 [0 rows, 0B] [0 rows/s, 0B/s]
```

```
presto> show schemas from tidb;
             Schema             
--------------------------------
 batch_write_test_index         
 batch_write_test_pk            
 information_schema             
 multi_column_pk_data_type_test
 mysql                          
 resolvelock_test               
 test                           
 tispark_test                   
 tpch_test                      
(9 rows)

Query 20191021_163809_00002_jdrew, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:03 [9 rows, 180B] [3 rows/s, 60B/s]
```

```
presto> SHOW TABLES FROM  tidb.tpch_test;
  Table   
----------
 customer
 lineitem
 nation   
 orders   
 part     
 partsupp
 region   
 supplier
(8 rows)

Query 20191021_163829_00003_jdrew, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:00 [8 rows, 206B] [23 rows/s, 609B/s]
```

```
presto> SHOW COLUMNS FROM tidb.tpch_test.customer;
    Column    |     Type      | Extra | Comment
--------------+---------------+-------+---------
 c_custkey    | bigint        |       |         
 c_name       | varchar       |       |         
 c_address    | varchar       |       |         
 c_nationkey  | bigint        |       |         
 c_phone      | varchar       |       |         
 c_acctbal    | decimal(15,2) |       |         
 c_mktsegment | varchar       |       |         
 c_comment    | varchar       |       |         
(8 rows)

Query 20191021_163849_00004_jdrew, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:00 [8 rows, 580B] [33 rows/s, 2.35KB/s]
```

```
presto> select * from tidb.tpch_test.customer limit 2;
 c_custkey |       c_name       |           c_address            | c_nationkey |     c_phone     | c_acctbal | c_mktsegment |                            c_comment                            
-----------+--------------------+--------------------------------+-------------+-----------------+-----------+--------------+-----------------------------------------------------------------
         1 | Customer#000000001 | IVhzIApeRb ot,c,E              |          15 | 25-989-741-2988 | 7.11      | BUILDING     | to the even, regular platelets. regular, ironic epitaphs nag e  
         2 | Customer#000000002 | XSTf4,NCwDVaWNe6tEgvwfmRchLXak |          13 | 23-768-687-3665 | 1.21      | AUTOMOBILE   | l accounts. blithely ironic theodolites integrate boldly: caref
(2 rows)

Query 20191021_163916_00005_jdrew, FINISHED, 1 node
Splits: 18 total, 18 done (100.00%)
0:00 [150 rows, 100B] [694 rows/s, 463B/s]
```
