The spark-flight-connector is a Spark DataSource API (v2) that reads/writes data from/to Arrow-Flight end-points, such as Dremio Flight Server. Arrow Flight enables 
high speed data transfer compared to ODBC/JDBC connections by utilizing the Apache Arrow format to avoid serializing and deserializing data.

To build the project:
```shell
  mvn clean install -DskipTests
```

The connector requires Spark 3.2.0+ due to the support of Interval types. For a quick start, please jump to this [tutorial](docs/tutorial.md).

In order to connect to an arrow-flight end-point, the following properties are mandatory:

- host: the full host-name or ip of an arrow-flight server;
- port: the port number;
- user: the user account for connecting to and reading/writing data from/to the arrow-flight end-point; 
- password: the password of the user account;
- table: the name of the table from/to which the connector reads/writes data. The table can be a physical data table, or any view. It can also be a select sql-statement or 
tables-joining statement. For example:
  - select-statement
    ```roomsql
     select id, name, address from customers where city = 'TORONTO'
    ```
  - tables-joining statement
    ```roomsql
     orders o inner join customers c on o.customer_id = c.id
    ```

The following properties are optional:
- tls.enabled: whether the arrow-flight end-point is tls-enabled for secure communication;
- tls.verifyServer - whether to verify the certificate from the arrow-flight end-point; Default: true if tls.enabled = true.
- tls.truststore.jksFile: the trust-store file in jks format;
- tls.truststore.pass: the pass code of the trust-store file;
- column.quote: the character to quote the name of fields that any illegal character is used, such as the following sql statement:
```roomsql
  select id, "departure-time", "arrival-time" from flights where "flight-no" = 'ABC-21';
```

### 1. Load data
```scala
val df = spark.read.format("flight")
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .option("table", """"e-commerce".orders""")
    .options(options)  //other options
  .load
df.printSchema
df.count
df.show
```
or
```scala
val query = "select e.event_id, e.start_time, u.name as host_user, e.city from events e left join users u on e.user_id = u.id"
val df = spark.read
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .option("table", query)
    .options(options)  //other options
  .flight
df.show
```
or 
```scala
val df = spark.read
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .optoin("column.quote", "\"")
    .options(options)  //other options
  .flight(""""e-commerce".orders""")
df.show
```

#### - Partitioning:

By default, the connector respects the partitioning from the source arrow-flight end-points, data from each end-point is put to one partition. However, the partitioning behavior can be customized with the following properties:
- partition.size: the number of partitions in the final dataframe. The default is 6.
- partition.byColumn: the name of one column used to partitioning. Only one column for partitioning is supported. This is mandatory.
- partition.lowerBound: the lower-bound of the by-column. This only applies when the data-type of the by-column is numeric or date-time.
- partition.upperBound: the upper-bound of the by-column. This only applies when the data-type of the by-column is numeric or date-time.
- partition.hashFunc: the name of the hash function supported in the arrow-flight end-points. This is required when the data-type of the by-column is not numeric or date-time, and the lower-bound, upper-bound are not provided. The default name is hash.
- partition.predicate: each individual partitioning predicate is prefixed with this key.
- partition.predicates: all partitioning predicates are concatenated by semi-colon (;) or comma (,).

Note:
- The by-column may or may not be on the select-list. If not, hash-partitioning is used by default due to the fact that the data-type of the by-column is not available.
- The lowerBound and upperBound must be both specified or none of them specified. If only of them specified, it is ignored.
- When lowerBound and upperBound are specified, and if their values don't match or are not compatible with the data-type of the by-column, then hash-partitioning is applied.

Examples:
```scala
//with lower-bound & upper-bound
spark.read
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .optoin("column.quote", "\"")
    .option("partition.size", 128).option("partition.byColumn", "order_date").option("partition.lowerBound", "2000-01-01").option("partition.upperBound", "2010-12-31")
  .flight(""""e-commerce".orders""")
```
```scala
//with hash-function
spark.read
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .optoin("column.quote", "\"")
    .option("partition.size", 128).option("partition.byColumn", "order_id").option("partition.hashFunc", "hash")
  .flight(""""e-commerce".orders""")
```
```scala
//with predicates
spark.read
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .optoin("column.quote", "\"")
    .option("partition.size", 128)
    .option("partition.predicate.1", "92 <= event_id and event_id < 715827379").option("partition.predicate.2", "715827379 <= event_id and event_id < 1431654667")
    .option("partition.predicates", "1431654667 <= event_id and event_id < 2147481954;2147481954 <= event_id and event_id < 2863309242;2863309242 <= event_id and event_id < 3579136529") //concatenated with ;
  .flight(""""e-commerce".events""")
```
Note: when lowerBound & upperBound with byColumn or predicates are used, they are eventually filters applied on the query to fetch data which may impact the final result-set. Please make sure these partitioning options
are not affecting the final output, but they are only for partitioning the output.

#### - Pushing filter & columns down
The filters and required-columns are pushed down when they are provided. This limits the data at the source which greatly decreases the amount of data being transferred and processed.
```scala
spark.read
      .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
      .options(options)  //other options
    .flight(""""e-commerce".orders""")
  .filter("order_date > '2020-01-01' and order_amount > 100")  //filter is pushed down
  .select("order_id", "customer_id", "payment_method", "order_amount", "order_date")  //required-columns are pushed down 
```

### 2. Write data
```scala
df.write.format("flight")
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .option("table", """"e-commerce".orders""")
    .option("write.protocol", "sql").option("batch.size", "512")
    .options(options)  //other options
    .mode("overwrite")
  .save
```
or
```scala
df.write
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .option("table", """"e-commerce".orders""")
    .option("merge.byColumn", "order_id")
    .options(options)  //other options
    .mode("overwrite")
  .flight
```
or
```scala
df.write
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .option("merge.byColumns", "order_id,customer_id").optoin("column.quote", "\"")
    .options(options)  //other options
    .mode("append")
  .flight(""""e-commerce".orders""")
```
The following options are supported for writing:
- write.protocol: the protocol of how to submit DML requests to flight end-points
  - arrow: the connector uses Flight-SQL to conduct all DML operations.
  - sql: the connector uses literal sql-statements for all DML operations. This is the default protocol.
- batch.size: the number of rows in each batch for writing. The default value is 1024. Note: depending on the size of each record, StackOverflowError might be thrown if the batch size is too big. In such case, adjust it to a smaller value. 
- merge.byColumn: the name of a column used for merging the data into the target table. This only applies when the save-mode is in append;
- merge.ByColumns: the name of multiple columns used for merging the data into the target table. This only applies when the save-mode is in append.

Example:
```scala
df.write
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .optoin("batch.size", 16000)
    .option("merge.byColumn.1", "user_id").options("merge.byColumn.2", "product_id")
    .option("merge.byColumns", "order_date;order_amount") //concatenated with ;    
    .mode("append")
  .flight(""""e-commerce".orders""")
```

### 3. Data-type Mapping
#### - Arrow-Flight >> Spark

 Arrow-Flight | Spark
 --- | --- 
bit | boolean
signed int8 | byte
un-signed int8 | short
signed int16 | short
un-signed int16 | int
signed int32 | int
un-signed int32 | long
signed int64 | long
un-signed int64 | long
var-char | string
decimal | decimal
decimal256 | decimal
floating-single | float
floating-half | float
floating-double | double
date | date
time | string (hh:mm:ss.*)
timestamp | timestamp
interval - year.month | year-month interval
interval - day.time | day-time interval
duration | struct(year-month, day-time)
interval - month-day-nano | struct(year-month, day-time)
list | array
struct | struct
map | map

Note: for Dremio Flight (up to v22.0.0), the Map type is converted to Struct. The connector detects the pattern and converts back to Map when reading data, and adapts to Struct when writing data.

#### - Spark >> Arrow-Flight

When the connector is writing data, the schema of the target table is retrieved first, then the connector tries to adapt the source field to the type of the target, so the types of source and target must be compatible. Otherwise runtime exception will be thrown. Such as
- Spark Int adapts to Arrow-Flight Decimal;
- Spark Timestamp adapts to Arrow-Flight Time;
- In sql mode (write.protocol = sql), all complex types (struct, map & list) are converted to json string.
- etc.

Note: for Dremio Flight (up to v22.0.0), iceberg tables don't support complex types yet, and only accepts literal sql-statements for DML operations which require write.protocol = sql.
