**spark-flight-connector** is an Apache Spark DataSource API that reads/writes data from/to arrow-flight endpoints, such as Dremio Flight Server. With proper partitioning, it supports fast loading of large datasets by parallelizing reads. It also supports all insert/merge/update/delete DML operations for writes. With arrow-flight, it enables high speed data transfers compared to ODBC/JDBC connections by utilizing the Apache Arrow format to avoid serializing and deserializing data.

To build the project, run:
```shell
  mvn clean install -DskipTests
```

The connector requires Spark 3.2.0+ due to the support of Interval types. For a quick start, please jump to this [tutorial](docs/tutorial.md).

In order to connect to an arrow-flight endpoint, the following properties are mandatory:

- `host`: the full host-name or ip of an arrow-flight server;
- `port`: the port number;
- `user`: the user account for connecting to and reading/writing data from/to the arrow-flight endpoint; 
- `password` or `bearerToken`: the password/pat or auth2 token of the user account. One of them must be provided; the password takes precedence if both provided.
- `table`: the name of a table from/to which the connector reads/writes data. The table can be a physical data table, or any view. It can also be a select sql-statement or tables-joining statement. For example:
  - Select statement:
    ```roomsql
     select id, name, address from customers where city = 'TORONTO'
    ```
  - Join tables statement:
    ```roomsql
     orders o inner join customers c on o.customer_id = c.id
    ```
  *Note: the connector doesn't support legacy flight authentication mode (flight.auth.mode = legacy.arrow.flight.auth).*

The following properties are optional:

- `tls.enabled`: whether the arrow-flight end-point is tls-enabled for secure communication;
- `tls.verifyServer` - whether to verify the certificate from the arrow-flight end-point; Default: true if tls.enabled = true.
- `tls.truststore.jksFile`: the trust-store file in jks format;
- `tls.truststore.pass`: the pass code of the trust-store;
- `column.quote`: the character to quote the name of fields if any special character is used, such as the following sql statement:
  ```roomsql
    select id, "departure-time", "arrival-time" from flights where "flight-no" = 'ABC-21';
  ```
- `default.schema`: default schema path to the dataset that the user wants to query.
- `routing.tag`: tag name associated with all queries executed within a Flight session. Used only during authentication.
- `routing.queue`: name of the workload management queue. Used only during authentication.

The connector supports optimized reads with filters, required columns and aggregation pushing-down, and parallel reads when partitioning is enabled.

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
import com.qwshen.flight.spark.implicits._
val df = spark.read
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .optoin("column.quote", "\"")
    .options(options)  //other options
  .flight(""""e-commerce".orders""")
df.show
```

#### - Partitioning:

By default, the connector respects the partitioning from the source arrow-flight endpoints. Data from each endpoint is assigned to one partition. However, the partitioning behavior can be further customized with the following properties:
- `partition.size`: the number of partitions in the final dataframe. The default is 6.
- `partition.byColumn`: the name of a column used for partitioning. Only one column is supported. This is mandatory when custom partitioning is applied.
- `partition.lowerBound`: the lower-bound of the by-column. This only applies when the data type of the by-column is numeric or date-time.
- `partition.upperBound`: the upper-bound of the by-column. This only applies when the data type of the by-column is numeric or date-time.
- `partition.hashFunc`: the name of the hash function supported in the arrow-flight end-points. This is required when the data-type of the by-column is not numeric or date-time, and the lower-bound, upper-bound are not provided. The default name is the hash as defined in Dremio.
- `partition.predicate`: each individual partitioning predicate is prefixed with this key.
- `partition.predicates`: all partitioning predicates, concatenated by semi-colons (;) or commas (,).

Notes:
- The by-column may or may not be on the select-list. If not, hash-partitioning is used by default due to the fact that the data-type of the by-column is not available.
- The lowerBound and upperBound must be both specified or none of them specified. If only of them specified, it is ignored.
- When lowerBound and upperBound are specified, and if their values don't match or are not compatible with the data-type of the by-column, then hash-partitioning is applied.

Examples:
```scala
//with lower-bound & upper-bound
import com.qwshen.flight.spark.implicits._
spark.read
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .optoin("column.quote", "\"")
    .option("partition.size", 128).option("partition.byColumn", "order_date").option("partition.lowerBound", "2000-01-01").option("partition.upperBound", "2010-12-31")
  .flight(""""e-commerce".orders""")
```
```scala
//with hash-function
import com.qwshen.flight.spark.implicits._
spark.read
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .optoin("column.quote", "\"")
    .option("partition.size", 128).option("partition.byColumn", "order_id").option("partition.hashFunc", "hash")
  .flight(""""e-commerce".orders""")
```
```scala
//with predicates
import com.qwshen.flight.spark.implicits._
spark.read
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .optoin("column.quote", "\"")
    .option("partition.size", 128)
    .option("partition.predicate.1", "92 <= event_id and event_id < 715827379").option("partition.predicate.2", "715827379 <= event_id and event_id < 1431654667")
    .option("partition.predicates", "1431654667 <= event_id and event_id < 2147481954;2147481954 <= event_id and event_id < 2863309242;2863309242 <= event_id and event_id < 3579136529") //concatenated with ;
  .flight(""""e-commerce".events""")
```
Note: when lowerBound & upperBound with byColumn or predicates are used, they are eventually filters applied on the queries to fetch data which may impact the final result-set. Please make sure these partitioning options do not affect the final output, but rather only apply for partitioning the output.

#### - Pushing filter & columns down
Filters and required-columns are pushed down when they are provided. This limits the data at the source which greatly decreases the amount of data being transferred and processed.
```scala
import com.qwshen.flight.spark.implicits._
spark.read
      .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
      .options(options)  //other options
    .flight(""""e-commerce".orders""")
  .filter("order_date > '2020-01-01' and order_amount > 100")  //filter is pushed down
  .select("order_id", "customer_id", "payment_method", "order_amount", "order_date")  //required-columns are pushed down 
```

#### - Pushing aggregation down
Aggregations are pushed down when they are used. Only the following aggregations are supported:
- max
- min
- count
- count distinct
- sum
- sum distinct

For avg, it can be achieved by combining count & sum. For any other aggregations, they are calculated at Spark level.

```scala
val df = spark.read
      .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
      .options(options)  //other options
    .flight(""""e-commerce".orders""")
  .filter("order_date > '2020-01-01' and order_amount > 100")  //filter is pushed down

df.agg(count(col("order_id")).as("num_orders"), sum(col("amount")).as("total_amount")).show()  //aggregation pushed down

df.groupBy(col("gender"))
  .agg(  
    countDistinct(col("order_id")).as("num_orders"),
    max(col("amount")).as("max_amount"),
    min(col("amount")).as("min_amount"),
    sum(col("amount")).as("total_amount")
  )  //aggregation pushed down
  .show()
```

### 2. Write & Streaming-Write data (tables being written must be iceberg tables in case of Dremio Flight)
```scala
df.write.format("flight")
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .option("table", """"e-commerce".orders""")
    .option("write.protocol", "literal-sql").option("batch.size", "512")
    .options(options)  //other options
    .mode("overwrite")
  .save
```
or
```scala
import com.qwshen.flight.spark.implicits._
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
import com.qwshen.flight.spark.implicits._
df.write
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .option("merge.byColumns", "order_id,customer_id").optoin("column.quote", "\"")
    .options(options)  //other options
    .mode("append")
  .flight(""""e-commerce".orders""")
```
streaming-write
```scala
df.writeStream.format("flight")
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .option("table", """"e-commerce".orders""")
    .option("checkpointLocation", "/tmp/checkpointing")        
    .options(options)  //other options
  .trigger(Trigger.Continuous(1000))
  .outputMode(OutputMode.Complete())
  .start()
  .awaitTermination(6000)
```
The following options are supported for writing:
- `write.protocol`: the protocol of how to submit DML requests to flight end-points. It must be one of the following:
  - `prepared-sql`: the connector uses PreparedStatement of Flight-SQL to conduct all DML operations.
  - `literal-sql`: the connector creates literal sql-statements for all DML operations. Type mappings between arrow and target flight end-point may be required, please check the Type-Mapping section below. This is the default protocol.
- `batch.size`: the number of rows in each batch for writing. The default value is 1024. Note: depending on the size of each record, StackOverflowError might be thrown if the batch size is too big. In such case, adjust it to a smaller value. 
- `merge.byColumn`: the name of a column used for merging the data into the target table. This only applies when the save-mode is `append`;
- `merge.ByColumns`: the name of multiple columns used for merging the data into the target table. This only applies when the save-mode is `append`.

Examples:
- Batch Write
```scala
df.write
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .optoin("batch.size", 16000)
    .option("merge.byColumn.1", "user_id").options("merge.byColumn.2", "product_id")
    .option("merge.byColumns", "order_date;order_amount") //concatenated with ;    
    .mode("append")
  .flight(""""e-commerce".orders""")
```
- Streaming Write
```scala
df.writeStream.format("flight")
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .option("table", """"local-iceberg".iceberg_db.iceberg_events""")
    .option("checkpointLocation", s"/tmp/staging/checkpoint/events")        
    .optoin("batch.size", 640)
  .trigger(Trigger.Once())
  .outputMode(OutputMode.Append())
  .start()
  .awaitTermination(300000)
```

### 3. Data-type Mapping
#### - Arrow >> Spark

 Arrow | Spark
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

Note: for Dremio Flight before v23.0.0, the Map type is converted to Struct. The connector detects the pattern and converts back to Map when reading data, and adapts to Struct when writing data (with v22.0.0 or above for write only).

#### - Spark >> Arrow

When the connector is writing data, the schema of the target table is retrieved first, then the connector tries to adapt the source field to the type of the target, so the types of source and target must be compatible. Otherwise, runtime exception will be thrown. Such as
- Spark Int adapts to Arrow Decimal;
- Spark Timestamp adapts to Arrow Time;
- When using literal sql-statements (write.protocol = literal-sql), all complex types (struct, map & list) are converted to json string.
- etc.

Note: for Dremio Flight (up to v22.0.0), it doesn't support writing complex types with DML statements yet, neither batch-writing with prepared-statements against iceberg tables. In such case, the connector could use literal sql-statements for DML operations.

#### - Arrow >> Flight End-Point (For "write.protocol = literal-sql" only)
When the connector uses literal sql-statements for DML operations, it needs to know the type system of the target flight end-point which may not support all types defined in Apache Arrow.

The following is the type-mapping between Apache Arrow and Dremio end-point:
```scala worksheet
  BIT --> BOOLEAN
  LARGEVARCHAR --> VARCHAR
  VARCHAR --> VARCHAR
  TINYINT --> INT
  SMALLINT --> INT
  UINT1 --> INT
  UINT2 --> INT
  UINT4 --> INT
  UINT8 --> INT
  INT  -> INT
  BIGINT  -> BIGINT
  FLOAT4 --> FLOAT
  FLOAT8 --> DOUBLE
  DECIMAL --> DECIMAL
  DECIMAL256 --> DECIMAL
  DATE --> DATE
  TIME --> TIME
  TIMESTAMP --> TIMESTAMP
```
This is also the default type-mapping used by the connector. To override it, please use the following option:
```scala worksheet
  df.write.format("flight")
      .option("write.protocol", "literal-sql")
      .option("write.typeMapping", "LARGEVARCHAR:VARCHAR;VARCHAR:VARCHAR;TINYINT:INT;SMALLINT:INT;UINT1:INT;UINT2:INT;UINT4:INT;UINT8:INT;INT:INT;BIGINT:BIGINT;FLOAT4:FLOAT;FLOAT8:DOUBLE;DECIMAL:DECIMAL;DECIMAL256:DECIMAL;DATE:DATE;TIME:TIME;TIMESTAMP:TIMESTAMP")
      .option(options)    //other options
.mode("overwrite").save
```
Currently, the binary, interval, and complex types are not supported when using literal sql-statement for DML operations.
