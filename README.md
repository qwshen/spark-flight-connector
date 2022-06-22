The flight-spark-connector is a Spark DataSource API (v2) that reads/writes data from/to any Arrow-Flight endpoints, such as Dremio Arrow-Flight Server. Arrow Flight enables 
high speed data transfer compared to ODBC/JDBC connections by utilizing the Apache Arrow format to avoid serializing and deserializing data.

To build the project:
```shell
  mvn clean install -DskipTests
```

The connector requires Spark 3.2.0+ due to the support of Interval types. In order to connect to an arrow-flight service, the following 
properties are mandatory:

- host: the full name or ip of the arrow-flight server host;
- port: the port number;
- user: the user account for connecting to and reading/writing data from/to the arrow-flight endpoint; 
- password: the password of the user account;
- table: the name of the table from/to which the connector reads/writes data. The table can be a physical data table, or any view. It can also be a select sql-statement or 
tables-joining statement. For example:
  - select-statement
    ```roomsql
     select id, name, address from customers where city = 'TORONTO'
    ```
  - tables-joining statement
    ```roomsql
     orders o inner join customers c on o.customer_id = c.id where o.id = 312831
    ```

The following property is optional:
- tls.enabled: whether the arrow-flight endpoint is tls-enabled for secure communication;
- tls.verifyServer - whether to verify the certificate from the arrow-flight server; Default: true if tls.enabled = true.
- tls.truststore.jksFile: the trust-store file in jks format;
- tls.truststore.pass: the pass code of the trust-store file;
- column.quote: the character to quote fields that any illegal character is used, such as the following sql statement:
```roomsql
  select id, "departure-time", "arrival-time" from flights where "flight-no" = 'ABC-21';
```

### Load data
```scala
spark.read.format("flight")
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .option("table", """"e-commerce".orders""")
    .options(options)  //other options
  .load
```
or
```scala
spark.read
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .option("table", """"e-commerce".orders""")
    .options(options)  //other options
  .flight
```
or 
```scala
spark.read
    .option("host", "192.168.0.26").option("port", 32010).option("tls.enabled", true).option("tls.verifyServer", false).option("user", "test").option("password", "Password@123")
    .optoin("column.quote", "\"")
    .options(options)  //other options
  .flight(""""e-commerce".orders""")
```

#### Read options:

- sdf

#### Pushing filter & columns down


### Write data
### Data-type Mapping

