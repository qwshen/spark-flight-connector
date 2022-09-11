This tutorial uses Dremio Community Edition v22.0.0 or above as the back-end Arrow-Flight server. Please follow the following steps:

1. Set up a linux environment, such as Ubuntu 14.04+ with Java SE 8 (JDK 1.8) or Java SE 11 (JDK 1.11);

2. Download the latest Dremio Community Edition [here](https://download.dremio.com/community-server/dremio-community-LATEST.tar.gz). Make sure it is v21.2.0 or above. Unzip the tar file to /opt/dremio;

3. Download Apache Spark v3.2.1 or above from [here](https://spark.apache.org/downloads.html). Unzip the tar file to /opt/spark.

4. Edit ~/.bashrc by adding the following line at the end of the file:
    ```shell
    export PATH=/opt/dremio/bin:/opt/spark/bin:$PATH
    ```

5. Run the following command:
   ```shell
   source ~/.bashrc
   ```

6. Start the Dremio server by running the following command:
   ```shell
   dremio start
   ```

7. In a browser, browse to http://127.0.0.1:9047 to open the Dremio Web Console. If this is your first time, you are required to create an admin user:
   - User-Name: test
   - Password: Password@123

8. Open the SQL Runner on the Dremio Web UI, and run the following query to make sure all iceberg entries are set correctly:
   ```roomsql
   SELECT name, bool_val, num_val FROM sys.options WHERE name like '%iceberg%'
   ```
   The following properties should be set to true:
   ```roomsql
   dremio.iceberg.enabled = true
   dremio.iceberg.dml.enabled = true
   dremio.iceberg.ctas.enabled = true
   dremio.iceberg.time_travel.enabled = true
   ```

9. If any above property is not set to true, please execute the following commands in the SQL Runner to enable iceberg:
   ```roomsql
   alter system set dremio.iceberg.enabled = true;
   alter system set dremio.iceberg.dml.enabled = true;
   alter system set dremio.iceberg.ctas.enabled = true;
   alter system set dremio.iceberg.time_travel.enabled = true;
   ```

10. Launch spark-shell
    ```shell
    # create the data folder first
    mkdir -p /tmp/data
    # make sure the spark-flight-connector-1.0.jar is copied to the current directory
    # launch spark-sql. 
    spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.2 --jars ./spark-flight-connector_3.2.1-1.0.0.jar \
      --conf spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.sql.catalog.iceberg_catalog.type=hadoop --conf spark.sql.catalog.iceberg_catalog.warehouse=file:///tmp/data
    ```

11. In spark-shell, run the following code to create the iceberg database and make it the current database:
    ```scala
    sql("create database iceberg_catalog.iceberg_db")
    sql("use iceberg_catalog.iceberg_db")
    ```

12. Create the customer table:
    ```scala
    sql("create table iceberg_customers(customer_id bigint not null, created_date string not null, company_name string, contact_person string, contact_phone string, active boolean) using iceberg;")
    sql("show tables").show(false)   // make sure the customer table has been created
    ```

13. Insert a few records into the customers table
    ```scala
    sql("insert into iceberg_customers values(3001, '2019-12-15', 'ABC Manufacturing', 'Jay Douglous', '123-xxx-1212', true)")
    sql("insert into iceberg_customers values(3002, '2018-12-14', 'My Pharma Corp.', 'Jessica Smith', '123-xxx-3652', true)")
    sql("insert into iceberg_customers values(3003, '2014-12-17', 'My Investors, Inc.', 'Chris Pandha', '123-xxx-6845', true)")
    sql("insert into iceberg_customers values(3004, '2013-12-15', 'XYZ Life Insurance, Inc.', 'Foster Ling', '123-xxx-9487', true)")

    //make sure the records have been inserted
    sql("select * from iceberg_customers").show(false)
    ```

14. Go back to the Dremio Web UI, and create a source pointing to /tmp/data.
  - Open http://127.0.0.1:9047
  - Sign in with test/Password@123
  - Click on the Datasets icon in the top-left corner on the page, then click on the + button at right of "Data Lakes" link in the left-bottom corner of the page
  - On the "Add Data Lake" window, pick NAS, then type:
    - Name: local-iceberg
    - Mount-Path: /tmp/data
  - Click the Save button 

15. Click the local_iceberg source to show the iceberg_customers table; hover your mouse over the iceberg_customer item, and then click on the "Format Folder" button. Dremio will automatically detects the iceberg format. Click Save to save the format.

16. Open SQL Runner, and run the following SQL statements:
    ```roomsql
    select * from "local-iceberg"."iceberg_db"."iceberg_customers"   -- make sure all pre-inserted records showing up
    -- insert a new record
    insert into "local-iceberg"."iceberg_db"."iceberg_customers"(customer_id, created_date, company_name, contact_person, contact_phone, active) values(3005, '2022-05-11', 'My Foods Inc.', 'Judy Smith', '416-xxx-2212', 'true')
    -- make sure the new record has been added
    select * from "local-iceberg"."iceberg_db"."iceberg_customers" 
    ```

17. Go back to spark-shell, and run the following code:
    ```scala
    val df = spark.read.format("flight")
        .option("host", "127.0.0.1").option("port", "32010").option("user", "test").option("password", "Password@12345")
        .option("table", """"local-iceberg"."iceberg_db"."iceberg_customers"""")
    .load
    df.show(false)   //to show the records from the table
    ```

18. Truncate the table, then run an insert:
    ```scala
    val df = spark.read.format("flight")
        .option("host", "127.0.0.1").option("port", "32010").option("user", "test").option("password", "Password@12345")
        .option("table", """"local-iceberg"."iceberg_db"."iceberg_customers"""")
    .load
    df.show(false)   //to show the records from the table

    //overwrite
    df.withColumn("customer_id", col("customer_id") + 90000)
      .write.format("flight")
        .option("host", "127.0.0.1").option("port", "32010").option("user", "test").option("password", "Password@12345")
        .option("table", """"local-iceberg"."iceberg_db"."iceberg_customers"""")
      .mode("overwrite").save()
    ```
    Then go to the Dremio web-ui to check if new data has been inserted with the new customer IDs.

19. Run the following merge by:
    ```scala
    val df = spark.read.format("flight")
        .option("host", "127.0.0.1").option("port", "32010").option("user", "test").option("password", "Password@12345")
        .option("table", """test."iceberg_db"."iceberg_customers"""")
    .load
    df.show(false)   // to show the records from the table

    // merge-by
    df.withColumn("customer_id", when(col("customer_id") % 3 === lit(0), col("customer_id") + 90000).otherwise(col("customer_id")))
      .withColumn("created_date", current_date())
      .withColumn("company", when(col("company") === lit("ABC Manufacturing"), lit("Central Bank")).otherwise(col("company")))
    .write.format("flight")
      .option("host", "127.0.0.1").option("port", "32010").option("user", "test").option("password", "Password@12345")
      .option("table", """test."iceberg_db"."iceberg_customers"""")
      .option("merge.byColumn", "customer_id")
    .mode("append").save()
    ```
    Then go to the Dremio web-ui to check data changes.

