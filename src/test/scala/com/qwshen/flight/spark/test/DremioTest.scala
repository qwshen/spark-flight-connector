package com.qwshen.flight.spark.test

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, col, count, countDistinct, lit, map, max, min, struct, sum, sum_distinct, when}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class DremioTest extends FunSuite with BeforeAndAfterEach {
  private val dremioHost = "192.168.0.19"
  private val dremioPort = "32010"
  private val dremioTlsEnabled = false
  private val user = "test"
  private val password = "Password@12345"

  test("Run a simple query") {
    val query = """select * from "azure-wstorage".input.users"""
    val run: SparkSession => DataFrame = this.load(Map("table" -> query, "column.quote" -> "\""))
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show()
  }

  test("Run a simple query with filter & fields") {
    val query = """select * from "azure-wstorage".input.users"""
    val run: SparkSession => DataFrame = this.load(Map("table" -> query, "column.quote" -> "\""))
    val df = this.execute(run)
      .filter("""(birthyear >= 1988 and birthyear < 1997) or (gender = 'female' and "joined-at" like '2012-11%')""")
      .select("user_id", "joined-at")
    df.printSchema()
    df.count()
    df.show()
  }

  test("Query a table with filter & fields") {
    val table = """"azure-wstorage".input.users"""
    val run: SparkSession => DataFrame = this.load(Map("table" -> table, "column.quote" -> "\""))
    val df = this.execute(run)
      .filter("""(birthyear >= 1988 and birthyear < 1997) or (gender = 'female' and "joined-at" like '2012-11%')""")
      .select("user_id", "joined-at")
    df.printSchema()
    df.count()
    df.show()
  }

  test("Query a join-table with filter & fields") {
    val table = """"azure-wstorage".input.events e left join "azure-wstorage".input.users u on e.user_id = u.user_id"""
    val run: SparkSession => DataFrame = this.load(Map("table" -> table, "column.quote" -> "\""))
    val df = this.execute(run)
      .filter("""birthyear is not null""")
      .select("user_name", "gender", "event_id", "start_time")
    df.printSchema()
    df.count()
    df.show()
  }

  test("Run a query with decimal, date, time, timestamp, year-month, day-time etc.") {
    val query = """
        |select
        |  cast(event_id as bigint) as event_id,
        |  gender,
        |  birthyear,
        |  cast(2022 - birthyear as interval year) as age_year,
        |  cast(2022 - birthyear as interval month) as age_month,
        |  cast(2022 - birthyear as interval day) as age_day,
        |  start_time as raw_start_time,
        |  cast(concat(substring(start_time, 1, 10), ' ', substring(start_time, 12, 8)) as timestamp) as real_start_time,
        |  cast(concat(substring(start_time, 1, 10), ' ', substring(start_time, 12, 12)) as timestamp) as milli_start_time,
        |  cast(substring(start_time, 12, 8) as time) as real_time,
        |  cast(now() - cast(concat(substring(start_time, 1, 10), ' ', substring(start_time, 12, 8)) as timestamp) as interval hour) as ts_hour,
        |  cast(now() - cast(concat(substring(start_time, 1, 10), ' ', substring(start_time, 12, 8)) as timestamp) as interval minute) as ts_minute,
        |  cast(now() - cast(concat(substring(start_time, 1, 10), ' ', substring(start_time, 12, 8)) as timestamp) as interval second) as ts_second,
        |  cast(substring(start_time, 1, 10) as date) as start_date,
        |  cast(case when extract(year from now()) - extract(year from cast(substring(start_time, 1, 10) as date)) >= 65 then 1 else 0 end as boolean) as senior,
        |  cast(3423.23 as float) as float_amount,
        |  cast(2342345.13 as double) as double_amount,
        |  cast(32423423.31 as decimal) as decimal_amount
        |from "@test".events e inner join "@test".users u on e.user_id = u.user_id
      |""".stripMargin
    val run: SparkSession => DataFrame = this.load(Map("table" -> query, "column.quote" -> "\""))
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show(false)
  }

  test("Run a query with simple list & struct types") {
    val query = """
        |select
        |  'Gnarly' as name,
        |  7 as age,
        |  CONVERT_FROM('{"name" : "Gnarly", "age": 7, "car": "BMW"}', 'json') as data,
        |  CONVERT_FROM('["apple", "strawberry", "banana"]', 'json') as favorites
      |""".stripMargin
    val run: SparkSession => DataFrame = this.load(Map("table" -> query, "column.quote" -> "\""))
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show()
  }

  test("Run a query with simple map types") {
    val query = """
        |select
        |  'James' as name,
        |  convert_from('["Newark", "NY"]', 'json') as cities,
        |  convert_from('{"city": "Newark", "state": "NY"}', 'json') as address,
        |  convert_from('{"map": [{"Key": "hair", "value": "block"}, {"key": "eye", "value": "brown"}]}', 'json') as prop_1,
        |  convert_from('{"map": [{"key": "height", "value": "5.9"}]}', 'json') as prop_2
        |""".stripMargin
    val run: SparkSession => DataFrame = this.load(Map("table" -> query, "column.quote" -> "\""))
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show()
  }

  test("Query a table with list, struct & map types") {
    /*
      Create a dataframe in spark-shell with the following code, then save the dataframe in parquet files.
      Create a data source pointing the parquet files in Dremio

      val arrayStructureData = Seq(
        Row("James",List(Row("Newark","NY"),
        Row("Brooklyn","NY")),Map("hair"->"black","eye"->"brown"), Map("height"->"5.9")),
        Row("Michael",List(Row("SanJose","CA"),Row("Sandiago","CA")), Map("hair"->"brown","eye"->"black"),Map("height"->"6")),
        Row("Robert",List(Row("LasVegas","NV")), Map("hair"->"red","eye"->"gray"),Map("height"->"6.3")),
        Row("Maria",null,Map("hair"->"blond","eye"->"red"), Map("height"->"5.6")),
        Row("Jen",List(Row("LAX","CA"),Row("Orange","CA")), Map("white"->"black","eye"->"black"),Map("height"->"5.2"))
      )

      val mapType  = DataTypes.createMapType(StringType,StringType)
      val arrayStructureSchema = new StructType()
        .add("name",StringType)
        .add("addresses", ArrayType(new StructType()
        .add("city",StringType)
        .add("state",StringType)))
        .add("properties", mapType)
        .add("secondProp", MapType(StringType,StringType))

        val mapTypeDF = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
        mapTypeDF.printSchema()
        mapTypeDF.show()
     */
    val run: SparkSession => DataFrame = this.load(Map("table" -> "ESG.\"map\"", "column.quote" -> "\""))
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show()
  }

  test("Query a table with partitioning by column") {
    val table = """"azure-wstorage".input.events"""
    val run: SparkSession => DataFrame = this.load(Map("table" -> table, "partition.byColumn" -> "event_id", "partition.size" -> "3"))
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show(10)
  }

  test("Query a table with partitioning by column having date-time lower-bound & upper-bound") {
    val table = """"azure-wstorage".input.events"""
    val run: SparkSession => DataFrame = this.load(Map("table" -> table, "partition.byColumn" -> "start_time", "partition.size" -> "6", "partition.lowerBound" -> "1912-01-01T05:59:24.003Z", "partition.upperBound" -> "3001-04-12T05:00:00.002Z"))
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show(10)
  }

  test("Query a table with partitioning by column having long lower-bound & upper-bound") {
    val query = """
        |select
        |  cast(event_id as bigint) as event_id,
        |  cast(user_id as bigint) as user_id,
        |  start_time,
        |  city,
        |  state,
        |  zip,
        |  country
        |from "azure-wstorage".input.events
        |""".stripMargin
    val run: SparkSession => DataFrame = this.load(Map("table" -> query, "partition.byColumn" -> "event_id", "partition.size" -> "6", "partition.lowerBound" -> "92", "partition.upperBound" -> "4294963817"))
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show()
  }

  test("Query a table with partitioning predicates") {
    val query = """
        |select
        |  cast(event_id as bigint) as event_id,
        |  cast(user_id as bigint) as user_id,
        |  start_time,
        |  city
        |from "azure-wstorage".input.events
        |""".stripMargin
    val predicates = Seq(
      "92 <= event_id and event_id < 715827379", "715827379 <= event_id and event_id < 1431654667", "1431654667 <= event_id and event_id < 2147481954",
      "2147481954 <= event_id and event_id < 2863309242", "2863309242 <= event_id and event_id < 3579136529", "3579136529 <= event_id and event_id < 4294963817"
    )
    val run: SparkSession => DataFrame = this.load(Map("table" -> query, "partition.predicates" -> predicates.mkString(";")))
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show()
  }

  test("Query a table with aggregation") {
    val table = """"local-iceberg".iceberg_db.log_events_iceberg_table_events"""
    val run: SparkSession => DataFrame = this.load(Map("table" -> table))
    val df = this.execute(run)

    df.agg(max(col("float_amount")).as("max_float_amount"), sum(col("double_amount")).as("sum_double_amount")).show()
    df.filter(col("float_amount") >= lit(2.34f)).agg(max(col("float_amount")).as("max_float_amount"), sum(col("double_amount")).as("sum_double_amount")).show()

    //df.limit(10).show() //not supported
    //df.distinct().show()  //not supported

    df.filter(col("float_amount") >= lit(2.34f))
      .groupBy(col("gender"), col("birthyear"))
      .agg(
        countDistinct(col("event_id")).as("distinct_count"),
        count(col("event_id")).as("count"),
        max(col("float_amount")).as("max_float_amount"),
        min(col("float_amount")).as("min_float_amount"),
        //avg(col("decimal_amount")).as("avg_decimal_amount"),  //not supported
        sum_distinct(col("double_amount")).as("distinct_sum_double_amount"),
        sum(col("double_amount")).as("sum_double_amount")
      )
      .show()
  }

  test("Query a table with aggregation with partitioning by hashing") {
    val table = """"local-iceberg".iceberg_db.log_events_iceberg_table_events"""
    val run: SparkSession => DataFrame = this.load(Map("table" -> table, "partition.size" -> "3", "partition.byColumn" -> "event_id"))
    val df = this.execute(run)

    df.agg(max(col("float_amount")).as("max_float_amount"), sum(col("double_amount")).as("sum_double_amount")).show()
    df.filter(col("float_amount") >= lit(2.34f)).agg(max(col("float_amount")).as("max_float_amount"), sum(col("double_amount")).as("sum_double_amount")).show()

    //df.limit(10).show() //not supported
    //df.distinct().show()  //not supported

    df.filter(col("float_amount") >= lit(2.34f))
      .groupBy(col("gender"), col("birthyear"))
      .agg(
        countDistinct(col("event_id")).as("distinct_count"),
        count(col("event_id")).as("count"),
        max(col("float_amount")).as("max_float_amount"),
        min(col("float_amount")).as("min_float_amount"),
        //avg(col("decimal_amount")).as("avg_decimal_amount"),  //not supported
        sum_distinct(col("double_amount")).as("distinct_sum_double_amount"),
        sum(col("double_amount")).as("sum_double_amount")
      )
      .show()
  }

  test("Query a table with aggregation with partitioning by lower/upper bounds") {
    val table = """"local-iceberg".iceberg_db.log_events_iceberg_table_events"""
    val run: SparkSession => DataFrame = this.load(Map("table" -> table, "partition.size" -> "3", "partition.byColumn" -> "start_date", "partition.lowerBound" -> "2012-10-01", "partition.upperBound" -> "2012-12-31"))
    val df = this.execute(run)

    df.agg(max(col("float_amount")).as("max_float_amount"), sum(col("double_amount")).as("sum_double_amount")).show()
    df.filter(col("float_amount") >= lit(2.34f)).agg(max(col("float_amount")).as("max_float_amount"), sum(col("double_amount")).as("sum_double_amount")).show()

    //df.limit(10).show() //not supported
    //df.distinct().show()  //not supported

    df.filter(col("float_amount") >= lit(2.34f))
      .groupBy(col("gender"), col("start_date"))
      .agg(
        countDistinct(col("event_id")).as("distinct_count"),
        count(col("event_id")).as("count"),
        max(col("float_amount")).as("max_float_amount"),
        min(col("float_amount")).as("min_float_amount"),
        //avg(col("decimal_amount")).as("avg_decimal_amount"),  //not supported
        sum_distinct(col("double_amount")).as("distinct_sum_double_amount"),
        sum(col("double_amount")).as("sum_double_amount")
      )
      .show()
  }

  test("Write a simple table") {
    val query = """
        |select
        |  cast(event_id as bigint) as event_id,
        |  gender,
        |  birthyear,
        |  start_time as raw_start_time,
        |  cast(concat(substring(start_time, 1, 10), ' ', substring(start_time, 12, 8)) as timestamp) as real_start_time,
        |  cast(concat(substring(start_time, 1, 10), ' ', substring(start_time, 12, 12)) as timestamp) as milli_start_time,
        |  cast(substring(start_time, 12, 8) as time) as real_time,
        |  cast(substring(start_time, 1, 10) as date) as start_date,
        |  cast(case when extract(year from now()) - extract(year from cast(substring(start_time, 1, 10) as date)) >= 65 then 1 else 0 end as boolean) as senior,
        |  cast(3423.23 as float) as float_amount,
        |  cast(2342345.13 as double) as double_amount,
        |  cast(32423423.31 as decimal) as decimal_amount
        |from "azure-wstorage".input.events e inner join "azure-wstorage".input.users u on e.user_id = u.user_id
      |""".stripMargin
    val runLoad: SparkSession => DataFrame = this.load(Map("table" -> query))
    val df = this.execute(runLoad).cache
    df.printSchema()
    df.show(false)

    val table = """"local-iceberg"."iceberg_db"."log_events_iceberg_table_events""""
    //overwrite
    val overwrite: () => Unit = this.overWrite(Map("table" -> table), df)
    this.execute(overwrite)
    //appendWrite
    val dfAppend = df.withColumn("event_id", col("event_id") + 10000000000L)
    val append: () => Unit = this.appendWrite(Map("table" -> table), dfAppend)
    this.execute(append)
    //merge
    val dfMerge = df
      .withColumn("event_id", when(col("event_id") === lit(42204521L), col("event_id") + 1).otherwise(col("event_id")))
      .withColumn("float_amount", col("float_amount") + 1111)
      .withColumn("double_amount", col("double_amount") + 2222)
      .withColumn("decimal_amount", col("decimal_amount") + 3333)
    val merge: () => Unit = this.mergeWrite(Map("table" -> table, "merge.byColumn" -> "event_id"), dfMerge)
    this.execute(merge)
  }

  test("Append a heavy table") {
    val srcTable = """"azure-wstorage".input.events"""
    val runLoad: SparkSession => DataFrame = this.load(Map("table" -> srcTable))
    val df = this.execute(runLoad).cache

    //appendWrite
    val dstTable = """"local-iceberg"."iceberg_db"."iceberg_events""""
    val dfAppend = df.limit(10000)
      .withColumn("event_id", col("event_id") + 1000000000L)
      .withColumn("remark", lit("new"))
    val overwrite: () => Unit = this.overWrite(Map("table" -> dstTable, "batch.size" -> "768"), dfAppend)
    this.execute(overwrite)
  }

  test("Append a heavy table with complex-type --> string") {
    val srcTable = """"azure-wstorage".input.events"""
    val runLoad: SparkSession => DataFrame = this.load(Map("table" -> srcTable))
    val df = this.execute(runLoad).filter(col("user_id").isin("781622845", "1519813515", "1733137333", "3709565024")).cache

    //the target table
    val dstTable = """"local-iceberg"."iceberg_db"."iceberg_events""""

    //appendWrite for struct
    val dfStruct = df.filter(col("user_id") === lit("781622845") || col("user_id") === lit("3709565024"))
      .withColumn("event_id", col("event_id") + 1000000000L)
      .withColumn("remark", struct(col("city").as("city"), col("state").as("state"), col("country").as("country")))
    val appendStruct: () => Unit = this.appendWrite(Map("table" -> dstTable, "merge.byColumns" -> "event_id,user_id"), dfStruct)
    this.execute(appendStruct)

    //appendWrite for map
    val dfMap = df.filter(col("user_id") === lit("1519813515"))
      .withColumn("event_id", col("event_id") + 1000000000L)
      .withColumn("remark", map(lit("city"), col("city"), lit("state"), col("state"), lit("country"), col("country")))
    val appendMap: () => Unit = this.appendWrite(Map("table" -> dstTable, "merge.byColumns_1" -> "event_id", "merge.byColumns_2" -> "user_id"), dfMap)
    this.execute(appendMap)

    //appendWrite for array
    val dfArray = df.filter(col("user_id") === lit("1733137333"))
      .withColumn("event_id", col("event_id") + 1000000000L)
      .withColumn("remark", array(col("city"), col("state"), col("country")))
    val appendArray: () => Unit = this.appendWrite(Map("table" -> dstTable, "merge.byColumns" -> "event_id;user_id"), dfArray)
    this.execute(appendArray)
  }

  //inserting complex type not supported yet due to un-support on the flight service
  ignore("Write a table with list and struct") {
    val table = """"local-iceberg"."iceberg_db"."log_events_iceberg_struct_list""""
    val runLoad: SparkSession => DataFrame = this.load(Map("table" -> table))
    val df = this.execute(runLoad).cache
    df.printSchema()
    df.show(false)

    val run: () => Unit = this.overWrite(Map("table" -> table), df)
    this.execute(run)
  }

  ignore("Write a table with map") {
    val table = """"local-iceberg"."iceberg_db"."log_events_iceberg_map""""
    val runLoad: SparkSession => DataFrame = this.load(Map("table" -> table))
    val df = this.execute(runLoad).cache
    df.printSchema()
    df.show(false)

    val write: () => Unit = this.appendWrite(Map("table" -> table), df)
    this.execute(write)
  }

  test("Streaming-write a table") {
    val resRoot: String = getClass.getClassLoader.getResource("").getPath

    val fields = Seq(StructField("event_id", StringType), StructField("user_id", StringType),
      StructField("start_time", StringType), StructField("city", StringType), StructField("province", StringType), StructField("country", StringType))
    val streamLoad: SparkSession => DataFrame = this.streamLoad(Map("header" -> "true", "delimiter" -> ","), StructType(fields), s"${resRoot}data/events")
    val df = this.execute(streamLoad)
      .select(col("event_id"), col("user_id"), col("start_time"), col("city"), col("province").as("state"), lit("n/a").as("zip"), col("country"), lit("n/a").as("remark"))
    val streamWrite: () => Unit = this.streamWrite(Map("table" -> """"local-iceberg".iceberg_db.iceberg_events""", "checkpointLocation" -> s"${resRoot}checkpoint/events"), df)
    this.execute(streamWrite)
  }

  //load the data-frame
  private def load(options: Map[String, String])(spark: SparkSession): DataFrame = spark.read.format("flight")
      .option("host", this.dremioHost).option("port", this.dremioPort).option("tls.enabled", dremioTlsEnabled).option("user", this.user).option("password", this.password)
      .options(options)
    .load

  //stream-load
  private def streamLoad(options: Map[String, String], schema: StructType, dataLocation: String)(spark: SparkSession): DataFrame = spark.readStream.format("csv").options(options).schema(schema).load(dataLocation)

  //overwrite with the data-frame
  private def overWrite(options: Map[String, String], df: DataFrame)(): Unit = df.write.format("flight")
      .option("host", this.dremioHost).option("port", this.dremioPort).option("tls.enabled", dremioTlsEnabled).option("user", this.user).option("password", this.password)
      .options(options)
    .mode("overwrite").save

  //appendWrite the data-frame
  private def appendWrite(options: Map[String, String], df: DataFrame)(): Unit = df.write.format("flight")
      .option("host", this.dremioHost).option("port", this.dremioPort).option("tls.enabled", dremioTlsEnabled).option("user", this.user).option("password", this.password)
      .options(options)
    .mode("append").save

  //merge the data-frame
  private def mergeWrite(options: Map[String, String], df: DataFrame)(): Unit = df.write.format("flight")
      .option("host", this.dremioHost).option("port", this.dremioPort).option("tls.enabled", dremioTlsEnabled).option("user", this.user).option("password", this.password)
      .options(options)
    .mode("append").save

  //streaming-write with the data-frame
  private def streamWrite(options: Map[String, String], df: DataFrame)(): Unit = df.writeStream.format("flight")
      .option("host", this.dremioHost).option("port", this.dremioPort).option("tls.enabled", dremioTlsEnabled).option("user", this.user).option("password", this.password)
      .options(options)
    .trigger(Trigger.Once())
    .outputMode(OutputMode.Append())
    .start()
    .awaitTermination(300000)

  //create spark-session
  private var spark: SparkSession = _
  //execute a job
  private def execute[T](read: SparkSession => T): T = read(spark)
  private def execute[T](write: () => T): T = write()

  override def beforeEach(): Unit = spark = SparkSession.builder.master("local[*]").config("spark.executor.memory", "24g").config("spark.driver.memory", "24g").appName("test").getOrCreate
  override def afterEach(): Unit = spark.stop()
}
