package com.qwshen.flight.spark.test

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, col, count, countDistinct, lit, map, max, min, struct, sum, sum_distinct, when}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class DremioTest extends FunSuite with BeforeAndAfterEach {
  private val dremioHost = "192.168.0.22"
  private val dremioPort = "32010"
  private val dremioTlsEnabled = false
  private val user = "test"
  private val password = "Password@12345"

  test("Run a simple query") {
    val query = """select * from "azure-wstorage".input.users"""
    val run: SparkSession => DataFrame = this.load(Map("table" -> query, "column.quote" -> "\""), None, Nil)
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show()
  }

  test("Run a simple query with filter & fields") {
    val query = """select * from "azure-wstorage".input.users"""
    val filter = Some("""(birthyear >= 1988 and birthyear < 1997) or (gender = 'female' and "joined-at" like '2012-11%')""")
    val fields = Seq("user_id", "joined-at")
    val run: SparkSession => DataFrame = this.load(Map("table" -> query, "column.quote" -> "\""), filter, fields)
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show()
  }

  test("Query a table with filter & fields") {
    val table = """"azure-wstorage".input.users"""
    val filter = Some("""(birthyear >= 1988 and birthyear < 1997) or (gender = 'female' and "joined-at" like '2012-11%')""")
    val fields = Seq("user_id", "joined-at")
    val run: SparkSession => DataFrame = this.load(Map("table" -> table, "column.quote" -> "\""), filter, fields)
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show()
  }

  test("Query a join-table with filter & fields") {
    val table = """"azure-wstorage".input.events e left join "azure-wstorage".input.users u on e.user_id = u.user_id"""
    val filter = Some("""birthyear is not null""")
    val fields = Seq("user_name", "gender", "event_id", "start_time")
    val run: SparkSession => DataFrame = this.load(Map("table" -> table, "column.quote" -> "\""), filter, fields)
    val df = this.execute(run)
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
        |from "azure-wstorage".input.events e inner join "azure-wstorage".input.users u on e.user_id = u.user_id
      |""".stripMargin
    val run: SparkSession => DataFrame = this.load(Map("table" -> query, "column.quote" -> "\""), None, Nil)
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
    val run: SparkSession => DataFrame = this.load(Map("table" -> query, "column.quote" -> "\""), None, Nil)
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show()
  }

  test("Run a query with simple map types") {
    val query = """
        |select
        |  'James' as name,
        |  convert_from('{"city": "Newark", "state": "NY"}', 'json') as address,
        |  convert_from('{"map": [{"Key": "hair", "value": "block"}, {"key": "eye", "value": "brown"}]}', 'json') as prop_1,
        |  convert_from('{"map": [{"key": "height", "value": "5.9"}]}', 'json') as prop_2
        |""".stripMargin
    val run: SparkSession => DataFrame = this.load(Map("table" -> query, "column.quote" -> "\""), None, Nil)
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show()
  }

  test("Query a table with partitioning by column") {
    val table = """"azure-wstorage".input.events"""
    val run: SparkSession => DataFrame = this.load(Map("table" -> table, "partition.byColumn" -> "event_id", "partition.size" -> "3"), None, Nil)
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show(10)
  }

  test("Query a table with partitioning by column having date-time lower-bound & upper-bound") {
    val table = """"azure-wstorage".input.events"""
    val run: SparkSession => DataFrame = this.load(Map("table" -> table, "partition.byColumn" -> "start_time", "partition.size" -> "6", "partition.lowerBound" -> "1912-01-01T05:59:24.003Z", "partition.upperBound" -> "3001-04-12T05:00:00.002Z"), None, Nil)
    val df = this.execute(run)//.filter(col("event_id") === lit(3928440935L))
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
    val run: SparkSession => DataFrame = this.load(Map("table" -> query, "partition.byColumn" -> "event_id", "partition.size" -> "6", "partition.lowerBound" -> "92", "partition.upperBound" -> "4294963817"), None, Nil)
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
    val run: SparkSession => DataFrame = this.load(Map("table" -> query, "partition.predicates" -> predicates.mkString(";")), None, Nil)
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show()
  }

  test("Query a table with aggregation") {
    val table = """"local-iceberg".iceberg_db.log_events_iceberg_table_events"""
    val run: SparkSession => DataFrame = this.load(Map("table" -> table), None, Nil)
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
    val run: SparkSession => DataFrame = this.load(Map("table" -> table, "partition.size" -> "3", "partition.byColumn" -> "event_id"), None, Nil)
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
    val run: SparkSession => DataFrame = this.load(Map("table" -> table, "partition.size" -> "3", "partition.byColumn" -> "start_date", "partition.lowerBound" -> "2012-10-01", "partition.upperBound" -> "2012-12-31"), None, Nil)
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
    val runLoad: SparkSession => DataFrame = this.load(Map("table" -> query), None, Nil)
    val df = this.execute(runLoad).cache
    df.printSchema()
    df.show(false)

    val table = """"local-iceberg"."iceberg_db"."log_events_iceberg_table_events""""
    //overwrite
    val overwrite: SparkSession => Unit = this.overwrite(Map("table" -> table), df)
    this.execute(overwrite)
    //append
    val dfAppend = df.withColumn("event_id", col("event_id") + 10000000000L)
    val append: SparkSession => Unit = this.append(Map("table" -> table), dfAppend)
    this.execute(append)
    //merge
    val dfMerge = df
      .withColumn("event_id", when(col("event_id") === lit(42204521L), col("event_id") + 1).otherwise(col("event_id")))
      .withColumn("float_amount", col("float_amount") + 1111)
      .withColumn("double_amount", col("double_amount") + 2222)
      .withColumn("decimal_amount", col("decimal_amount") + 3333)
    val merge: SparkSession => Unit = this.merge(Map("table" -> table, "merge.byColumn" -> "event_id"), dfMerge)
    this.execute(merge)
  }

  test("Append a heavy table") {
    val srcTable = """"azure-wstorage".input.events"""
    val runLoad: SparkSession => DataFrame = this.load(Map("table" -> srcTable), None, Nil)
    val df = this.execute(runLoad).cache

    //append
    val dstTable = """"local-iceberg"."iceberg_db"."iceberg_events""""
    val dfAppend = df.limit(10000)
      .withColumn("event_id", col("event_id") + 1000000000L)
      .withColumn("remark", lit("new"))
    val overwrite: SparkSession => Unit = this.overwrite(Map("table" -> dstTable, "batch.size" -> "768"), dfAppend)
    this.execute(overwrite)
  }

  test("Append a heavy table with complex-type --> string") {
    val srcTable = """"azure-wstorage".input.events"""
    val runLoad: SparkSession => DataFrame = this.load(Map("table" -> srcTable), None, Nil)
    val df = this.execute(runLoad).filter(col("user_id").isin("781622845", "1519813515", "1733137333", "3709565024")).cache

    //the target table
    val dstTable = """"local-iceberg"."iceberg_db"."iceberg_events""""

    //append for struct
    val dfStruct = df.filter(col("user_id") === lit("781622845") || col("user_id") === lit("3709565024"))
      .withColumn("event_id", col("event_id") + 1000000000L)
      .withColumn("remark", struct(col("city").as("city"), col("state").as("state"), col("country").as("country")))
    val appendStruct: SparkSession => Unit = this.append(Map("table" -> dstTable, "merge.byColumns" -> "event_id,user_id"), dfStruct)
    this.execute(appendStruct)

    //append for map
    val dfMap = df.filter(col("user_id") === lit("1519813515"))
      .withColumn("event_id", col("event_id") + 1000000000L)
      .withColumn("remark", map(lit("city"), col("city"), lit("state"), col("state"), lit("country"), col("country")))
    val appendMap: SparkSession => Unit = this.append(Map("table" -> dstTable, "merge.byColumns_1" -> "event_id", "merge.byColumns_2" -> "user_id"), dfMap)
    this.execute(appendMap)

    //append for array
    val dfArray = df.filter(col("user_id") === lit("1733137333"))
      .withColumn("event_id", col("event_id") + 1000000000L)
      .withColumn("remark", array(col("city"), col("state"), col("country")))
    val appendArray: SparkSession => Unit = this.append(Map("table" -> dstTable, "merge.byColumns" -> "event_id;user_id"), dfArray)
    this.execute(appendArray)
  }

  //inserting complex type not supported yet due to un-support on the flight service
  ignore("Write a table with list and struct") {
    val table = """"local-iceberg"."iceberg_db"."log_events_iceberg_struct_list""""
    val runLoad: SparkSession => DataFrame = this.load(Map("table" -> table), None, Nil)
    val df = this.execute(runLoad).cache
    df.printSchema()
    df.show(false)

    val run: SparkSession => Unit = this.overwrite(Map("table" -> table), df)
    this.execute(run)
  }

  ignore("Write a table with map") {
    val table = """"local-iceberg"."iceberg_db"."log_events_iceberg_map""""
    val runLoad: SparkSession => DataFrame = this.load(Map("table" -> table), None, Nil)
    val df = this.execute(runLoad).cache
    df.printSchema()
    df.show(false)

    val run: SparkSession => Unit = this.append(Map("table" -> table), df)
    this.execute(run)
  }

  //load the data-frame
  private def load(options: Map[String, String], where: Option[String], fields: Seq[String])(spark: SparkSession): DataFrame = {
    val dfInit = spark.read.format("flight")
      .option("host", this.dremioHost).option("port", this.dremioPort).option("tls.enabled", dremioTlsEnabled).option("user", this.user).option("password", this.password)
      .options(options).load
    (where, fields) match {
      case (Some(filter), Seq(_, _ @ _*)) => dfInit.filter(filter).select(fields.map(field => col(field)): _*)
      case (_, Seq(_, _ @ _*)) => dfInit.select(fields.map(field => col(field)): _*)
      case (Some(filter), _) => dfInit.filter(filter)
      case (_, _) => dfInit
    }
  }

  //overwrite with the data-frame
  private def overwrite(options: Map[String, String], df: DataFrame)(spark: SparkSession): Unit = {
    df.write.format("flight")
        .option("host", this.dremioHost).option("port", this.dremioPort).option("tls.enabled", dremioTlsEnabled).option("user", this.user).option("password", this.password)
        .options(options)
      .mode("overwrite").save
  }
  //append the data-frame
  private def append(options: Map[String, String], df: DataFrame)(spark: SparkSession): Unit = {
    df.write.format("flight")
        .option("host", this.dremioHost).option("port", this.dremioPort).option("tls.enabled", dremioTlsEnabled).option("user", this.user).option("password", this.password)
        .options(options)
      .mode("append").save
  }
  //merge the data-frame
  private def merge(options: Map[String, String], df: DataFrame)(spark: SparkSession): Unit = {
    df.write.format("flight")
        .option("host", this.dremioHost).option("port", this.dremioPort).option("tls.enabled", dremioTlsEnabled).option("user", this.user).option("password", this.password)
        .options(options)
      .mode("append").save
  }

  //create spark-session
  private var spark: SparkSession = _
  //execute a job
  private def execute[T](run: SparkSession => T): T = run(spark)

  override def beforeEach(): Unit = spark = SparkSession.builder.master("local[*]").config("spark.executor.memory", "24g").config("spark.driver.memory", "24g").appName("test").getOrCreate
  override def afterEach(): Unit = spark.stop()
}
