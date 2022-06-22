package com.qwshen.flight.spark.test

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest.FunSuite

class DremioTest extends FunSuite {
  private val dremioHost = "192.168.0.26"
  private val dremioPort = "32010"
  private val dremioTlsEnabled = false;
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
    df.show()
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
    df.show()
  }

  test("Query a table with partitioning by column having date-time lower-bound & upper-bound") {
    val table = """"azure-wstorage".input.events"""
    val run: SparkSession => DataFrame = this.load(Map("table" -> table, "partition.byColumn" -> "start_time", "partition.size" -> "6", "partition.lowerBound" -> "1912-01-01T05:59:24.003Z", "partition.upperBound" -> "3001-04-12T05:00:00.002Z"), None, Nil)
    val df = this.execute(run)
    df.printSchema()
    df.count()
    df.show()
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
    val run: SparkSession => Unit = this.save(Map("table" -> table), df)
    this.execute(run)
  }

  test("Write a table with list and struct") {
    val table = """"local-iceberg"."iceberg_db"."log_events_iceberg_struct_list""""
    val runLoad: SparkSession => DataFrame = this.load(Map("table" -> table), None, Nil)
    val df = this.execute(runLoad).cache
    df.printSchema()
    df.show(false)

    val run: SparkSession => Unit = this.save(Map("table" -> table), df)
    this.execute(run)
  }

  test("Write a table with map") {
    val table = """"local-iceberg"."iceberg_db"."log_events_iceberg_map""""
    val runLoad: SparkSession => DataFrame = this.load(Map("table" -> table), None, Nil)
    val df = this.execute(runLoad).cache
    df.printSchema()
    df.show(false)

    val run: SparkSession => Unit = this.save(Map("table" -> table), df)
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

  //save the data-frame
  private def save(options: Map[String, String], df: DataFrame)(spark: SparkSession): Unit = {
    df.write.format("flight")
      .option("host", this.dremioHost).option("port", this.dremioPort).option("tls.enabled", dremioTlsEnabled).option("user", this.user).option("password", this.password)
      .options(options)
      .mode("overwrite").save
  }

  //create spark-session
  private def createSparkSession(): SparkSession = SparkSession.builder.appName("test").master("local[*]").getOrCreate
  //execute a job
  private def execute[T](run: SparkSession => T): T = {
    val spark = createSparkSession();
    try {
      run(spark)
    } finally {
      //stopSparkSession(spark)
    }
  }
  //stop spark-session
  private def stopSparkSession(spark: SparkSession): Unit = spark.stop()
}
