package com.qwshen.flight.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

object implicits {
  /**
   * Flight DataFrameReader is to load data into a DataFrame from remote flight service
   * @param r - the data-frame reader
   */
  implicit class FlightDataFrameReader(r: DataFrameReader) {
    /**
     * Given the format as flight
     * @return - data-frame loaded from remote flight service
     */
    def flight(): DataFrame = r.format("flight").load()

    /**
     * Given the format as flight with an input table
     * @param table - the name of a table being queried against
     * @return - data-frame loaded from remote flight service
     */
    def flight(table: String): DataFrame = r.format("flight").option("table", table).load()
  }

  /**
   * Flight DataFrameWriter is to write a DataFrame into remote flight service
   * @param w - the data-frame writer
   */
  implicit class FlightDataFrameWriter(r: DataFrameWriter[org.apache.spark.sql.DataFrame]) {
    /**
     * Given the format as flight
     */
    def flight(): Unit = r.format("flight").save()

    /**
     * Given the format as flight with an input table
     * @param table - the name of a table being written into
     */
    def flight(table: String): Unit = r.format("flight").option("table", table).save()
  }
}
