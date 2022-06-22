package com.qwshen.flight.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader}

object implicits {
  /**
   * Flight DataFrame to load data into a DataFrame from remote flight service
   * @param r - the data-frame reader
   */
  implicit class FlightDataFrame(r: DataFrameReader) {
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
}
