package com.qwshen.flight.spark.read;

import com.qwshen.flight.Configuration;
import com.qwshen.flight.Table;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import java.io.Serializable;

/**
 * Describes the data-structure of FlightScan
 */
public class FlightScan implements Scan, Serializable {
    private final Configuration _configuration;
    private final Table _table;

    /**
     * Construct a FligthScan
     * @param configuration - the configuration of remote flight service
     * @param table - the table object
     */
    public FlightScan(Configuration configuration, Table table) {
        this._configuration = configuration;
        this._table = table;
    }

    /**
     * Get the schema of the scan
     * @return - the scan for the scan
     */
    @Override
    public StructType readSchema() {
        return this._table.getSparkSchema();
    }

    /**
     * The description of the scan
     * @return - description
     */
    @Override
    public String description() {
        return this._table.getQueryStatement();
    }

    /**
     * Translate the scan to batch
     * @return - the batch desribes the scan
     */
    @Override
    public Batch toBatch() {
        return new FlightBatch(this._configuration, this._table);
    }
}
