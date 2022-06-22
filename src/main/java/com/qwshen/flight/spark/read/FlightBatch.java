package com.qwshen.flight.spark.read;

import com.qwshen.flight.Configuration;
import com.qwshen.flight.Table;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Describes a flight-batch
 */
public class FlightBatch implements Batch, Serializable {
    private final Configuration _configuration;
    private final Table _table;

    /**
     * Construct a FligthBatch for a scan
     * @param configuration - the configuration of remote flight service
     * @param table - the table object
     */
    public FlightBatch(Configuration configuration, Table table) {
        this._configuration = configuration;
        this._table = table;
    }

    /**
     * Plan the input-partitions
     * @return - the logical partitions
     */
    @Override
    public InputPartition[] planInputPartitions() {
        String[] partitionQueries = this._table.getPartitionStatements();
        return (partitionQueries.length > 0)
            ? Arrays.stream(partitionQueries).map(q -> new FlightInputPartition.FlightQueryInputPartition(this._table.getSchema(), q)).toArray(InputPartition[]::new)
            : Arrays.stream(this._table.getEndpoints()).map(e -> new FlightInputPartition.FlightEndpointInputPartition(this._table.getSchema(), e)).toArray(InputPartition[]::new);
    }

    /**
     * Create a partition reader factory
     * @return - the partition reader factory
     */
    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new FlightPartitionReaderFactory(this._configuration);
    }
}
