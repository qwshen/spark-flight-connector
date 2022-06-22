package com.qwshen.flight.spark.read;

import com.qwshen.flight.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/**
 * The flight partition-reader factory for creating flight partition-readers
 */
public class FlightPartitionReaderFactory implements PartitionReaderFactory {
    private final Configuration _configuration;

    /**
     * Construct a flight partition-reader factory
     * @param configuration - the configuration of remote flight service
     */
    public FlightPartitionReaderFactory(Configuration configuration) {
        this._configuration = configuration;
    }

    /**
     * Create a reader
     * @param inputPartition - the input-partition for the reader
     * @return - a partition-reader
     */
    public PartitionReader<InternalRow> createReader(InputPartition inputPartition) {
        return new FlightPartitionReader(this._configuration, inputPartition);
    }
}
