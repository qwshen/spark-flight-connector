package com.qwshen.flight.spark.write;

import com.qwshen.flight.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.LoggerFactory;
import java.util.Arrays;

/**
 * Defines the FLightDataWriterFactory to create DataWriters
 */
public class FlightDataWriterFactory implements DataWriterFactory, StreamingDataWriterFactory {
    private final Configuration _configuration;

    private final WriteStatement _stmt;
    private final WriteProtocol _protocol;
    private final int _batchSize;

    /**
     * Construct a flight-write
     * @param configuration - the configuration of remote flight service
     * @param table - the table object for describing the target flight table
     * @param dataSchema - the schema of data being written
     * @param writeBehavior - the write-behavior
     */
    public FlightDataWriterFactory(Configuration configuration, Table table, StructType dataSchema, WriteBehavior writeBehavior) {
        this._configuration = configuration;
        this._stmt = (writeBehavior.getMergeByColumns() == null || writeBehavior.getMergeByColumns().length == 0)
            ? new WriteStatement(table.getName(), dataSchema, table.getSchema(), table.getColumnQuote(), writeBehavior.getTypeMapping())
            : new WriteStatement(table.getName(), writeBehavior.getMergeByColumns(), dataSchema, table.getSchema(), table.getColumnQuote(), writeBehavior.getTypeMapping());
        this._protocol = writeBehavior.getProtocol();
        this._batchSize = writeBehavior.getBatchSize();

        //truncate the table if requested
        if (writeBehavior.isTruncate()) {
            this.truncate(table.getName());
        }
    }

    /**
     * truncate the target table
     * @param table - the name of the table
     */
    private void truncate(String table) {
        try {
            Client.getOrCreate(this._configuration).truncate(table);
        } catch (Exception e) {
            LoggerFactory.getLogger(this.getClass()).error(e.getMessage() + " --> " + Arrays.toString(e.getStackTrace()));
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a DataWriter for batch-write
     * @param partitionId - the partition id
     * @param taskId - the task id
     * @return - a DataWriter
     */
    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new FlightDataWriter(partitionId, taskId, this._configuration, this._stmt, this._protocol, this._batchSize);
    }

    /**
     * Create a DataWriter for streaming-write
     * @param partitionId - the partition id
     * @param taskId - the task id
     * @param epochId - a monotonically increasing id for streaming queries that are split into discrete periods of execution.
     * @return - a DataWriter
     */
    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
        return new FlightDataWriter(partitionId, taskId, epochId, this._configuration, this._stmt, this._protocol, this._batchSize);
    }
}
