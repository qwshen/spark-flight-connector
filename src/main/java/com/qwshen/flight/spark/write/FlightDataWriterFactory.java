package com.qwshen.flight.spark.write;

import com.qwshen.flight.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Defines the FLightDataWriterFactory to create DataWriters
 */
public class FlightDataWriterFactory implements DataWriterFactory {
    private final Configuration _configuration;
    private final WriteStatement _stmt;
    private final StructType _dataSchema;
    //TODO: remove _jsonSchema
    private final String _jsonSchema;
    private final int _batchSize;

    /**
     * Construct a flight-write
     * @param configuration - the configuraton of remote flight service
     * @param table - the table object for describing the target flight table
     * @param dataSchema - the schema of data being written
     * @param writeBehavior - the write-behavior
     */
    public FlightDataWriterFactory(Configuration configuration, Table table, StructType dataSchema, WriteBehavior writeBehavior) {
        this._configuration = configuration;
        this._stmt = table.getWriteStatement(writeBehavior.getMergeByColumns(), dataSchema);
        this._dataSchema = dataSchema;
        //TODO: remove _jsonSchema
        this._jsonSchema = new Schema(Arrays.stream(this._stmt.getParams()).map(p -> table.getSchema().findField(p)).collect(Collectors.toList()), table.getSchema().getCustomMetadata()).toJson();
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
     * Create a DataWriter
     * @param partitionId - the partition id
     * @param taskId - the task id
     * @return - a DataWriter
     */
    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        //TODO: remove _jsonSchema
        return new FlightDataWriter(partitionId, taskId, this._configuration, this._dataSchema, this._jsonSchema, this._stmt.getStatement(), this._batchSize);
    }
}
