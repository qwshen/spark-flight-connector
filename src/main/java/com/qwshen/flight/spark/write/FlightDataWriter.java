package com.qwshen.flight.spark.write;

import com.qwshen.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.io.IOException;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Flight DataWriter writes rows to the target flight table
 */
public class FlightDataWriter implements DataWriter<InternalRow> {
    private int _partitionId;
    private long _taskId;
    private String _epochId;

    private final StructType _dataSchema;
    private final Schema _arrowSchema;

    private final WriteStatement _stmt;
    private final int _batchSize;

    private final Client _client;
    private Field[] _fields = null;
    private FlightSqlClient.PreparedStatement _preparedStmt = null;
    private VectorSchemaRoot _root = null;
    private ArrowConversion _conversion = null;

    private final java.util.List<InternalRow> _rows;
    private long _count = 0;

    /**
     * Construct a DataWriter for batch write
     * @param partitionId - the partition id of the data block to be written
     * @param taskId - the task id of the write operation
     * @param configuration - the configuration of remote flight service
     * @param protocol - the protocol for writing - sql or arrow
     * @param stmt - the write-statement
     * @param batchSize - the batch size for write
     */
    public FlightDataWriter(int partitionId, long taskId, Configuration configuration, WriteStatement stmt, WriteProtocol protocol, int batchSize) {
        this(configuration, stmt, protocol, batchSize);
        this._partitionId = partitionId;
        this._taskId = taskId;
        this._epochId = "";
    }

    /**
     * Construct a DataWriter for streaming write
     * @param partitionId - the partition id of the data block to be written
     * @param taskId - the task id of the write operation
     * @param configuration - the configuration of remote flight service
     * @param protocol - the protocol for writing - sql or arrow
     * @param stmt - the write-statement
     * @param batchSize - the batch size for write
     * @param epochId - a monotonically increasing id for streaming queries that are split into discrete periods of execution.
     */
    public FlightDataWriter(int partitionId, long taskId, long epochId, Configuration configuration, WriteStatement stmt, WriteProtocol protocol, int batchSize) {
        this(configuration, stmt, protocol, batchSize);
        this._partitionId = partitionId;
        this._taskId = taskId;
        this._epochId = Long.toString(epochId);
    }

    /**
     * Internal Constructor
     * @param configuration - the configuration of remote flight service
     * @param protocol - the protocol for writing - sql or arrow
     * @param stmt - the write-statement
     * @param batchSize - the batch size for write
     */
    private FlightDataWriter(Configuration configuration, WriteStatement stmt, WriteProtocol protocol, int batchSize) {
        this._stmt = stmt;
        this._batchSize = batchSize;

        this._dataSchema = this._stmt.getDataSchema();
        this._client = Client.getOrCreate(configuration);
        if (protocol == WriteProtocol.PREPARED_SQL) {
            this._preparedStmt = this._client.getPreparedStatement(this._stmt.getStatement());
            this._arrowSchema = this._preparedStmt.getParameterSchema();
            this._fields = this._arrowSchema.getFields().toArray(new Field[0]);
            this._root = VectorSchemaRoot.create(this._arrowSchema, new RootAllocator(Integer.MAX_VALUE));
            this._conversion = ArrowConversion.getOrCreate();
        } else {
            try {
                this._arrowSchema = this._stmt.getArrowSchema();
            } catch (Exception e) {
                throw new RuntimeException("The arrow schema is invalid.", e);
            }
        }
        this._rows = new java.util.ArrayList<>();
    }

    /**
     * Write one row
     * @param row - the row of data
     * @throws IOException - thrown when writing failed
     */
    @Override
    public void write(InternalRow row) throws IOException {
        this._rows.add(row.copy());
        if (this._rows.size() > this._batchSize) {
            this.write(this._rows.toArray(new InternalRow[0]));
            this._rows.clear();
        }
    }
    /**
     * Write out all rows
     * @param rows - the data rows
     */
    private void write(InternalRow[] rows) {
        if (this._conversion != null) {
            Function<String, DataType> dtFind = (name) -> this._dataSchema.find(field -> field.name().equalsIgnoreCase(name)).map(StructField::dataType).get();
            IntStream.range(0, this._fields.length).forEach(idx -> this._conversion.populate(this._root.getVector(idx), rows, idx, dtFind.apply(this._fields[idx].getName())));
            this._root.setRowCount(rows.length);
            this._preparedStmt.setParameters(this._root);
            try {
                this._client.executeUpdate(this._preparedStmt);
            } finally {
                this._preparedStmt.clearParameters();
                this._root.clear();
            }
        } else {
            this._client.execute(this._stmt.fillStatement(rows, this._arrowSchema.getFields().toArray(new Field[0])));
        }
        this._count += rows.length;
    }

    /**
     * Commit write
     * @return - a commit-message
     */
    @Override
    public WriterCommitMessage commit() {
        //write any left-over
        if (this._rows.size() > 0) {
            this.write(this._rows.toArray(new InternalRow[0]));
            this._rows.clear();
        }

        long cnt = this._count;
        this._count = 0;
        return (this._epochId.length() == 0) ? new FlightWriterCommitMessage(this._partitionId, this._taskId, cnt)
            : new FlightWriterCommitMessage(this._partitionId, this._taskId, this._epochId, cnt);
    }

    /**
     * Abort write
     * @throws IOException - the exception with the error message
     */
    @Override
    public void abort() throws IOException {
        throw (this._epochId.length() == 0) ? new FlightWriteAbortException(this._partitionId, this._taskId, this._count)
            : new FlightWriteAbortException(this._partitionId, this._taskId, this._epochId, this._count);
    }

    /**
     * Close any connections
     */
    @Override
    public void close() {
        if (this._preparedStmt != null) {
            this._preparedStmt.close();
        }
        if (this._root != null) {
            this._root.close();
        }
    }
}
