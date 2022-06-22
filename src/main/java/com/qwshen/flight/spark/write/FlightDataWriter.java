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
    private final int _partitionId;
    private final long _taskId;

    private final StructType _dataSchema;
    private final Field[] _fields;
    private final int _batchSize;

    private final Client _client;
    private final FlightSqlClient.PreparedStatement _preparedStmt = null;

    private final VectorSchemaRoot _root;
    private final java.util.List<InternalRow> _rows;
    private final Vector _vector;

    /**
     * Construct a DataWriter
     * @param partitionId - the partition id of the data block to be written
     * @param taskId - the task id of the write operation
     * @param configuration - the configuration of remote flight service
     * @param jsonSchema - the flight schema matching the data being written
     * @param stmt - the write-statement
     * @param batchSize - the batch size for the write
     */
    public FlightDataWriter(int partitionId, long taskId, Configuration configuration, StructType dataSchema, String jsonSchema, String stmt, int batchSize) {
        this._partitionId = partitionId;
        this._taskId = taskId;

        this._dataSchema = dataSchema;
        try {
            this._client = Client.getOrCreate(configuration);
            //this._preparedStmt = this._client.getPreparedStatement(stmt);
            //final Schema schema = this._preparedStmt.getParameterSchema();
            //TODO: remove the schema from JSON
            Schema schema = Schema.fromJSON(jsonSchema);

            this._fields = schema.getFields().toArray(new Field[0]);
            this._root = VectorSchemaRoot.create(schema, new RootAllocator(Integer.MAX_VALUE));
        } catch (Exception e){
            throw new RuntimeException(e);
        }
        this._batchSize = batchSize;
        this._rows = new java.util.ArrayList<>();
        this._vector = Vector.getOrCreate();
    }

    @Override
    public void write(InternalRow row) throws IOException {
        this._rows.add(row);
        if (this._rows.size() > this._batchSize) {
            this.write(this._rows.toArray(new InternalRow[0]));
            this._rows.clear();
        }
    }

    private void write(InternalRow[] rows) {
        Function<String, DataType> dtFind = (name) -> this._dataSchema.find(field -> field.name().equalsIgnoreCase(name)).map(StructField::dataType).get();
        IntStream.range(0, this._fields.length).forEach(idx -> this._vector.populate(this._root.getVector(idx), rows, idx, dtFind.apply(this._fields[idx].getName())));
        this._root.setRowCount(rows.length);
        this._preparedStmt.setParameters(this._root);
        if (this._client.executeUpdate(this._preparedStmt) != rows.length) {

        }
        this._preparedStmt.clearParameters();
        this._root.clear();
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        //write any left-over
        if (this._rows.size() > 0) {
            this.write(this._rows.toArray(new InternalRow[0]));
            this._rows.clear();
        }
        //construct a commit message
        return null;
    }

    @Override
    public void abort() throws IOException {

    }

    @Override
    public void close() throws IOException {
        this._preparedStmt.close();
        this._root.close();
    }
}
