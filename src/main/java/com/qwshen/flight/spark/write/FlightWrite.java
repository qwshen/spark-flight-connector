package com.qwshen.flight.spark.write;

import com.qwshen.flight.Configuration;
import com.qwshen.flight.Table;
import com.qwshen.flight.WriteBehavior;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.types.StructType;

/**
 * Defines how to write data to target flight-service
 */
public class FlightWrite implements Write, BatchWrite {
    private final Configuration _configuration;
    private final Table _table;
    private final StructType _dataSchema;
    private final WriteBehavior _writeBehavior;

    /**
     * Construct a flight-write
     * @param configuration - the configuraton of remote flight service
     * @param table - the table object for describing the target flight table
     * @param dataSchema - the schema of data being written
     * @param writeBehavior - the write-behavior
     */
    public FlightWrite(Configuration configuration, Table table, StructType dataSchema, WriteBehavior writeBehavior) {
        this._configuration = configuration;
        this._table = table;
        this._dataSchema = dataSchema;
        this._writeBehavior = writeBehavior;
    }

    @Override
    public BatchWrite toBatch() {
        return this;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
        return new FlightDataWriterFactory(this._configuration, this._table, this._dataSchema, this._writeBehavior);
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message) {
        BatchWrite.super.onDataWriterCommit(message);
    }

    @Override
    public void commit(WriterCommitMessage[] writerCommitMessages) {

    }

    @Override
    public void abort(WriterCommitMessage[] writerCommitMessages) {

    }
}

