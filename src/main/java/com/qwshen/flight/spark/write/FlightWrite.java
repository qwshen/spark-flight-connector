package com.qwshen.flight.spark.write;

import com.qwshen.flight.Configuration;
import com.qwshen.flight.Table;
import com.qwshen.flight.WriteBehavior;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Defines how to write data to target flight-service
 */
public class FlightWrite implements Write, BatchWrite, StreamingWrite {
    private final Logger _logger = LoggerFactory.getLogger(this.getClass());

    private final Configuration _configuration;
    private final Table _table;
    private final StructType _dataSchema;
    private final WriteBehavior _writeBehavior;

    /**
     * Construct a flight-write
     * @param configuration - the configuration of remote flight service
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
    public StreamingWrite toStreaming() {
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
    public void commit(WriterCommitMessage[] messages) {
        if (this._logger.isDebugEnabled()) {
            this._logger.info(this.concat.apply(messages));
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        this._logger.error(this.concat.apply(messages));
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo var1) {
        return new FlightDataWriterFactory(this._configuration, this._table, this._dataSchema, this._writeBehavior);
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        if (this._logger.isDebugEnabled()) {
            this._logger.info(this.concat.apply(this.adapt.apply(epochId, messages)));
        }
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        this._logger.error(this.concat.apply(this.adapt.apply(epochId, messages)));
    }

    //concatenate all commit messages
    private final Function<WriterCommitMessage[], String> concat = (messages) -> {
        String[] details = Arrays.stream(messages).map(message -> (message instanceof FlightWriterCommitMessage) ? ((FlightWriterCommitMessage)message).getMessage() : "").toArray(String[]::new);
        return String.join(System.lineSeparator(), details);
    };
    //adapt all commit messages
    private final BiFunction<Long, WriterCommitMessage[], WriterCommitMessage[]> adapt = (epochId, messages) -> Arrays.stream(messages).map(message -> (message instanceof FlightWriterCommitMessage) ? new FlightWriterCommitMessage((FlightWriterCommitMessage)message, epochId) : message).toArray(WriterCommitMessage[]::new);
}

