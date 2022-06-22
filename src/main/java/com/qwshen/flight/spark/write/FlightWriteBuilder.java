package com.qwshen.flight.spark.write;

import com.qwshen.flight.Configuration;
import com.qwshen.flight.Table;
import com.qwshen.flight.WriteBehavior;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/**
 * The flight write builder to build flight writers
 */
public class FlightWriteBuilder implements WriteBuilder, SupportsTruncate {
    private final Configuration _configuration;
    private final Table _table;
    private final StructType _dataSchema;
    private final WriteBehavior _writeBehavior;

    /**
     * Construct a builder for creating flight writers
     * @param configuration - the configuraton of remote flight service
     * @param table - the table object for describing the target flight table
     * @param dataSchema - the schema of the data being written
     * @param writeBehavior - the write-behavior
     */
    public FlightWriteBuilder(Configuration configuration, Table table, StructType dataSchema, WriteBehavior writeBehavior) {
        this._configuration = configuration;
        this._table = table;
        this._dataSchema = dataSchema;
        this._writeBehavior = writeBehavior;
    }

    @Override
    public Write build() {
        return new FlightWrite(this._configuration, this._table, this._dataSchema, this._writeBehavior);
    }

    /**
     * flag to truncate the target table
     * @return - the write-build which truncates the target table
     */
    @Override
    public WriteBuilder truncate() {
        this._writeBehavior.truncate();
        return this;
    }
}
