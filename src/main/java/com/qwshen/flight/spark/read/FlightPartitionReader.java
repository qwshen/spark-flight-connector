package com.qwshen.flight.spark.read;

import com.qwshen.flight.Client;
import com.qwshen.flight.Configuration;
import com.qwshen.flight.QueryEndpoints;
import com.qwshen.flight.RowSet;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import java.io.IOException;
import java.util.Arrays;

/**
 * Describes a partition-reader for reading partitions from remote flight service
 */
public class FlightPartitionReader implements PartitionReader<InternalRow> {
    private final Configuration _configuration;
    private final InputPartition _inputPartition;

    //hold all rows
    private RowSet.Row[] _rows = null;
    //the row pointer
    private int _curIdx = 0;

    /**
     * Constructor a partion reader
     * @param configuration - the configuration of remote flight service
     * @param inputPartition - the input-partition
     */
    public FlightPartitionReader(Configuration configuration, InputPartition inputPartition) {
        this._configuration = configuration;
        this._inputPartition = inputPartition;
    }

    //fetch data from remote flight service
    private void execute() throws IOException {
        if (this._inputPartition instanceof FlightInputPartition.FlightEndpointInputPartition) {
            this.executeEndpoint((FlightInputPartition.FlightEndpointInputPartition)this._inputPartition);
        } else if (this._inputPartition instanceof FlightInputPartition.FlightQueryInputPartition) {
            this.executeQuery((FlightInputPartition.FlightQueryInputPartition)this._inputPartition);
        }
    }
    //fetch data by end-point
    private void executeEndpoint(FlightInputPartition.FlightEndpointInputPartition dePartition) throws IOException {
        try {
            Client client = Client.getOrCreate(this._configuration);
            //fetch the data
            this._rows = client.fetch(dePartition.getEndpoint(), dePartition.getSchema()).getData();
            //reset the pointer
            this._curIdx = 0;
        } catch (Exception e) {
            LoggerFactory.getLogger(this.getClass()).error(e.getMessage() + Arrays.toString(e.getStackTrace()));
            throw new IOException(e);
        }
    }
    //fetch data by query
    private void executeQuery(FlightInputPartition.FlightQueryInputPartition dqPartition) throws IOException {
        try {
            Client client = Client.getOrCreate(this._configuration);
            //get all end-points of the query
            QueryEndpoints qeps = client.getQueryEndpoints(dqPartition.getQuery());
            //fetch the data
            this._rows = client.fetch(qeps).getData();
            //reset the pointer
            this._curIdx = 0;
        } catch (Exception e) {
            LoggerFactory.getLogger(this.getClass()).error(e.getMessage() + Arrays.toString(e.getStackTrace()));
            throw new IOException(e);
        }
    }

    /**
     * Move to the next row
     * @return - true if next row is available, otherwise false
     * @throws IOException - throws if the read fails
     */
    @Override
    public boolean next() throws IOException {
        if (this._rows == null) {
            this.execute();
        }
        return (this._rows != null && this._curIdx < this._rows.length);
    }

    /**
     * Get the current row
     * @return - the current row
     */
    @Override
    public InternalRow get() {
        return InternalRow.fromSeq(JavaConverters.asScalaBuffer(Arrays.asList(this._rows[this._curIdx++].getData())).toSeq());
    }

    /**
     * Close the reader
     */
    @Override
    public void close() {
    }
}
