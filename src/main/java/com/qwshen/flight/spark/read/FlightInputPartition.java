package com.qwshen.flight.spark.read;

import com.qwshen.flight.Endpoint;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.read.InputPartition;
import java.io.IOException;
import java.io.Serializable;

/**
 * Describes the data-structure of flight input-partitions
 */
public class FlightInputPartition implements InputPartition, Serializable {
    /**
     * The input-partition with end-points
     */
    public static class FlightEndpointInputPartition extends FlightInputPartition {
        private final Endpoint _ep;

        /**
         * Construct a flight end-point input-partition
         * @param schema - the schema of the partition
         * @param ep - the end-point of the partition
         */
        public FlightEndpointInputPartition(Schema schema, Endpoint ep) {
            super(schema);
            this._ep = ep;
        }

        /**
         * Get the end-point of the input-partition
         * @return - the end-point of the input partition
         */
        public Endpoint getEndpoint() {
            return this._ep;
        }
    }

    /**
     * The input-partition with query
     */
    public static class FlightQueryInputPartition extends FlightInputPartition {
        private final String _query;

        /**
         * Construct a flight query input-partition
         * @param schema - the schema of the partition
         * @param query - the query for the partition
         */
        public FlightQueryInputPartition(Schema schema, String query) {
            super(schema);
            this._query = query;
        }

        /**
         * Get the query of the input-partition
         * @return - the query of the input-partition
         */
        public String getQuery() {
            return this._query;
        }
    }

    //the schema of the input partition
    private final String _schema;

    /**
     * Construct a flight-input-partition
     * @param schema - the schema of the partition
     */
    protected FlightInputPartition(Schema schema) {
        this._schema = schema.toJson();
    }

    /**
     * Get the schema of the partition
     * @return - the schema of the partition
     * @throws IOException - thrown when the schema is in invalid json format.
     */
    public Schema getSchema() throws IOException {
        return Schema.fromJSON(this._schema);
    }
}
