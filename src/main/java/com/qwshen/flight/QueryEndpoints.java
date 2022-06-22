package com.qwshen.flight;

import org.apache.arrow.vector.types.pojo.Schema;
import java.io.Serializable;

/**
 * Describes the data-structure from executing a query on remote flight service
 */
public class QueryEndpoints implements Serializable {
    //the schema
    private final Schema _schema;
    //the collection of end-points exposed for the query
    private final Endpoint[] _endpoints;

    /**
     * Construct a QueryEndpoints
     * @param schema - the schema of the query result
     * @param endpoints - end end-points exposed on the remote flight-service for fetching data
     */
    public QueryEndpoints(Schema schema, Endpoint[] endpoints) {
        this._schema = schema;
        this._endpoints = endpoints;
    }

    /**
     * Get the Schema
     * @return - the schema of the QueryEndpoints
     */
    public Schema getSchema() {
        return this._schema;
    }

    /**
     * Get the end-points
     * @return - the end-points of the QueryEndpoints
     */
    public Endpoint[] getEndpoints() {
        return this._endpoints;
    }
}
