package com.qwshen.flight;

import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Describes row batch from remote flight service
 */
public class RowSet implements Serializable {
    /**
     * The definition of Row
     */
    public static class Row implements Serializable {
        private final ArrayList<Object> _data;

        public Row() {
            this._data = new ArrayList<>();
        }

        public void add(Object o) {
            this._data.add(o);
        }

        public Object[] getData() {
            return this._data.toArray(new Object[0]);
        }
    }

    //the schema of each ROw
    private final Schema _schema;
    //the row collection
    private final ArrayList<Row> _data;

    /**
     * Construct a RowSet
     * @param schema - the schema of each row in the collection
     */
    public RowSet(Schema schema) {
        this._schema = schema;
        this._data = new ArrayList<>();
    }

    /**
     * Get the schema of the RowSet
     * @return - the schema
     */
    public Schema getSchema() {
        return this._schema;
    }

    /**
     * Add one Row
     * @param row - the row to be added
     */
    public void add(Row row) {
        this._data.add(row);
    }

    /**
     * Add all rows from another RowSet
     * @param rs - the input RowSet
     */
    public void add(RowSet rs) {
        if (rs._schema != this._schema) {
            throw new RuntimeException("The schema doesn't match. Cannot add the RowSet.");
        }
        this._data.addAll(rs._data);
    }

    /**
     * Get all Rows
     * @return - all rows in the RowSet
     */
    public Row[] getData() {
        return this._data.toArray(new Row[] {});
    }
}
