package com.qwshen.flight;

import java.io.Serializable;
import java.util.Map;

/**
 * Defines the write-behavior
 */
public class WriteBehavior implements Serializable {
    private final WriteProtocol _protocol;
    private final Map<String, String> _typeMapping;

    private final int _batchSize;
    private final String[] _mergeByColumns;

    //the truncate flag
    private Boolean _truncate = false;

    /**
     * Construct a WriteBehavior
     * @param protocol - the protocol for submitting DML requests. It must be either literal-sql or prepared-sql
     * @param batchSize - the size of each batch to be written
     * @param mergeByColumn - the columns on which to merge data into the target table
     * @param typeMapping - the arrow-type to target data-type mapping
     */
    public WriteBehavior(WriteProtocol protocol, int batchSize, String[] mergeByColumn, Map<String, String> typeMapping) {
        this._protocol = protocol;
        this._batchSize = batchSize;
        this._mergeByColumns = mergeByColumn;
        this._typeMapping = typeMapping;
    }

    /**
     * Get the write-procotol
     * @return - the protocol for writing
     */
    public WriteProtocol getProtocol() {
        return this._protocol;
    }

    /**
     * Get the size of each batch
     * @return - the size of batch for writing
     */
    public int getBatchSize() {
        return this._batchSize;
    }

    /**
     * Get the merge-by columns
     * @return - the columns on which to merge data into the target table
     */
    public String[] getMergeByColumns() {
        return isTruncate() ? new String[0] : this._mergeByColumns;
    }

    /**
     * Get the type-mapping
     * @return - the mapping between arrow-type & target data-types
     */
    public Map<String, String> getTypeMapping() { return this._typeMapping; }

    /**
     * set the flag to truncate the target table
     */
    public void truncate() {
        if (this._mergeByColumns != null && this._mergeByColumns.length > 0) {
            throw new RuntimeException("The merge-by can only work with append mode.");
        }
        this._truncate = true;
    }

    /**
     * Flag to truncate the target table
     * @return - true if it is to truncate the target table
     */
    public Boolean isTruncate() {
        return this._truncate;
    }
}
