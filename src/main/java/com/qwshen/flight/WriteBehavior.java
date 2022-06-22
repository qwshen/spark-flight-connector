package com.qwshen.flight;

import java.io.Serializable;

/**
 * Defines the write-behavior
 */
public class WriteBehavior implements Serializable {
    private final int _numPartitions;
    private final int _batchSize;
    private final String[] _mergeByColumns;

    //the truncate flag
    private Boolean _truncate = false;

    /**
     * Construct a WriteBehavior
     * @param numPartitions - the number of partitions when writing
     * @param batchSize - the size of each batch to be written
     * @param mergeByColumn - the columns on which to merge data into the target table
     */
    public WriteBehavior(int numPartitions, int batchSize, String[] mergeByColumn) {
        this._numPartitions = numPartitions;
        this._batchSize = batchSize;
        this._mergeByColumns = mergeByColumn;
    }

    /**
     * Get the number of partitions
     * @return - the number of partitions for writing
     */
    public int getNumPartitions() {
        return this._numPartitions;
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
        return this._mergeByColumns;
    }

    /**
     * set the flag to truncate the target table
     */
    public void truncate() {
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
