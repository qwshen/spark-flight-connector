package com.qwshen.flight.spark.write;

import org.apache.spark.sql.connector.write.WriterCommitMessage;
import java.io.Serializable;

/**
 * Describes the flight writer commit message
 */
public class FlightWriterCommitMessage implements WriterCommitMessage, Serializable {
    public final int partitionId;
    public final long taskId;
    public final boolean success;
    public final long size;
    public final String error;

    /**
     * Construct a success Flight-Writer-Commit-Message
     * @param partitionId - the partition-id of the data-frame been written
     * @param taskId - the task id of the writing operation
     * @param size - number of rows been written
     */
    public FlightWriterCommitMessage(int partitionId, long taskId, long size) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.size = size;
        this.success = true;
        this.error = "";
    }

    /**
     * Construct a failure Flight-Writer-Commit-Message
     * @param partitionId - the partition-id of the data-frame been written
     * @param taskId - the task id of the writing operation
     * @param size - number of rows been written
     * @param error - the error message describing the root or cause of the failure.
     */
    public FlightWriterCommitMessage(int partitionId, long taskId, long size, String error) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.size = size;
        this.success = false;
        this.error = error;
    }
}
