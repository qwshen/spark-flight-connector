package com.qwshen.flight.spark.write;

import org.apache.spark.sql.connector.write.WriterCommitMessage;
import java.io.Serializable;

/**
 * Describes the flight writer commit message
 */
public class FlightWriterCommitMessage implements WriterCommitMessage, Serializable {
    private final int _partitionId;
    private final long _taskId;
    private final String _epochId;
    private final long _messageCount;

    /**
     * Construct a success Flight-Writer-Commit-Message
     * @param partitionId - the partition-id of the data-frame been written
     * @param taskId - the task id of the writing operation
     * @param messageCount - number of rows been written
     */
    public FlightWriterCommitMessage(int partitionId, long taskId, long messageCount) {
        this._partitionId = partitionId;
        this._taskId = taskId;
        this._epochId = "";
        this._messageCount = messageCount;
    }

    /**
     * Construct a failure Flight-Writer-Commit-Message
     * @param partitionId - the partition-id of the data-frame been written
     * @param taskId - the task id of the writing operation
     * @param epochId - the epoch-id for streaming write.
     * @param messageCount - number of rows been written
     */
    public FlightWriterCommitMessage(int partitionId, long taskId, String epochId, long messageCount) {
        this._partitionId = partitionId;
        this._taskId = taskId;
        this._epochId = epochId;
        this._messageCount = messageCount;
    }

    /**
     * Construct a failure Flight-Writer-Commit-Message
     * @param message - the base write commit message
     * @param epochId - the epoch-id for streaming write.
     */
    public FlightWriterCommitMessage(FlightWriterCommitMessage message, long epochId) {
        this._partitionId = message._partitionId;
        this._taskId = message._taskId;
        this._epochId = Long.toString(epochId);
        this._messageCount = message._messageCount;
    }

    /**
     * form the committed message
     * @return - the commit message
     */
    public String getMessage() {
        return (this._epochId == null || this._epochId.length() == 0) ? String.format("Streaming write for %d messages with partition (%d), task (%d) committed.", this._messageCount, this._partitionId, this._taskId)
            : String.format("Streaming write for %d messages with partition (%d), task (%d) and epoch (%s) committed.", this._messageCount, this._partitionId, this._taskId, this._epochId);
    }
}
