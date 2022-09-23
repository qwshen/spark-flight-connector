package com.qwshen.flight;

import java.io.Serializable;

/**
 * Describes the data structure for pushed-down aggregation
 */
public class PushAggregation implements Serializable {
    //pushed-down Aggregate-Columns (expressions)
    private String[] _columnExpressions = null;
    //pushed-down GroupBy-Columns
    private String[] _groupByColumns = null;

    /**
     * Push down aggregation of columns
     *   select max(age), sum(distinct amount) from table where ...
     * @param columnExpressions - the collection of aggregation expressions
     */
    public PushAggregation(String[] columnExpressions) {
        this._columnExpressions = columnExpressions;
    }

    /**
     * Push down aggregation with group by columns
     *   select max(age), sum(amount) from table where ... group by gender
     * @param columnExpressions - the collection of aggregation expressions
     * @param groupByColumns - the columns in group by
     */
    public PushAggregation(String[] columnExpressions, String[] groupByColumns) {
        this(columnExpressions);
        this._groupByColumns = groupByColumns;
    }

    /**
     * Return the collection of aggregation expressions
     * @return - the expressions
     */
    public String[] getColumnExpressions() {
        return this._columnExpressions;
    }

    /**
     * The columns for group-by
     * @return - columns
     */
    public String[] getGroupByColumns() {
        return this._groupByColumns;
    }
}
