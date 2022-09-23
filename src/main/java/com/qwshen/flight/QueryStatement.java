package com.qwshen.flight;

import java.io.Serializable;

/**
 * The read statement for querying data from remote flight service
 */
public class QueryStatement implements Serializable {
    private final String _stmt;
    private final String _where;
    private final String _groupBy;

    /**
     * Construct a ReadStatement
     * @param stmt - the select portion of a select-statement
     * @param where - the where portion of a select-statement
     * @param groupBy - the groupBy portion of a select-statement
     */
    public QueryStatement(String stmt, String where, String groupBy) {
        this._stmt = stmt;
        this._where = where;
        this._groupBy = groupBy;
    }

    /**
     * Check if the current ReadStatement is different from the input ReadStatement
     * @param rs - one ReadStatement to be compared
     * @return - true if they are different
     */
    public boolean different(QueryStatement rs) {
        boolean changed = (rs == null || !rs._stmt.equalsIgnoreCase(this._stmt));
        if (!changed) {
            changed = (rs._where != null) ? !rs._where.equalsIgnoreCase(this._where) : this._where != null;
        }
        if (!changed) {
            changed = (rs._groupBy != null) ? !rs._groupBy.equalsIgnoreCase(this._groupBy) : this._groupBy != null;
        }
        return changed;
    }

    /**
     * Get the whole select-statement
     * @return - the select-statement
     */
    public String getStatement() {
        return String.format("%s %s %s", this._stmt,
            (this._where != null && this._where.length() > 0) ? String.format("where %s", this._where) : "",
            (this._groupBy != null && this._groupBy.length() > 0) ? String.format("group by %s", this._groupBy) : ""
        );
    }
}
