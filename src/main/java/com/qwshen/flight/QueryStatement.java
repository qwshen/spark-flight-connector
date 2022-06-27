package com.qwshen.flight;

import java.io.Serializable;

/**
 * The read statement for querying data from remote flight service
 */
public class QueryStatement implements Serializable {
    private final String _stmt;
    private final String _where;

    /**
     * Construct a ReadStatement
     * @param stmt - the select portion of a select-statement
     * @param where - the where portion of a select-statement
     */
    public QueryStatement(String stmt, String where) {
        this._stmt = stmt;
        this._where = where;
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
        return changed;
    }

    /**
     * Get the whole select-statement
     * @return - the select-statement
     */
    public String getStatement() {
        return (this._where != null && this._where.length() > 0) ? String.format("%s where %s", this._stmt, this._where) : this._stmt;
    }
}
