package com.qwshen.flight;

import java.io.Serializable;

/**
 * The write statement for writing data to remote flight service
 */
public class WriteStatement implements Serializable {
    //the statement in the format of either merge into or insert into sql statement
    public final String _stmt;
    //the parameter name in the WriteStatement
    public final String[] _params;

    /**
     * Construct a WriteStatement
     * @param stmt - the merge into or insert into statement
     * @param params - the name of parameters
     */
    public WriteStatement(String stmt, String[] params) {
        this._stmt = stmt;
        this._params = params;
    }

    /**
     * Get the statement
     * @return - the merge into or insert into statement
     */
    public String getStatement() {
        return this._stmt;
    }

    /**
     * Get the name of the parameters in the statement
     * @return - the name of parameters
     */
    public String[] getParams() {
        return this._params;
    }
}
