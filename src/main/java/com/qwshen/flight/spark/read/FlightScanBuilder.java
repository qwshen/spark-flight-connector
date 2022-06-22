package com.qwshen.flight.spark.read;

import com.qwshen.flight.Configuration;
import com.qwshen.flight.PartitionBehavior;
import com.qwshen.flight.Table;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.StructType;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

/**
 * Build flight scans which supports pushed-down filter, fields & aggregates
 */
public final class FlightScanBuilder implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsPushDownAggregates {
    private final Configuration _configuration;
    private final Table _table;
    private final PartitionBehavior _partitionBehavior;

    //the pushed-down filters
    private Filter[] _pdFilters = new Filter[0];
    //the pushed-down columns
    private String[] _pdColumns = new String[0];
    //only supports pushed-down COUNT
    private boolean _pdCount = false;

    /**
     * Construct a flight-scan builder
     * @param configuration - the configuration of remote flight-service
     * @param table - the table instance representing a table in remote flight-service
     * @param partitionBehavior - the partitioning behavior when loading data from remote flight service
     */
    public FlightScanBuilder(Configuration configuration, Table table, PartitionBehavior partitionBehavior) {
        this._configuration = configuration;
        this._table = table;
        this._partitionBehavior = partitionBehavior;
    }

    /**
     * Only COUNT is supported for now
     * @param aggregation - the pushed aggregation
     * @return - true if COUNT is pushed
     */
    @Override
    public boolean pushAggregation(Aggregation aggregation) {
        this._pdCount = Arrays.stream(aggregation.aggregateExpressions()).map(e -> e.toString().equalsIgnoreCase("count(*)")).findFirst().orElse(false);
        return this._pdCount;
    }

    /**
     * For SupportsPushDownFilters interface
     * @param filters - the pushed-down filters
     * @return - not-accepted filters
     */
    @Override
    public Filter[] pushFilters(Filter[] filters) {
        Function<Filter, Boolean> isValid = (filter) -> (filter instanceof IsNotNull || filter instanceof IsNull
            || filter instanceof EqualTo || filter instanceof EqualNullSafe
            || filter instanceof LessThan || filter instanceof LessThanOrEqual || filter instanceof GreaterThan || filter instanceof GreaterThanOrEqual
            || filter instanceof StringStartsWith || filter instanceof StringContains || filter instanceof StringEndsWith
            || filter instanceof And || filter instanceof Or || filter instanceof Not || filter instanceof In
        );

        java.util.List<Filter> pushed = new java.util.ArrayList<>();
        try {
            return Arrays.stream(filters).map(filter -> {
                if (isValid.apply(filter)) {
                    pushed.add(filter);
                    return null;
                } else {
                    return filter;
                }
            }).filter(Objects::nonNull).toArray(Filter[]::new);
        } finally {
            this._pdFilters = pushed.toArray(new Filter[0]);
        }
    }

    /**
     * For SupportsPushDownFilters interface
     * @return - the pushed-down filters
     */
    @Override
    public Filter[] pushedFilters() {
        return this._pdFilters;
    }

    /**
     * For SupportsPushDownRequiredColumns interface
     * @param columns - the schema containing the required columns
     */
    @Override
    public void pruneColumns(StructType columns) {
        this._pdColumns = columns.fieldNames();
    }

    /**
     * To build a flight-scan
     * @return - A fligh scan
     */
    @Override
    public Scan build() {
        //adjust flight-table upon pushed filters & columns
        String where = String.join(" and ", Arrays.stream(this._pdFilters).map(this._table::toWhereClause).toArray(String[]::new));
        if (this._table.probe(where, this._pdColumns, this._pdCount, this._partitionBehavior)) {
            this._table.initialize(this._configuration);
        }
        return new FlightScan(this._configuration, this._table);
    }
}
