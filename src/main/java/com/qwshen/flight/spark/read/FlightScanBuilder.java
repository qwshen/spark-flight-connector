package com.qwshen.flight.spark.read;

import com.qwshen.flight.Configuration;
import com.qwshen.flight.PartitionBehavior;
import com.qwshen.flight.PushAggregation;
import com.qwshen.flight.Table;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.expressions.aggregate.*;
import org.apache.spark.sql.connector.read.*;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    private StructField[] _pdColumns = new StructField[0];

    //pushed-down aggregation
    private PushAggregation _pdAggregation = null;

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
     * Collection aggregations that will be pushed down
     * @param aggregation - the pushed aggregation
     * @return - pushed aggregation
     */
    @Override
    public boolean pushAggregation(Aggregation aggregation) {
        Function<String, String> quote = (s) -> String.format("%s%s%s", this._table.getColumnQuote(), s, this._table.getColumnQuote());
        Function<String[], String> mks = (ss) -> String.join(",", Arrays.stream(ss).map(quote).toArray(String[]::new));
        Function<Expression, String> e2s = (e) -> String.join(",", Arrays.stream(e.references()).map(r -> mks.apply(r.fieldNames())).toArray(String[]::new));

        List<String> pdAggregateColumns = new ArrayList<>();
        boolean push = true;
        for (AggregateFunc agg : aggregation.aggregateExpressions()) {
            if (agg instanceof CountStar) {
                pdAggregateColumns.add(agg.toString().toLowerCase());
            } else if (agg instanceof Count) {
                Count c = (Count)agg;
                pdAggregateColumns.add(c.isDistinct() ? String.format("count(distinct(%s))", e2s.apply(c.column())) : String.format("count(%s)", e2s.apply(c.column())));
            } else if (agg instanceof Min) {
                Min m = (Min)agg;
                pdAggregateColumns.add(String.format("min(%s)", e2s.apply(m.column())));
            } else if (agg instanceof Max) {
                Max m = (Max)agg;
                pdAggregateColumns.add(String.format("max(%s)", e2s.apply(m.column())));
            } else if (agg instanceof Sum) {
                Sum s = (Sum)agg;
                pdAggregateColumns.add(s.isDistinct() ? String.format("sum(distinct(%s))", e2s.apply(s.column())) : String.format("sum(%s)", e2s.apply(s.column())));
            } else {
                push = false;
                break;
            }
        }
        if (push) {
            String[] pdGroupByColumns = Arrays.stream(aggregation.groupByExpressions()).flatMap(e -> Arrays.stream(e.references()).flatMap(r -> Arrays.stream(r.fieldNames()).map(quote))).toArray(String[]::new);
            pdAggregateColumns.addAll(0, Arrays.asList(pdGroupByColumns));
            this._pdAggregation = pdGroupByColumns.length > 0 ? new PushAggregation(pdAggregateColumns.toArray(new String[0]), pdGroupByColumns) : new PushAggregation(pdAggregateColumns.toArray(new String[0]));
        } else {
            this._pdAggregation = null;
        }
        return this._pdAggregation != null;
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
        this._pdColumns = columns.fields();
    }

    /**
     * To build a flight-scan
     * @return - A flight scan
     */
    @Override
    public Scan build() {
        //adjust flight-table upon pushed filters & columns
        String where = String.join(" and ", Arrays.stream(this._pdFilters).map(this._table::toWhereClause).toArray(String[]::new));
        if (this._table.probe(where, this._pdColumns, this._pdAggregation, this._partitionBehavior)) {
            this._table.initialize(this._configuration);
        }
        return new FlightScan(this._configuration, this._table);
    }
}
