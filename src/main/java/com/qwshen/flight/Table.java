package com.qwshen.flight;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Describes a flight table
 */
public final class Table implements Serializable {
    //the name of a flight table whose data will be queried/updated
    private final String _name;
    //the character for quoting columns in sql statements
    private final String _columnQuote;

    //the read-statement
    private QueryStatement _stmt;

    //the spark schema
    private StructType _sparkSchema = null;
    //the flight schema
    private Schema _schema = null;
    //the end-points exposed by the remote flight-service for fetching data of this table
    private Endpoint[] _endpoints = new Endpoint[0];

    //the container for holding the partitioning queries
    private final java.util.List<String> _partitionStmts = new java.util.ArrayList<>();

    /**
     * Construct a Table object
     * @param name - the name of the table
     * @param columnQuote - the character for quoting columns in sql statements
     */
    private Table(String name, String columnQuote) {
        this._name = name;
        this._columnQuote = columnQuote;

        this.prepareQueryStatement(null, null, null, null);
    }

    /**
     * Get the name of the table
     * @return - the name of the table
     */
    public String getName() {
        return this._name;
    }

    /**
     * Get the sql-statement for querying the table
     * @return - the physical query which will be submitted to remote flight service
     */
    public String getQueryStatement() {
        if (this._stmt == null) {
            throw new RuntimeException("The read statement is not valid.");
        }
        return this._stmt.getStatement();
    }

    /**
     * Get the partition queries
     * @return - the partition queries with each of which is submitted from a spark executor
     */
    public String[] getPartitionStatements() {
        return this._partitionStmts.toArray(new String[0]);
    }

    /**
     * Get the spark schema
     * @return - the spark schema
     */
    public StructType getSparkSchema() {
        return this._sparkSchema;
    }

    /**
     * Get the end-points
     * @return - end-points exposed by the remote flight service upon submitted query
     */
    public Endpoint[] getEndpoints() {
        return this._endpoints;
    }

    /**
     * Get the flight schema
     * @return - the flight schema
     */
    public Schema getSchema() {
        return this._schema;
    }

    /**
     * Get the character for quoting columns
     * @return - the character for quoting columns
     */
    public String getColumnQuote() {
        return this._columnQuote;
    }

    /**
     * Initialize the schema and end-points by submitting the physical query
     * @param config - the connection configuration
     */
    public void initialize(Configuration config) {
        try {
            Client client = Client.getOrCreate(config);
            QueryEndpoints eps = client.getQueryEndpoints(this.getQueryStatement());

            this._sparkSchema = new StructType(Arrays.stream(Field.from(eps.getSchema())).map(fs -> new StructField(fs.getName(), FieldType.toSpark(fs.getType()), true, Metadata.empty())).toArray(StructField[]::new));
            this._schema = eps.getSchema();
            this._endpoints = eps.getEndpoints();
        } catch (Exception e) {
            LoggerFactory.getLogger(this.getClass()).error(e.getMessage() + " --> " + Arrays.toString(e.getStackTrace()));
            throw new RuntimeException(e);
        }
    }

    //Prepare the query for submitting to remote flight service
    private boolean prepareQueryStatement(PushAggregation aggregation, StructField[] fields, String filter, PartitionBehavior partitionBehavior) {
        //aggregation mode: 0 -> no aggregation; 1 -> aggregation without group-by; 2 -> aggregation with group-by
        int aggMode = 0;
        String select = "", groupBy = "";
        if (aggregation != null) {
            String[] groupByFields = aggregation.getGroupByColumns();
            if (groupByFields != null && groupByFields.length > 0) {
                aggMode = 2;
                groupBy = String.join(",", groupByFields);
            } else {
                aggMode = 1;
            }
            select = String.format("select %s from %s", String.join(",", aggregation.getColumnExpressions()), this._name);
        } else if (fields != null && fields.length > 0) {
            select = String.format("select %s from %s", String.join(",", Arrays.stream(fields).map(column -> String.format("%s%s%s", this._columnQuote, column.name(), this._columnQuote)).toArray(String[]::new)), this._name);
        } else {
            select = String.format("select * from %s", this._name);
        }
        QueryStatement stmt = new QueryStatement(select, filter, groupBy);
        boolean changed = stmt.different(this._stmt);
        if (changed) {
            this._stmt = stmt;
        }

        if (aggMode == 1) {
            this._partitionStmts.clear();
        } else if (partitionBehavior != null && partitionBehavior.enabled()) {
            String where = (filter != null && !filter.isEmpty()) ? String.format("(%s) and ", filter) : "";
            BiFunction<StructField[], StructField[], StructField[]> merge = (s1, s2) -> {
                Hashtable<String, StructField> s = new Hashtable<String, StructField>();
                for (StructField sf : s1) {
                    s.put(sf.name(), sf);
                }
                for (StructField sf : s2) {
                    s.put(sf.name(), sf);
                }
                return s.values().toArray(new StructField[0]);
            };
            String[] predicates = partitionBehavior.predicateDefined() ? partitionBehavior.getPredicates()
                : partitionBehavior.calculatePredicates(this._sparkSchema == null ? fields : merge.apply(fields, this._sparkSchema.fields()));
            for (String predicate : predicates) {
                QueryStatement s = new QueryStatement(select, String.format("%s(%s)", where, predicate), groupBy);
                this._partitionStmts.add(s.getStatement());
            }
        }
        return changed;
    }

    //translate a filter to where clause
    public String toWhereClause(Filter filter) {
        StringBuilder sb = new StringBuilder();
        if (filter instanceof EqualTo) {
            EqualTo et = (EqualTo)filter;
            sb.append((et.value() instanceof Number)
                ? String.format("%s%s%s = %s", this._columnQuote, et.attribute(), this._columnQuote, et.value().toString())
                : String.format("%s%s%s = '%s'", this._columnQuote, et.attribute(), this._columnQuote, et.value().toString())
            );
        } else if (filter instanceof EqualNullSafe) {
            EqualNullSafe ens = (EqualNullSafe)filter;
            sb.append(String.format("((%s%s%s is null and %s is null) or (%s%s%s is not null and %s is not null))", this._columnQuote, ens.attribute(), this._columnQuote, ens.value(), this._columnQuote, ens.attribute(), this._columnQuote, ens.value()));
        } else if (filter instanceof LessThan) {
            LessThan lt = (LessThan)filter;
            sb.append((lt.value() instanceof Number) ? String.format("%s%s%s < %s", this._columnQuote, lt.attribute(), this._columnQuote, lt.value()) : String.format("%s%s%s < '%s'", this._columnQuote, lt.attribute(), this._columnQuote, lt.value()));
        } else if (filter instanceof LessThanOrEqual) {
            LessThanOrEqual lt = (LessThanOrEqual)filter;
            sb.append((lt.value() instanceof Number) ? String.format("%s%s%s <= %s", this._columnQuote, lt.attribute(), this._columnQuote, lt.value()) : String.format("%s%s%s <= '%s'", this._columnQuote, lt.attribute(), this._columnQuote, lt.value()));
        } else if (filter instanceof GreaterThan) {
            GreaterThan gt = (GreaterThan)filter;
            sb.append((gt.value() instanceof Number) ? String.format("%s%s%s > %s", this._columnQuote, gt.attribute(), this._columnQuote, gt.value()) : String.format("%s%s%s > '%s'", this._columnQuote, gt.attribute(), this._columnQuote, gt.value()));
        } else if (filter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual gt = (GreaterThanOrEqual)filter;
            sb.append((gt.value() instanceof Number) ? String.format("%s%s%s >= %s", this._columnQuote, gt.attribute(), this._columnQuote, gt.value()) : String.format("%s%s%s >= '%s'", this._columnQuote, gt.attribute(), this._columnQuote, gt.value()));
        } else if (filter instanceof And) {
            And and = (And)filter;
            sb.append(String.format("(%s and %s)", toWhereClause(and.left()), toWhereClause(and.right())));
        } else if (filter instanceof Or) {
            Or or = (Or)filter;
            sb.append(String.format("(%s or %s)", toWhereClause(or.left()), toWhereClause(or.right())));
        } else if (filter instanceof IsNull) {
            IsNull in = (IsNull)filter;
            sb.append(String.format("%s%s%s is null", this._columnQuote, in.attribute(), this._columnQuote));
        } else if (filter instanceof IsNotNull) {
            IsNotNull in = (IsNotNull)filter;
            sb.append(String.format("%s%s%s is not null", this._columnQuote, in.attribute(), this._columnQuote));
        } else if (filter instanceof StringStartsWith) {
            StringStartsWith ss = (StringStartsWith)filter;
            sb.append(String.format("%s%s%s like '%s%s'", this._columnQuote, ss.attribute(), this._columnQuote, ss.value(), "%"));
        } else if (filter instanceof StringContains) {
            StringContains sc = (StringContains)filter;
            sb.append(String.format("%s%s%s like '%s%s%s'", this._columnQuote, sc.attribute(), this._columnQuote, "%", sc.value(), "%"));
        } else if (filter instanceof StringEndsWith) {
            StringEndsWith se = (StringEndsWith)filter;
            sb.append(String.format("%s%s%s like '%s%s'", this._columnQuote, se.attribute(), this._columnQuote, "%", se.value()));
        } else if (filter instanceof Not) {
            Not not = (Not)filter;
            sb.append(String.format("not (%s)", toWhereClause(not.child())));
        } else if (filter instanceof In) {
            In in = (In)filter;
            sb.append(String.format("%s%s%s in (%s)", this._columnQuote, in.attribute(), this._columnQuote, String.join(",", Arrays.stream(in.values()).map(v -> (v instanceof Number) ? v.toString() : String.format("'%s'", v.toString())).toArray(String[]::new))));
        }
        return sb.toString();
    }

    /**
     * Probe if the pushed filter, fields and aggregation would affect the existing schema & end-points
     * @param pushedFilter - the pushed filter
     * @param pushedFields - the pushed fields
     * @param pushedAggregation - the pushed aggregation
     * @param partitionBehavior - the partitioning behavior
     * @return - true if initialization is required
     */
    public Boolean probe(String pushedFilter, StructField[] pushedFields, PushAggregation pushedAggregation, PartitionBehavior partitionBehavior) {
        if ((pushedFilter == null || pushedFilter.isEmpty()) && (pushedFields == null || pushedFields.length == 0) && pushedAggregation == null) {
            return false;
        }
        return this.prepareQueryStatement(pushedAggregation, pushedFields, pushedFilter, partitionBehavior);
    }

    /**
     * Table with name
     * @param tableName - the name of a table
     * @param columnQuote - the character for quoting columns in sql statements
     * @return - a Table object
     */
    public static Table forTable(String tableName, String columnQuote) {
        Function<String, Boolean> isQuery = (t) -> t.replaceAll("[\r|\n]", " ").trim().toLowerCase().matches("^select .+ [from]?.+");
        return new Table(isQuery.apply(tableName) ? String.format("(%s) t", tableName) : tableName, columnQuote);
    }
}
