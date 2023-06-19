package com.qwshen.flight.spark;

import com.qwshen.flight.*;
import com.qwshen.flight.spark.read.FlightScanBuilder;
import com.qwshen.flight.spark.write.FlightWriteBuilder;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Describes the flight-table
 */
public class FlightTable implements org.apache.spark.sql.connector.catalog.Table, SupportsRead, SupportsWrite {
    //the default type-mapping
    private static final String _TYPE_MAPPING_DEFAULT = "BIT:BOOLEAN;"
        + "LARGEVARCHAR:VARCHAR; VARCHAR:VARCHAR;"
        + "TINYINT:INT; SMALLINT:INT; UINT1:INT; UINT2:INT; UINT4:INT; UINT8:INT; INT:INT; BIGINT:BIGINT;"
        + "FLOAT4:FLOAT; FLOAT8:DOUBLE; DECIMAL:DECIMAL; DECIMAL256:DECIMAL;"
        + "DATE:DATE; TIME:TIME; TIMESTAMP:TIMESTAMP";
    //the number of partitions when reading and writing
    private static final String PARTITION_SIZE = "partition.size";
    //the option keys for partitioning
    private static final String PARTITION_HASH_FUN = "partition.hashFunc";
    private static final String PARTITION_BY_COLUMN = "partition.byColumn";
    private static final String PARTITION_LOWER_BOUND = "partition.lowerBound";
    private static final String PARTITION_UPPER_BOUND = "partition.upperBound";
    private static final String PARTITION_PREDICATE = "partition.predicate";
    private static final String PARTITION_PREDICATES = "partition.predicates";

    //write protocol
    private static final String WRITE_PROTOCOL = "write.protocol";
    //type mapping
    private static final String WRITE_TYPE_MAPPING = "write.typeMapping";
    //the batch-size for writing
    private static final String BATCH_SIZE = "batch.size";
    //merge by keys
    private static final String MERGE_BY_COLUMN = "merge.byColumn";
    private static final String MERGE_BY_COLUMNS = "merge.byColumns";

    //the configuration of remote flight service
    private final Configuration _configuration;
    //the table description
    private final Table _table;

    //the table capabilities
    private final Set<TableCapability> _capabilities = new HashSet<>();

    /**
     * Construct a flight-table
     * @param configuration  - the configuration of remote flight-service
     * @param table - the table instance pointing to a remote flight-table
     */
    public FlightTable(Configuration configuration, Table table) {
        this._configuration = configuration;
        this._table = table;

        //the data-source supports batch read/write, truncate the table
        this._capabilities.add(TableCapability.ACCEPT_ANY_SCHEMA);
        this._capabilities.add(TableCapability.BATCH_READ);
        this._capabilities.add(TableCapability.BATCH_WRITE);
        this._capabilities.add(TableCapability.TRUNCATE);
        this._capabilities.add(TableCapability.STREAMING_WRITE);
    }

    /**
     * For Table interface
     * @return - the name of the table
     */
    @Override
    public String name() {
        return this._table.getName();
    }

    /**
     * For Table interface
     * @return - the schema for Spark
     */
    @Override
    public StructType schema() {
        return this._table.getSparkSchema();
    }

    /**
     * For Table interface
     * @return - the capabilities of this table
     */
    @Override
    public Set<TableCapability> capabilities() {
        return this._capabilities;
    }

    /**
     * For SupportsRead interface
     * @param options - the options from the api call
     * @return - A scan builder
     */
    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        //the partitioning behavior
        PartitionBehavior partitionBehavior = new PartitionBehavior(
            options.getOrDefault(FlightTable.PARTITION_HASH_FUN, "hash"), options.getOrDefault(FlightTable.PARTITION_BY_COLUMN, ""),
            Integer.parseInt(options.getOrDefault(FlightTable.PARTITION_SIZE, "6")),
            options.getOrDefault(FlightTable.PARTITION_LOWER_BOUND, ""), options.getOrDefault(FlightTable.PARTITION_UPPER_BOUND, ""),
            (String[]) ArrayUtils.addAll(
                //filter out any partition.predicates for only partition.predicate
                options.keySet().stream()
                    .filter(k -> !k.equalsIgnoreCase(FlightTable.PARTITION_PREDICATES) && k.toLowerCase().startsWith(FlightTable.PARTITION_PREDICATE.toLowerCase()))
                    .map(k -> options.getOrDefault(k, "")).filter(p -> !p.isEmpty()).toArray(String[]::new),
                //combine with partition.predicates
                options.containsKey(FlightTable.PARTITION_PREDICATES) ? options.get(FlightTable.PARTITION_PREDICATES).split("[;|,]") : new String[0]
            )
        );
        return new FlightScanBuilder(this._configuration, this._table, partitionBehavior);
    }

    /**
     * For SupportsWrite interface
     * @param logicalWriteInfo - the logical information for the writing
     * @return - A write builder
     */
    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
        //validate the schema - fields being written must be in the table schema
        Function<String, Boolean> exists = (field) -> this._table.getSparkSchema().exists(sf -> sf.name().equalsIgnoreCase(field));
        if (!Arrays.stream(logicalWriteInfo.schema().fields()).map(sf -> exists.apply(sf.name())).reduce((x, y) -> x & y).orElse(false)) {
            throw new RuntimeException("The schema of the dataframe being written is not compatible with the schema of the underlying table.");
        }

        //construct write-behavior
        CaseInsensitiveStringMap options = logicalWriteInfo.options();
        WriteBehavior writeBehavior = new WriteBehavior(
            //by default, the write-protocol is submitting literal sql statements
            options.getOrDefault(FlightTable.WRITE_PROTOCOL, "literal-sql").equalsIgnoreCase("prepared-sql") ? WriteProtocol.PREPARED_SQL : WriteProtocol.LITERAL_SQL,
            //by default, the batch-size is 10,240.
            Integer.parseInt(options.getOrDefault(FlightTable.BATCH_SIZE, "1024")),
                ArrayUtils.addAll(
                    //filter out any merge.ByColumns for only merge.ByColumn
                    options.keySet().stream()
                            .filter(k -> !k.equalsIgnoreCase(FlightTable.MERGE_BY_COLUMNS) && k.toLowerCase().startsWith(FlightTable.MERGE_BY_COLUMN.toLowerCase()))
                        .map(k -> options.getOrDefault(k, "")).filter(p -> !p.isEmpty()).toArray(String[]::new),
                    //combine with merge.ByColumns
                    options.containsKey(FlightTable.MERGE_BY_COLUMNS) ? options.get(FlightTable.MERGE_BY_COLUMNS).split("[;|,]") : new String[0]
                ),
            Arrays.stream(options.getOrDefault(FlightTable.WRITE_TYPE_MAPPING, FlightTable._TYPE_MAPPING_DEFAULT).split(";"))
                .map(tm -> Arrays.stream(tm.split(":")).map(s -> s.trim().toLowerCase()).toArray(String[]::new)).collect(Collectors.toMap(s -> s[0], s -> s[1]))
        );
        return new FlightWriteBuilder(this._configuration, this._table, logicalWriteInfo.schema(), writeBehavior);
    }
}
