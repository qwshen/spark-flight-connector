package com.qwshen.flight;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.types.*;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.time.format.DateTimeFormatter;

/**
 * The write statement for writing data to remote flight service
 */
public class WriteStatement implements Serializable {
    //the execute interface
    @FunctionalInterface
    private interface execute<X, Y, Z, R> {
        R apply(X x, Y y, Z z);
    }
    //the convert interface
    @FunctionalInterface
    private interface convert<A, B, C, D, R> {
        R apply(A a, B b, C c, D d);
    }
    //the values variable
    private static final String _varValues = "${param_Values}";
    //the to-object-converter container
    private final java.util.Map<String, execute<Object, Field, DataType, String>> _converters;

    //data schema
    private final StructType _dataSchema;
    //arrow schema
    private final String _arrowSchema;
    //the parameter name in the WriteStatement
    private final String[] _params;

    //the statement in the format of either merge into or insert into sql statement
    private String _stmt;

    //the following is the mapping between arrow-types to data-types that the target flight end-point supports
    private String _mapTinyInt = "int";
    private String _mapSmallInt = "int";
    private String _mapInt = "int";
    private String _mapBigInt = "bigint";
    private String _mapDate = "date";
    private String _mapTime = "time";
    private String _mapTimestamp = "timestamp";
    private String _mapFloat4 = "float";
    private String _mapFloat8 = "double";
    private String _mapVarchar = "varchar";
    private String _mapLargeVarchar = "varchar";
    private String _mapDecimal = "decimal";
    private String _mapDecimal256 = "decimal";
    private String _mapUInt1 = "int";
    private String _mapUInt2 = "int";
    private String _mapUInt4 = "int";
    private String _mapUInt8 = "int";
    private String _mapBit = "boolean";

    /**
     * Construct a WriteStatement
     * @param tableName - the name of the table
     * @param dataSchema - the schema for the data
     * @param arrowSchema - the arrow schema for the table
     * @param columnQuote - the character for quoting columns
     */
    public WriteStatement(String tableName, StructType dataSchema, Schema arrowSchema, String columnQuote, Map<String, String> typeMapping) {
        this(dataSchema, arrowSchema, typeMapping);
        this._stmt = String.format("insert into %s(%s) %s", tableName, String.join(",", WriteStatement._quote.apply(this._params, columnQuote)), WriteStatement._varValues);
    }

    /**
     * Construct a WriteStatement
     * @param tableName - the name of the table
     * @param mergeByColumns - the name of columns for merging by
     * @param dataSchema - the schema for the data
     * @param arrowSchema - the arrow schema for the table
     * @param columnQuote - the character for quoting columns
     */
    public WriteStatement(String tableName, String[] mergeByColumns, StructType dataSchema, Schema arrowSchema, String columnQuote, Map<String, String> typeMapping) {
        this(dataSchema, arrowSchema, typeMapping);

        java.util.Map<String, Integer> entries = new java.util.LinkedHashMap<>();
        for (String field: WriteStatement._quote.apply(this._params, columnQuote)) {
            entries.put(field, 0);
        }
        for (String field: WriteStatement._quote.apply(mergeByColumns, columnQuote)) {
            entries.put(field, 1);
        }
        String matchOn = String.join(" and ", entries.entrySet().stream().filter(e -> e.getValue() == 1).map(e -> String.format("t.%s = s.%s", e.getKey(), e.getKey())).toArray(String[]::new));
        String setUpdate = String.join(",", entries.entrySet().stream().filter(e -> e.getValue() == 0).map(e -> String.format("%s = s.%s", e.getKey(), e.getKey())).toArray(String[]::new));
        String varInsert = String.join(",", entries.keySet().toArray(new String[0]));
        String valInsert = String.join(",", entries.keySet().stream().map(integer -> String.format("s.%s", integer)).toArray(String[]::new));
        this._stmt =String.format("merge into %s t using (%s) s(%s) on %s when matched then update set %s when not matched then insert (%s) values(%s)", tableName, WriteStatement._varValues, varInsert, matchOn, setUpdate, varInsert, valInsert);
    }

    //initialize properties
    private WriteStatement(StructType dataSchema, Schema arrowSchema, Map<String, String> typeMapping) {
        this._dataSchema = dataSchema;
        this._arrowSchema = arrowSchema.toJson();
        this._params = dataSchema.fieldNames();
        this._converters = new java.util.HashMap<>();

        this._mapTinyInt = typeMapping.getOrDefault("tinyint", "int");
        this._mapSmallInt = typeMapping.getOrDefault("smallint", "int");
        this._mapInt = typeMapping.getOrDefault("int", "int");
        this._mapBigInt = typeMapping.getOrDefault("bigint", "bigint");
        this._mapDate = typeMapping.getOrDefault("date", "date");
        this._mapTime = typeMapping.getOrDefault("time", "time");
        this._mapTimestamp = typeMapping.getOrDefault("timestamp", "timestamp");
        this._mapFloat4 = typeMapping.getOrDefault("float4", "float");
        this._mapFloat8 = typeMapping.getOrDefault("float8", "double");
        this._mapVarchar = typeMapping.getOrDefault("varchar", "varchar");
        this._mapLargeVarchar = typeMapping.getOrDefault("largevarchar", "varchar");
        this._mapDecimal = typeMapping.getOrDefault("decimal", "decimal");
        this._mapDecimal256 = typeMapping.getOrDefault("decimal256", "decimal");
        this._mapUInt1 = typeMapping.getOrDefault("uint1", "int");
        this._mapUInt2 = typeMapping.getOrDefault("uint2", "int");
        this._mapUInt4 = typeMapping.getOrDefault("uint4", "int");
        this._mapUInt8 = typeMapping.getOrDefault("uint8", "int");
        this._mapBit = typeMapping.getOrDefault("bit", "boolean");
    }

    /**
     * Get the data schema
     * @return - the data schema
     */
    public StructType getDataSchema() {
        return this._dataSchema;
    }

    /**
     * Get the arrow-schema
     * @return - arrow schema
     * @throws IOException - thrown when the arrow-schema is invalid
     */
    public Schema getArrowSchema() throws IOException {
        return Schema.fromJSON(this._arrowSchema);
    }

    /**
     * Get the statement
     * @return - the merge into or insert into statement
     */
    public String getStatement() {
        return this._stmt.replace(WriteStatement._varValues, String.format("values(%s)", String.join(",", Arrays.stream(this._params).map(param -> "?").toArray(String[]::new))));
    }

    /**
     * Fill the statment with data
     * @param rows - the rows of data
     * @param arrowFields - the fields of output
     * @return - a statement with data
     */
    public String fillStatement(InternalRow[] rows, Field[] arrowFields) {
        Function<String, Optional<Field>> find = (name) -> Arrays.stream(arrowFields).filter(x -> x.getName().equalsIgnoreCase(name)).findFirst();
        StructField[] dataFields = this._dataSchema.fields();
        Object[] columns = IntStream.range(0, dataFields.length).mapToObj(idx -> {
            Optional<Field> arrowField = find.apply(dataFields[idx].name());
            if (!arrowField.isPresent()) {
                throw new RuntimeException("The arrow field is not available.");
            }
            return this.fillColumns(rows, idx, dataFields[idx].dataType(), arrowField.get());
        }).toArray(Object[]::new);

        String[] values = IntStream.range(0, rows.length).mapToObj(i -> String.format("(%s)", String.join(",", Arrays.stream(columns).map(column -> ((String[])column)[i]).toArray(String[]::new)))).toArray(String[]::new);
        return this._stmt.replace(WriteStatement._varValues, String.format("values%s", String.join(",", values)));
    }

    //convert the values of a specific column
    private String[] fillColumns(InternalRow[] rows, int idxColumn, DataType dataType, Field arrowField) {
        String key = String.format("%s-%s", dataType.getClass().getTypeName().replaceAll("\\$$", ""), Types.getMinorTypeForArrowType(arrowField.getType()).getClass().getTypeName());
        if (!this._converters.containsKey(key)) {
            this.initialize();
        }
        return Arrays.stream(rows).map(row -> this._converters.get(key).apply(row.get(idxColumn, dataType), arrowField, dataType)).toArray(String[]::new);
    }

    //initialize all converters
    private void initialize() {
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.DATEDAY.getClass().getTypeName()), (o, f, t) -> WriteStatement._timestamp_2_dateDay.apply(o, f, t, this._mapDate));
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.DATEDAY.getClass().getTypeName()), (o, f, t) -> WriteStatement._date_2_dateDay.apply(o, f, t, this._mapDate));
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPNANOTZ.getClass().getTypeName()), (o, f, t) -> WriteStatement._timestamp_2_timestampNanoTZ.apply(o, f, t, this._mapTimestamp));
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPNANOTZ.getClass().getTypeName()), (o, f, t) -> WriteStatement._date_2_timestampNanoTZ.apply(o, f, t, this._mapTimestamp));
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPMICROTZ.getClass().getTypeName()), (o, f, t) -> WriteStatement._timestamp_2_timestampMicroTZ.apply(o, f, t, this._mapTimestamp));
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPMICROTZ.getClass().getTypeName()), (o, f, t) -> WriteStatement._date_2_timestampMicroTZ.apply(o, f, t, this._mapTimestamp));
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPSECTZ.getClass().getTypeName()), (o, f, t) -> WriteStatement._timestamp_2_timestampSecTZ.apply(o, f, t, this._mapTimestamp));
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPSECTZ.getClass().getTypeName()), (o, f, t) -> WriteStatement._date_2_timestampSecTZ.apply(o, f, t, this._mapTimestamp));
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPMILLITZ.getClass().getTypeName()), (o, f, t) -> WriteStatement._timestamp_2_timestampMilliTZ.apply(o, f, t, this._mapTimestamp));
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPMILLITZ.getClass().getTypeName()), (o, f, t) -> WriteStatement._date_2_timestampMilliTZ.apply(o, f, t, this._mapTimestamp));
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPNANO.getClass().getTypeName()), (o, f, t) -> WriteStatement._timestamp_2_timestampNano.apply(o, f, t, this._mapTimestamp));
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPNANO.getClass().getTypeName()), (o, f, t) -> WriteStatement._date_2_timestampNano.apply(o, f, t, this._mapTimestamp));
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPMICRO.getClass().getTypeName()), (o, f, t) -> WriteStatement._timestamp_2_timestampMicro.apply(o, f, t, this._mapTimestamp));
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPMICRO.getClass().getTypeName()), (o, f, t) -> WriteStatement._date_2_timestampMicro.apply(o, f, t, this._mapTimestamp));
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPSEC.getClass().getTypeName()), (o, f, t) -> WriteStatement._timestamp_2_timestampSec.apply(o, f, t, this._mapTimestamp));
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPSEC.getClass().getTypeName()), (o, f, t) -> WriteStatement._date_2_timestampSec.apply(o, f, t, this._mapTimestamp));
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPMILLI.getClass().getTypeName()), (o, f, t) -> WriteStatement._timestamp_2_timestampMilli.apply(o, f, t, this._mapTimestamp));
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPMILLI.getClass().getTypeName()), (o, f, t) -> WriteStatement._date_2_timestampMilli.apply(o, f, t, this._mapTimestamp));
        this._converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.TIMEMICRO.getClass().getTypeName()), (o, f, t) -> WriteStatement._string_2_timeMicro.apply(o, f, t, this._mapTime));
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMEMICRO.getClass().getTypeName()), (o, f, t) -> WriteStatement._timestamp_2_timeMicro.apply(o, f, t, this._mapTime));
        this._converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.TIMESEC.getClass().getTypeName()), (o, f, t) -> WriteStatement._string_2_timeSec.apply(o, f, t, this._mapTime));
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESEC.getClass().getTypeName()), (o, f, t) -> WriteStatement._timestamp_2_timeSec.apply(o, f, t, this._mapTime));
        this._converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.TIMENANO.getClass().getTypeName()), (o, f, t) -> WriteStatement._string_2_timeNano.apply(o, f, t, this._mapTime));
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMENANO.getClass().getTypeName()), (o, f, t) -> WriteStatement._timestamp_2_timeNano.apply(o, f, t, this._mapTime));
        this._converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.TIMEMILLI.getClass().getTypeName()), (o, f, t) -> WriteStatement._string_2_timeMilli.apply(o, f, t, this._mapTime));
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMEMILLI.getClass().getTypeName()), (o, f, t) -> WriteStatement._timestamp_2_timeMilli.apply(o, f, t, this._mapTime));
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.DATEMILLI.getClass().getTypeName()), (o, f, t) -> WriteStatement._date_2_dateMilli.apply(o, f, t, this._mapDate));
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.DATEMILLI.getClass().getTypeName()), (o, f, t) -> WriteStatement._timestamp_2_dateMilli.apply(o, f, t, this._mapDate));
        this._converters.put(String.format("%s-%s", DecimalType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_decimal256.apply(o, f, t, this._mapDecimal256));
        this._converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_decimal256.apply(o, f, t, this._mapDecimal256));
        this._converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_decimal256.apply(o, f, t, this._mapDecimal256));
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_decimal256.apply(o, f, t, this._mapDecimal256));
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_decimal256.apply(o, f, t, this._mapDecimal256));
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_decimal256.apply(o, f, t, this._mapDecimal256));
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_decimal256.apply(o, f, t, this._mapDecimal256));
        this._converters.put(String.format("%s-%s", DecimalType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_decimal.apply(o, f, t, this._mapDecimal));
        this._converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_decimal.apply(o, f, t, this._mapDecimal));
        this._converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_decimal.apply(o, f, t, this._mapDecimal));
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_decimal.apply(o, f, t, this._mapDecimal));
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_decimal.apply(o, f, t, this._mapDecimal));
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_decimal.apply(o, f, t, this._mapDecimal));
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_decimal.apply(o, f, t, this._mapDecimal));
        this._converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.FLOAT8.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_float8.apply(o, f, t, this._mapFloat8));
        this._converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.FLOAT8.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_float8.apply(o, f, t, this._mapFloat8));
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.FLOAT8.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_float8.apply(o, f, t, this._mapFloat8));
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.FLOAT8.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_float8.apply(o, f, t, this._mapFloat8));
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.FLOAT8.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_float8.apply(o, f, t, this._mapFloat8));
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName() , Types.MinorType.FLOAT8.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_float8.apply(o, f, t, this._mapFloat8));
        this._converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_float4.apply(o, f, t, this._mapFloat4));
        this._converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_float4.apply(o, f, t, this._mapFloat4));
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_float4.apply(o, f, t, this._mapFloat4));
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_float4.apply(o, f, t, this._mapFloat4));
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_float4.apply(o, f, t, this._mapFloat4));
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_float4.apply(o, f, t, this._mapFloat4));
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.BIGINT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_bigint.apply(o, f, t, this._mapBigInt));
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.BIGINT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_bigint.apply(o, f, t, this._mapBigInt));
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.BIGINT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_bigint.apply(o, f, t, this._mapBigInt));
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName() , Types.MinorType.BIGINT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_bigint.apply(o, f, t, this._mapBigInt));
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName() , Types.MinorType.UINT8.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_uint8.apply(o, f, t, this._mapUInt8));
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.UINT8.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_uint8.apply(o, f, t, this._mapUInt8));
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.UINT8.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_uint8.apply(o, f, t, this._mapUInt8));
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.UINT8.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_uint8.apply(o, f, t, this._mapUInt8));
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.UINT4.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_uint4.apply(o, f, t, this._mapUInt4));
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.UINT4.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_uint4.apply(o, f, t, this._mapUInt4));
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.UINT4.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_uint4.apply(o, f, t, this._mapUInt4));
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.INT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_int.apply(o, f, t, this._mapInt));
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.INT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_int.apply(o, f, t, this._mapInt));
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.INT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_int.apply(o, f, t, this._mapInt));
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.UINT2.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_uint2.apply(o, f, t, this._mapUInt2));
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.UINT2.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_uint2.apply(o, f, t, this._mapUInt2));
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.UINT1.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_uint1.apply(o, f, t, this._mapUInt1));
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.SMALLINT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_smallint.apply(o, f, t, this._mapSmallInt));
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.SMALLINT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_smallint.apply(o, f, t, this._mapSmallInt));
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.TINYINT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_tinyint.apply(o, f, t, this._mapTinyInt));
        this._converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_varchar.apply(o, f, t, this._mapVarchar));
        this._converters.put(String.format("%s-%s", BooleanType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_varchar.apply(o, f, t, this._mapVarchar));
        this._converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_varchar.apply(o, f, t, this._mapVarchar));
        this._converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_varchar.apply(o, f, t, this._mapVarchar));
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_varchar.apply(o, f, t, this._mapVarchar));
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_varchar.apply(o, f, t, this._mapVarchar));
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_varchar.apply(o, f, t, this._mapVarchar));
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_varchar.apply(o, f, t, this._mapVarchar));
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_varchar.apply(o, f, t, this._mapVarchar));
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_varchar.apply(o, f, t, this._mapVarchar));
        this._converters.put(String.format("%s-%s", DecimalType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_varchar.apply(o, f, t, this._mapVarchar));
        this._converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_largevarchar.apply(o, f, t, this._mapLargeVarchar));
        this._converters.put(String.format("%s-%s", BooleanType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_largevarchar.apply(o, f, t, this._mapLargeVarchar));
        this._converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_largevarchar.apply(o, f, t, this._mapLargeVarchar));
        this._converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_largevarchar.apply(o, f, t, this._mapLargeVarchar));
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_largevarchar.apply(o, f, t, this._mapLargeVarchar));
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_largevarchar.apply(o, f, t, this._mapLargeVarchar));
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_largevarchar.apply(o, f, t, this._mapLargeVarchar));
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_largevarchar.apply(o, f, t, this._mapLargeVarchar));
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_largevarchar.apply(o, f, t, this._mapLargeVarchar));
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_largevarchar.apply(o, f, t, this._mapLargeVarchar));
        this._converters.put(String.format("%s-%s", DecimalType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._primitive_2_largevarchar.apply(o, f, t, this._mapLargeVarchar));
        this._converters.put(String.format("%s-%s", MapType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._complex_2_varchar.apply(o, f, t, this._mapVarchar));
        this._converters.put(String.format("%s-%s", ArrayType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._complex_2_varchar.apply(o, f, t, this._mapVarchar));
        this._converters.put(String.format("%s-%s", StructType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._complex_2_varchar.apply(o, f, t, this._mapVarchar));
        this._converters.put(String.format("%s-%s", MapType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._complex_2_largevarchar.apply(o, f, t, this._mapLargeVarchar));
        this._converters.put(String.format("%s-%s", ArrayType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._complex_2_largevarchar.apply(o, f, t, this._mapLargeVarchar));
        this._converters.put(String.format("%s-%s", StructType.class.getTypeName(), Types.MinorType.LARGEVARCHAR.getClass().getTypeName()), (o, f, t) -> WriteStatement._complex_2_largevarchar.apply(o, f, t, this._mapLargeVarchar));
        this._converters.put(String.format("%s-%s", BooleanType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement._boolean_2_bit.apply(o, f, t, this._mapBit));
        this._converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement._string_2_bit.apply(o, f, t, this._mapBit));
        this._converters.put(String.format("%s-%s", DecimalType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_bit.apply(o, f, t, this._mapBit));
        this._converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_bit.apply(o, f, t, this._mapBit));
        this._converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_bit.apply(o, f, t, this._mapBit));
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_bit.apply(o, f, t, this._mapBit));
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_bit.apply(o, f, t, this._mapBit));
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_bit.apply(o, f, t, this._mapBit));
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), (o, f, t) -> WriteStatement._number_2_bit.apply(o, f, t, this._mapBit));
    }

    //conversion - Number to DECIMAL256
    private static final convert<Object, Field, DataType, String, String> _number_2_decimal256 = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to DECIMAL
    private static final convert<Object, Field, DataType, String, String> _number_2_decimal = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to FLOAT8
    private static final convert<Object, Field, DataType, String, String> _number_2_float8 = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to FLOAT4
    private static final convert<Object, Field, DataType, String, String> _number_2_float4 = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to BIGINT
    private static final convert<Object, Field, DataType, String, String> _number_2_bigint = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to UINT8
    private static final convert<Object, Field, DataType, String, String> _number_2_uint8 = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to UINT4
    private static final convert<Object, Field, DataType, String, String> _number_2_uint4 = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to INT
    private static final convert<Object, Field, DataType, String, String> _number_2_int = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to UINT2
    private static final convert<Object, Field, DataType, String, String> _number_2_uint2 = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to UINT1
    private static final convert<Object, Field, DataType, String, String> _number_2_uint1 = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to SMALLINT
    private static final convert<Object, Field, DataType, String, String> _number_2_smallint = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to TINYINT
    private static final convert<Object, Field, DataType, String, String> _number_2_tinyint = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Primitive to VARCHAR
    private static final convert<Object, Field, DataType, String, String> _primitive_2_varchar = (o, f, t, n) -> (o == null) ? String.format("cast(null as %s)", n) : String.format("'%s'", o.toString().replace("'", "''"));
    //conversion - Primitive to LARGEVARCHAR
    private static final convert<Object, Field, DataType, String, String> _primitive_2_largevarchar = (o, f, t, n) -> (o == null) ? String.format("cast(null as %s)", n) : String.format("'%s'", o.toString().replace("'", "''"));
    //conversion - Complex to VARCHAR
    private static final convert<Object, Field, DataType, String, String> _complex_2_varchar = (o, f, t, n) -> (o == null) ? String.format("cast(null as %s)", n) : String.format("'%s'", WriteStatement._to_json.apply(o, t).replace("'", "''"));
    //conversion - Complex to LARGEVARCHAR
    private static final convert<Object, Field, DataType, String, String> _complex_2_largevarchar = (o, f, t, n) -> (o == null) ? String.format("cast(null as %s)", n) : String.format("'%s'", WriteStatement._to_json.apply(o, t).replace("'", "''"));
    //conversion - String to TIMESEC
    private static final convert<Object, Field, DataType, String, String> _string_2_timeSec = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : LocalTime.parse((o instanceof String) ? (String)o : o.toString()).format(DateTimeFormatter.ofPattern("HH:mm:ss")), n);
    //conversion - Timestamp to TIMESEC
    private static final convert<Object, Field, DataType, String, String> _timestamp_2_timeSec = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("HH:mm:ss")), n);
    //conversion - String to TIMENANO
    private static final convert<Object, Field, DataType, String, String> _string_2_timeNano = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : LocalTime.parse((o instanceof String) ? (String)o : o.toString()).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS")), n);
    //conversion - Timestamp to TIMENANO
    private static final convert<Object, Field, DataType, String, String> _timestamp_2_timeNano = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS")), n);
    //conversion - String to TIMEMICRO
    private static final convert<Object, Field, DataType, String, String> _string_2_timeMicro = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : LocalTime.parse((o instanceof String) ? (String)o : o.toString()).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")), n);
    //conversion - Timestamp to TIMEMICRO
    private static final convert<Object, Field, DataType, String, String> _timestamp_2_timeMicro = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")), n);
    //conversion - String to TIMEMILLI
    private static final convert<Object, Field, DataType, String, String> _string_2_timeMilli = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : LocalTime.parse((o instanceof String) ? (String)o : o.toString()).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")), n);
    //conversion - Timestamp to TIMEMILLI
    private static final convert<Object, Field, DataType, String, String> _timestamp_2_timeMilli = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")), n);
    //conversion - Date to DATEMILLI
    private static final convert<Object, Field, DataType, String, String> _date_2_dateMilli = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), n);
    //conversion - Timestamp to DATEMILLI
    private static final convert<Object, Field, DataType, String, String> _timestamp_2_dateMilli = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(DateTimeUtils.daysToMicros((o instanceof Number) ? (int)o : Integer.parseInt(o.toString()), ZoneId.systemDefault())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), n);
    //conversion - Timestamp to DateDay
    private static final convert<Object, Field, DataType, String, String> _timestamp_2_dateDay = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), n);
    //conversion - Date to DateDay
    private static final convert<Object, Field, DataType, String, String> _date_2_dateDay = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), n);
    //conversion - Boolean to BIT
    private static final convert<Object, Field, DataType, String, String> _boolean_2_bit = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString(), n);
    //conversion - Number to BIT
    private static final convert<Object, Field, DataType, String, String> _number_2_bit = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : (((int)o) != 0) ? "true" : "false", n);
    //conversion - String to BIT
    private static final convert<Object, Field, DataType, String, String> _string_2_bit = (o, f, t, n) -> String.format("cast(%s as %s)", (o == null) ? "null" : o.toString().equalsIgnoreCase("true") ? "true" : "false", n);
    //conversion - Timestamp to TIMESTAMPMILLI
    private static final convert<Object, Field, DataType, String, String> _timestamp_2_timestampMilli = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")), n);
    //conversion - Date to TIMESTAMPMILLI
    private static final convert<Object, Field, DataType, String, String> _date_2_timestampMilli = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")), n);
    //conversion - Timestamp to TIMESTAMPSEC
    private static final convert<Object, Field, DataType, String, String> _timestamp_2_timestampSec = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")), n);
    //conversion - Date to TIMESTAMPSEC
    private static final convert<Object, Field, DataType, String, String> _date_2_timestampSec = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")), n);
    //conversion - Timestamp to TIMESTAMPNANO
    private static final convert<Object, Field, DataType, String, String> _timestamp_2_timestampNano = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")), n);
    //conversion - Date to TIMESTAMPNANO
    private static final convert<Object, Field, DataType, String, String> _date_2_timestampNano = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")), n);
    //conversion - Timestamp to TIMESTAMPMICRO
    private static final convert<Object, Field, DataType, String, String> _timestamp_2_timestampMicro = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")), n);
    //conversion - Date to TIMESTAMPMICRO
    private static final convert<Object, Field, DataType, String, String> _date_2_timestampMicro = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")), n);
    //conversion - Timestamp to TIMESTAMPMILLITZ
    private static final convert<Object, Field, DataType, String, String> _timestamp_2_timestampMilliTZ = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement._micros_2_microsTZ.apply((o instanceof Number) ? (long)o : Long.parseLong(o.toString()), f, LongType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")), n);
    //conversion - Date to TIMESTAMPMILLITZ
    private static final convert<Object, Field, DataType, String, String> _date_2_timestampMilliTZ = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement._days_2_microsTZ.apply((o instanceof Number) ? (int)o : Integer.parseInt(o.toString()), f, IntegerType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")), n);
    //conversion - Timestamp to TIMESTAMPSEC
    private static final convert<Object, Field, DataType, String, String> _timestamp_2_timestampSecTZ = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement._micros_2_microsTZ.apply((o instanceof Number) ? (long)o : Long.parseLong(o.toString()), f, LongType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")), n);
    //conversion - Date to TIMESTAMPSEC
    private static final convert<Object, Field, DataType, String, String> _date_2_timestampSecTZ = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement._days_2_microsTZ.apply((o instanceof Number) ? (int)o : Integer.parseInt(o.toString()), f, IntegerType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")), n);
    //conversion - Timestamp to TIMESTAMPNANO
    private static final convert<Object, Field, DataType, String, String> _timestamp_2_timestampNanoTZ = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement._micros_2_microsTZ.apply((o instanceof Number) ? (long)o : Long.parseLong(o.toString()), f, LongType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")), n);
    //conversion - Date to TIMESTAMPNANO
    private static final convert<Object, Field, DataType, String, String> _date_2_timestampNanoTZ = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement._days_2_microsTZ.apply((o instanceof Number) ? (int)o : Integer.parseInt(o.toString()), f, IntegerType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")), n);
    //conversion - Timestamp to TIMESTAMPMICRO
    private static final convert<Object, Field, DataType, String, String> _timestamp_2_timestampMicroTZ = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement._micros_2_microsTZ.apply((o instanceof Number) ? (long)o : Long.parseLong(o.toString()), f, LongType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")), n);
    //conversion - Date to TIMESTAMPMICRO
    private static final convert<Object, Field, DataType, String, String> _date_2_timestampMicroTZ = (o, f, t, n) -> String.format("cast('%s' as %s)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement._days_2_microsTZ.apply((o instanceof Number) ? (int)o : Integer.parseInt(o.toString()), f, IntegerType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")), n);
    //micros to micros-TZ
    private static final execute<Long, Field, DataType, Long> _micros_2_microsTZ = (micros, field, dataType) -> {
        ArrowType.Timestamp arrowType = (ArrowType.Timestamp)field.getFieldType().getType();
        return DateTimeUtils.convertTz(micros, ZoneId.systemDefault(), ZoneId.of(arrowType.getTimezone()));
    };
    //days to micros-TZ
    private static final execute<Integer, Field, DataType, Long> _days_2_microsTZ = (days, field, dataType) -> {
        ArrowType.Timestamp arrowType = (ArrowType.Timestamp)field.getFieldType().getType();
        return DateTimeUtils.daysToMicros(days, ZoneId.of(arrowType.getTimezone()));
    };
    //to-json
    private static final BiFunction<Object, DataType, String> _to_json = (o, dt) -> {
        StringBuilder sb = new StringBuilder();
        try {
            if (dt instanceof org.apache.spark.sql.types.StructType && o instanceof UnsafeRow) {
                StructField[] fields = ((StructType)dt).fields();
                UnsafeRow data = (UnsafeRow)o;
                sb.append(String.format("{ %s }", String.join(", ", IntStream.range(0, fields.length).mapToObj(idx -> String.format("\"%s\": %s", fields[idx].name(), WriteStatement._to_json.apply(data.get(idx, fields[idx].dataType()), fields[idx].dataType()))).toArray(String[]::new))));
            } else if (dt instanceof org.apache.spark.sql.types.MapType && o instanceof UnsafeMapData) {
                MapType mt = (MapType)dt;
                UnsafeMapData data = (UnsafeMapData)o;
                UnsafeArrayData keys = data.keyArray();
                UnsafeArrayData values = data.valueArray();
                sb.append(String.format("{ \"map\": [%s] }", String.join(", ", IntStream.range(0, data.numElements()).mapToObj(idx -> String.format("{ \"key\": %s, \"value\": %s }", WriteStatement._to_json.apply(keys.get(idx, mt.keyType()), mt.keyType()), WriteStatement._to_json.apply(values.get(idx, mt.valueType()), mt.valueType()))).toArray(String[]::new))));
            } else if (dt instanceof org.apache.spark.sql.types.ArrayType && o instanceof UnsafeArrayData) {
                ArrayType at = (ArrayType)dt;
                UnsafeArrayData data = (UnsafeArrayData)o;
                sb.append(String.format("[%s]", String.join(", ", IntStream.range(0, data.numElements()).mapToObj(idx -> WriteStatement._to_json.apply(data.get(idx, at.elementType()), at.elementType())).toArray(String[]::new))));
            } else {
                sb.append(String.format("\"%s\"", o));
            }
        } catch (Exception e){
            LoggerFactory.getLogger(WriteStatement.class).warn(e.getMessage() + Arrays.toString(e.getStackTrace()));
        }
        return sb.toString().replace("'", "''");
    };
    //quote all fields in the collection
    private static final BiFunction<String[], String, String[]> _quote = (fields, quote) -> Arrays.stream(fields).map(field -> String.format("%s%s%s", quote, field, quote)).toArray(String[]::new);
}
