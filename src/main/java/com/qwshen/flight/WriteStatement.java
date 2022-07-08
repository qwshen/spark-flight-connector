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
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.time.format.DateTimeFormatter;

/**
 * The write statement for writing data to remote flight service
 */
public class WriteStatement implements Serializable {
    //the toConversion interface
    @FunctionalInterface
    private interface conversion<X, Y, Z, R> {
        R apply(X x, Y y, Z z);
    }
    //the values variable
    private static final String _varValues = "${param_Values}";
    //the to-object-converter container
    private final java.util.Map<String, conversion<Object, Field, DataType, String>> _converters;

    //data schema
    private final StructType _dataSchema;
    //arrow schema
    private final String _arrowSchema;
    //the parameter name in the WriteStatement
    private final String[] _params;

    //the statement in the format of either merge into or insert into sql statement
    private String _stmt;

    /**
     * Construct a WriteStatement
     * @param tableName - the name of the table
     * @param dataSchema - the schema for the data
     * @param arrowSchema - the arrow schema for the table
     * @param columnQuote - the character for quoting columns
     */
    public WriteStatement(String tableName, StructType dataSchema, Schema arrowSchema, String columnQuote) {
        this(dataSchema, arrowSchema);
        this._stmt = String.format("insert into %s(%s) %s", tableName, String.join(",", this._quote.apply(this._params, columnQuote)), WriteStatement._varValues);
    }

    /**
     * Construct a WriteStatement
     * @param tableName - the name of the table
     * @param mergeByColumns - the name of columns for merging by
     * @param dataSchema - the schema for the data
     * @param arrowSchema - the arrow schema for the table
     * @param columnQuote - the character for quoting columns
     */
    public WriteStatement(String tableName, String[] mergeByColumns, StructType dataSchema, Schema arrowSchema, String columnQuote) {
        this(dataSchema, arrowSchema);

        java.util.Map<String, Integer> entries = new java.util.LinkedHashMap<>();
        for (String field: this._quote.apply(this._params, columnQuote)) {
            entries.put(field, 0);
        }
        for (String field: this._quote.apply(mergeByColumns, columnQuote)) {
            entries.put(field, 1);
        }
        String matchOn = String.join(" and ", entries.entrySet().stream().filter(e -> e.getValue() == 1).map(e -> String.format("t.%s = s.%s", e.getKey(), e.getKey())).toArray(String[]::new));
        String setUpdate = String.join(",", entries.entrySet().stream().filter(e -> e.getValue() == 0).map(e -> String.format("%s = s.%s", e.getKey(), e.getKey())).toArray(String[]::new));
        String varInsert = String.join(",", entries.keySet().toArray(new String[0]));
        String valInsert = String.join(",", entries.keySet().stream().map(integer -> String.format("s.%s", integer)).toArray(String[]::new));
        this._stmt =String.format("merge into %s t using (%s) s(%s) on %s when matched then update set %s when not matched then insert (%s) values(%s)", tableName, WriteStatement._varValues, varInsert, matchOn, setUpdate, varInsert, valInsert);
    }

    //initialize properties
    private WriteStatement(StructType dataSchema, Schema arrowSchema) {
        this._dataSchema = dataSchema;
        this._arrowSchema = arrowSchema.toJson();
        this._params = dataSchema.fieldNames();
        this._converters = new java.util.HashMap<>();
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

    //conversion - Number to BIGINT
    private final conversion<Object, Field, DataType, String> _number_2_num = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast(%s as bigint)", (o == null) ? "null" : o.toString());
    //conversion - Primitive to VARCHAR
    private final conversion<Object, Field, DataType, String> _primitive_2_varchar = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> (o == null) ? "cast(null as varchar)" : String.format("'%s'", o.toString().replace("'", "''"));
    //conversion - Complex to VARCHAR
    private final conversion<Object, Field, DataType, String> _complex_2_varchar = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> (o == null) ? "cast(null as varchar)" : String.format("'%s'", this._to_json.apply(o, dt).replace("'", "''"));
    //conversion - String to TIMESEC
    private final conversion<Object, Field, DataType, String> _string_2_timeSec = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as time)", (o == null) ? "null" : LocalTime.parse((o instanceof String) ? (String)o : o.toString()).format(DateTimeFormatter.ofPattern("HH:mm:ss")));
    //conversion - Timestamp to TIMESEC
    private final conversion<Object, Field, DataType, String> _timestamp_2_timeSec = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as time)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("HH:mm:ss")));
    //conversion - String to TIMENANO
    private final conversion<Object, Field, DataType, String> _string_2_timeNano = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as time)", (o == null) ? "null" : LocalTime.parse((o instanceof String) ? (String)o : o.toString()).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS")));
    //conversion - Timestamp to TIMENANO
    private final conversion<Object, Field, DataType, String> _timestamp_2_timeNano = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as time)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS")));
    //conversion - String to TIMEMICRO
    private final conversion<Object, Field, DataType, String> _string_2_timeMicro = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as time)", (o == null) ? "null" : LocalTime.parse((o instanceof String) ? (String)o : o.toString()).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")));
    //conversion - Timestamp to TIMEMICRO
    private final conversion<Object, Field, DataType, String> _timestamp_2_timeMicro = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as time)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")));
    //conversion - String to TIMEMILLI
    private final conversion<Object, Field, DataType, String> _string_2_timeMilli = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as time)", (o == null) ? "null" : LocalTime.parse((o instanceof String) ? (String)o : o.toString()).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")));
    //conversion - Timestamp to TIMEMILLI
    private final conversion<Object, Field, DataType, String> _timestamp_2_timeMilli = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as time)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")));
    //conversion - Date to DATEMILLI
    private final conversion<Object, Field, DataType, String> _date_2_dateMilli = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as date)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
    //conversion - Timestamp to DATEMILLI
    private final conversion<Object, Field, DataType, String> _timestamp_2_dateMilli = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as date)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(DateTimeUtils.daysToMicros((o instanceof Number) ? (int)o : Integer.parseInt(o.toString()), ZoneId.systemDefault())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
    //conversion - Timestamp to DateDay
    private final conversion<Object, Field, DataType, String> _timestamp_2_dateDay = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as date)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
    //conversion - Date to DateDay
    private final conversion<Object, Field, DataType, String> _date_2_dateDay = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as date)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
    //conversion - Boolean to BIT
    private final conversion<Object, Field, DataType, String> _boolean_2_bit = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast(%s as boolean)", (o == null) ? "null" : o.toString());
    //conversion - Number to BIT
    private final conversion<Object, Field, DataType, String> _number_2_bit = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast(%s as boolean)", (o == null) ? "null" : (((int)o) != 0) ? "true" : "false");
    //conversion - String to BIT
    private final conversion<Object, Field, DataType, String> _string_2_bit = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast(%s as boolean)", (o == null) ? "null" : o.toString().equalsIgnoreCase("true") ? "true" : "false");
    //conversion - Timestamp to TIMESTAMPMILLI
    private final conversion<Object, Field, DataType, String> _timestamp_2_timestampMilli = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
    //conversion - Date to TIMESTAMPMILLI
    private final conversion<Object, Field, DataType, String> _date_2_timestampMilli = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
    //conversion - Timestamp to TIMESTAMPSEC
    private final conversion<Object, Field, DataType, String> _timestamp_2_timestampSec = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    //conversion - Date to TIMESTAMPSEC
    private final conversion<Object, Field, DataType, String> _date_2_timestampSec = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    //conversion - Timestamp to TIMESTAMPNANO
    private final conversion<Object, Field, DataType, String> _timestamp_2_timestampNano = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")));
    //conversion - Date to TIMESTAMPNANO
    private final conversion<Object, Field, DataType, String> _date_2_timestampNano = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")));
    //conversion - Timestamp to TIMESTAMPMICRO
    private final conversion<Object, Field, DataType, String> _timestamp_2_timestampMicro = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")));
    //conversion - Date to TIMESTAMPMICRO
    private final conversion<Object, Field, DataType, String> _date_2_timestampMicro = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")));
    //conversion - Timestamp to TIMESTAMPMILLITZ
    private final conversion<Object, Field, DataType, String> _timestamp_2_timestampMilliTZ = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(this._micros_2_microsTZ.apply((o instanceof Number) ? (long)o : Long.parseLong(o.toString()), f, LongType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
    //conversion - Date to TIMESTAMPMILLITZ
    private final conversion<Object, Field, DataType, String> _date_2_timestampMilliTZ = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(this._days_2_microsTZ.apply((o instanceof Number) ? (int)o : Integer.parseInt(o.toString()), f, IntegerType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
    //conversion - Timestamp to TIMESTAMPSEC
    private final conversion<Object, Field, DataType, String> _timestamp_2_timestampSecTZ = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(this._micros_2_microsTZ.apply((o instanceof Number) ? (long)o : Long.parseLong(o.toString()), f, LongType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    //conversion - Date to TIMESTAMPSEC
    private final conversion<Object, Field, DataType, String> _date_2_timestampSecTZ = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(this._days_2_microsTZ.apply((o instanceof Number) ? (int)o : Integer.parseInt(o.toString()), f, IntegerType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    //conversion - Timestamp to TIMESTAMPNANO
    private final conversion<Object, Field, DataType, String> _timestamp_2_timestampNanoTZ = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(this._micros_2_microsTZ.apply((o instanceof Number) ? (long)o : Long.parseLong(o.toString()), f, LongType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")));
    //conversion - Date to TIMESTAMPNANO
    private final conversion<Object, Field, DataType, String> _date_2_timestampNanoTZ = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(this._days_2_microsTZ.apply((o instanceof Number) ? (int)o : Integer.parseInt(o.toString()), f, IntegerType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")));
    //conversion - Timestamp to TIMESTAMPMICRO
    private final conversion<Object, Field, DataType, String> _timestamp_2_timestampMicroTZ = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(this._micros_2_microsTZ.apply((o instanceof Number) ? (long)o : Long.parseLong(o.toString()), f, LongType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")));
    //conversion - Date to TIMESTAMPMICRO
    private final conversion<Object, Field, DataType, String> _date_2_timestampMicroTZ = (conversion<Object, Field, DataType, String> & Serializable)(o, f, dt) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(this._days_2_microsTZ.apply((o instanceof Number) ? (int)o : Integer.parseInt(o.toString()), f, IntegerType$.MODULE$)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")));

    //initialize all converters
    private void initialize() {
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.DATEDAY.getClass().getTypeName()), this._timestamp_2_dateDay);
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.DATEDAY.getClass().getTypeName()), this._date_2_dateDay);
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPNANOTZ.getClass().getTypeName()), this._timestamp_2_timestampNanoTZ);
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPNANOTZ.getClass().getTypeName()), this._date_2_timestampNanoTZ);
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPMICROTZ.getClass().getTypeName()), this._timestamp_2_timestampMicroTZ);
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPMICROTZ.getClass().getTypeName()), this._date_2_timestampMicroTZ);
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPSECTZ.getClass().getTypeName()), this._timestamp_2_timestampSecTZ);
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPSECTZ.getClass().getTypeName()), this._date_2_timestampSecTZ);
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPMILLITZ.getClass().getTypeName()), this._timestamp_2_timestampMilliTZ);
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPMILLITZ.getClass().getTypeName()), this._date_2_timestampMilliTZ);
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPNANO.getClass().getTypeName()), this._timestamp_2_timestampNano);
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPNANO.getClass().getTypeName()), this._date_2_timestampNano);
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPMICRO.getClass().getTypeName()), this._timestamp_2_timestampMicro);
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPMICRO.getClass().getTypeName()), this._date_2_timestampMicro);
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPSEC.getClass().getTypeName()), this._timestamp_2_timestampSec);
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPSEC.getClass().getTypeName()), this._date_2_timestampSec);
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESTAMPMILLI.getClass().getTypeName()), this._timestamp_2_timestampMilli);
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.TIMESTAMPMILLI.getClass().getTypeName()), this._date_2_timestampMilli);
        this._converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.TIMEMICRO.getClass().getTypeName()), this._string_2_timeMicro);
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMEMICRO.getClass().getTypeName()), this._timestamp_2_timeMicro);
        this._converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.TIMESEC.getClass().getTypeName()), this._string_2_timeSec);
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMESEC.getClass().getTypeName()), this._timestamp_2_timeSec);
        this._converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.TIMENANO.getClass().getTypeName()), this._string_2_timeNano);
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMENANO.getClass().getTypeName()), this._timestamp_2_timeNano);
        this._converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.TIMEMILLI.getClass().getTypeName()), this._string_2_timeMilli);
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.TIMEMILLI.getClass().getTypeName()), this._timestamp_2_timeMilli);
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.DATEMILLI.getClass().getTypeName()), this._date_2_dateMilli);
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.DATEMILLI.getClass().getTypeName()), this._timestamp_2_dateMilli);
        this._converters.put(String.format("%s-%s", DecimalType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.DECIMAL256.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", DecimalType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.DECIMAL.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.FLOAT8.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.FLOAT8.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.FLOAT8.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.FLOAT8.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.FLOAT8.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName() , Types.MinorType.FLOAT8.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.FLOAT4.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", BooleanType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), this._boolean_2_bit);
        this._converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), this._string_2_bit);
        this._converters.put(String.format("%s-%s", DecimalType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), this._number_2_bit);
        this._converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), this._number_2_bit);
        this._converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), this._number_2_bit);
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), this._number_2_bit);
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), this._number_2_bit);
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), this._number_2_bit);
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.BIT.getClass().getTypeName()), this._number_2_bit);
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.BIGINT.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.BIGINT.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.BIGINT.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName() , Types.MinorType.BIGINT.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName() , Types.MinorType.UINT8.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.UINT8.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.UINT8.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.UINT8.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.UINT4.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.UINT4.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.UINT4.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.INT.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.INT.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.INT.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.UINT2.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.UINT2.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.UINT1.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.SMALLINT.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.SMALLINT.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.TINYINT.getClass().getTypeName()), this._number_2_num);
        this._converters.put(String.format("%s-%s", StringType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), this._primitive_2_varchar);
        this._converters.put(String.format("%s-%s", BooleanType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), this._primitive_2_varchar);
        this._converters.put(String.format("%s-%s", DoubleType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), this._primitive_2_varchar);
        this._converters.put(String.format("%s-%s", FloatType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), this._primitive_2_varchar);
        this._converters.put(String.format("%s-%s", ByteType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), this._primitive_2_varchar);
        this._converters.put(String.format("%s-%s", IntegerType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), this._primitive_2_varchar);
        this._converters.put(String.format("%s-%s", LongType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), this._primitive_2_varchar);
        this._converters.put(String.format("%s-%s", ShortType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), this._primitive_2_varchar);
        this._converters.put(String.format("%s-%s", DateType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), this._primitive_2_varchar);
        this._converters.put(String.format("%s-%s", TimestampType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), this._primitive_2_varchar);
        this._converters.put(String.format("%s-%s", DecimalType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), this._primitive_2_varchar);
        this._converters.put(String.format("%s-%s", MapType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), this._complex_2_varchar);
        this._converters.put(String.format("%s-%s", ArrayType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), this._complex_2_varchar);
        this._converters.put(String.format("%s-%s", StructType.class.getTypeName(), Types.MinorType.VARCHAR.getClass().getTypeName()), this._complex_2_varchar);
    }

    //micros to micros-TZ
    private final conversion<Long, Field, DataType, Long> _micros_2_microsTZ = (conversion<Long, Field, DataType, Long> & Serializable)(micros, field, dataType) -> {
        ArrowType.Timestamp arrowType = (ArrowType.Timestamp)field.getFieldType().getType();
        return DateTimeUtils.convertTz(micros, ZoneId.systemDefault(), ZoneId.of(arrowType.getTimezone()));
    };
    //days to micros-TZ
    private final conversion<Integer, Field, DataType, Long> _days_2_microsTZ = (conversion<Integer, Field, DataType, Long> & Serializable)(days, field, dataType) -> {
        ArrowType.Timestamp arrowType = (ArrowType.Timestamp)field.getFieldType().getType();
        return DateTimeUtils.daysToMicros(days, ZoneId.of(arrowType.getTimezone()));
    };
    //to-json
    private final BiFunction<Object, DataType, String> _to_json = (BiFunction<Object, DataType, String> & Serializable)(o, dt) -> {
        StringBuilder sb = new StringBuilder();
        try {
            if (dt instanceof org.apache.spark.sql.types.StructType && o instanceof UnsafeRow) {
                StructField[] fields = ((StructType)dt).fields();
                UnsafeRow data = (UnsafeRow)o;
                sb.append(String.format("{ %s }", String.join(", ", IntStream.range(0, fields.length).mapToObj(idx -> String.format("\"%s\": %s", fields[idx].name(), this._to_json.apply(data.get(idx, fields[idx].dataType()), fields[idx].dataType()))).toArray(String[]::new))));
            } else if (dt instanceof org.apache.spark.sql.types.MapType && o instanceof UnsafeMapData) {
                MapType mt = (MapType)dt;
                UnsafeMapData data = (UnsafeMapData)o;
                UnsafeArrayData keys = data.keyArray();
                UnsafeArrayData values = data.valueArray();
                sb.append(String.format("{ \"map\": [%s] }", String.join(", ", IntStream.range(0, data.numElements()).mapToObj(idx -> String.format("{ \"key\": %s, \"value\": %s }", this._to_json.apply(keys.get(idx, mt.keyType()), mt.keyType()), this._to_json.apply(values.get(idx, mt.valueType()), mt.valueType()))).toArray(String[]::new))));
            } else if (dt instanceof org.apache.spark.sql.types.ArrayType && o instanceof UnsafeArrayData) {
                ArrayType at = (ArrayType)dt;
                UnsafeArrayData data = (UnsafeArrayData)o;
                sb.append(String.format("[%s]", String.join(", ", IntStream.range(0, data.numElements()).mapToObj(idx -> this._to_json.apply(data.get(idx, at.elementType()), at.elementType())).toArray(String[]::new))));
            } else {
                sb.append(String.format("\"%s\"", o));
            }
        } catch (Exception e){
            LoggerFactory.getLogger(WriteStatement.class).warn(e.getMessage() + Arrays.toString(e.getStackTrace()));
        }
        return sb.toString().replace("'", "''");
    };
    //quote all fields in the collection
    private final BiFunction<String[], String, String[]> _quote = (BiFunction<String[], String, String[]> & Serializable)(fields, quote) -> Arrays.stream(fields).map(field -> String.format("%s%s%s", quote, field, quote)).toArray(String[]::new);
}
