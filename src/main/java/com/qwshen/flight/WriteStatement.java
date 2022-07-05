package com.qwshen.flight;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.types.*;
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
    private interface toConversion<X, Y, Z> {
        Z apply(X x, Y y);
    }
    //the to-object-converter container
    private final java.util.Map<String, toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String>> _toObjectConverters;
    //the values variable
    private static final String _varValues = "${param_Values}";

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
        this(dataSchema, arrowSchema, columnQuote);
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
    public WriteStatement(String tableName, String[] mergeByColumns, StructType dataSchema, Schema arrowSchema, String columnQuote) {
        this(dataSchema, arrowSchema, columnQuote);

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
    private WriteStatement(StructType dataSchema, Schema arrowSchema, String columnQuote) {
        this._dataSchema = dataSchema;
        this._arrowSchema = arrowSchema.toJson();
        this._params = dataSchema.fieldNames();
        this._toObjectConverters = new java.util.HashMap<>();
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
     * @param dataFields - the fields of the data
     * @param arrowFields - the fields of output
     * @return - a statement with data
     */
    public String fillStatement(InternalRow[] rows, StructField[] dataFields, org.apache.arrow.vector.types.pojo.Field[] arrowFields) {
        Function<String, Optional<org.apache.arrow.vector.types.pojo.Field>> find = (name) -> Arrays.stream(arrowFields).filter(x -> x.getName().equalsIgnoreCase(name)).findFirst();
        Object[] columns = IntStream.range(0, dataFields.length).mapToObj(idx -> {
            Optional<org.apache.arrow.vector.types.pojo.Field> arrowField = find.apply(dataFields[idx].name());
            if (!arrowField.isPresent()) {
                throw new RuntimeException("The arrow field is not available.");
            }
            return this.fillColumns(rows, idx, dataFields[idx].dataType(), arrowField.get());
        }).toArray(Object[]::new);

        String[] values = IntStream.range(0, rows.length).mapToObj(i -> String.format("(%s)", String.join(",", Arrays.stream(columns).map(column -> ((String[])column)[i]).toArray(String[]::new)))).toArray(String[]::new);
        return this._stmt.replace(WriteStatement._varValues, String.format("values%s", String.join(",", values)));
    }

    //convert the values of a specific column
    private String[] fillColumns(InternalRow[] rows, int idxColumn, DataType dataType, org.apache.arrow.vector.types.pojo.Field arrowField) {
        String key = String.format("%s-%s", Types.getMinorTypeForArrowType(arrowField.getType()).getClass().getTypeName(), dataType.getClass().getTypeName().replaceAll("\\$$", ""));
        if (!this._toObjectConverters.containsKey(key)) {
            this.initialize();
        }
        return Arrays.stream(rows).map(row -> this._toObjectConverters.get(key).apply(row.get(idxColumn, dataType), arrowField)).toArray(String[]::new);
    }

    //conversion - Number to BIGINT
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _number_2_num = (o, f) -> String.format("cast(%s as bigint)", (o == null) ? "null" : o.toString());
    //conversion - String to VARCHAR
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _string_2_varchar = (o, f) -> String.format("cast(%s as varchar)", (o == null) ? "null" : String.format("'%s'", o));
    //conversion - String to TIMESEC
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _string_2_timeSec = (o, f) -> String.format("cast('%s' as time)", (o == null) ? "null" : LocalTime.parse((o instanceof String) ? (String)o : o.toString()).format(DateTimeFormatter.ofPattern("HH:mm:ss")));
    //conversion - Timestamp to TIMESEC
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _timestamp_2_timeSec = (o, f) -> String.format("cast('%s' as time)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("HH:mm:ss")));
    //conversion - String to TIMENANO
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _string_2_timeNano = (o, f) -> String.format("cast('%s' as time)", (o == null) ? "null" : LocalTime.parse((o instanceof String) ? (String)o : o.toString()).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS")));
    //conversion - Timestamp to TIMENANO
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _timestamp_2_timeNano = (o, f) -> String.format("cast('%s' as time)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS")));
    //conversion - String to TIMEMICRO
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _string_2_timeMicro = (o, f) -> String.format("cast('%s' as time)", (o == null) ? "null" : LocalTime.parse((o instanceof String) ? (String)o : o.toString()).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")));
    //conversion - Timestamp to TIMEMICRO
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _timestamp_2_timeMicro = (o, f) -> String.format("cast('%s' as time)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")));
    //conversion - String to TIMEMILLI
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _string_2_timeMilli = (o, f) -> String.format("cast('%s' as time)", (o == null) ? "null" : LocalTime.parse((o instanceof String) ? (String)o : o.toString()).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")));
    //conversion - Timestamp to TIMEMILLI
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _timestamp_2_timeMilli = (o, f) -> String.format("cast('%s' as time)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")));
    //conversion - Date to DATEMILLI
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _date_2_dateMilli = (o, f) -> String.format("cast('%s' as date)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
    //conversion - Timestamp to DATEMILLI
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _timestamp_2_dateMilli = (o, f) -> String.format("cast('%s' as date)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(DateTimeUtils.daysToMicros((o instanceof Number) ? (int)o : Integer.parseInt(o.toString()), ZoneId.systemDefault())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
    //conversion - Timestamp to DateDay
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _timestamp_2_dateDay = (o, f) -> String.format("cast('%s' as date)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
    //conversion - Date to DateDay
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _date_2_dateDay = (o, f) -> String.format("cast('%s' as date)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
    //conversion - Boolean to BIT
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _boolean_2_bit = (o, f) -> String.format("cast(%s as boolean)", (o == null) ? "null" : o.toString());
    //conversion - Number to BIT
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _number_2_bit = (o, f) -> String.format("cast(%s as boolean)", (o == null) ? "null" : (((int)o) != 0) ? "true" : "false");
    //conversion - String to BIT
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _string_2_bit = (o, f) -> String.format("cast(%s as boolean)", (o == null) ? "null" : o.toString().equalsIgnoreCase("true") ? "true" : "false");
    //conversion - Timestamp to TIMESTAMPMILLI
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _timestamp_2_timestampMilli = (o, f) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
    //conversion - Date to TIMESTAMPMILLI
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _date_2_timestampMilli = (o, f) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
    //conversion - Timestamp to TIMESTAMPSEC
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _timestamp_2_timestampSec = (o, f) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    //conversion - Date to TIMESTAMPSEC
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _date_2_timestampSec = (o, f) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    //conversion - Timestamp to TIMESTAMPNANO
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _timestamp_2_timestampNano = (o, f) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")));
    //conversion - Date to TIMESTAMPNANO
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _date_2_timestampNano = (o, f) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")));
    //conversion - Timestamp to TIMESTAMPMICRO
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _timestamp_2_timestampMicro = (o, f) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime((o instanceof Number) ? (long)o : Long.parseLong(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")));
    //conversion - Date to TIMESTAMPMICRO
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _date_2_timestampMicro = (o, f) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.daysToLocalDate((o instanceof Number) ? (int)o : Integer.parseInt(o.toString())).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")));
    //conversion - Timestamp to TIMESTAMPMILLITZ
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _timestamp_2_timestampMilliTZ = (o, f) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement._micros_2_microsTZ.apply((o instanceof Number) ? (long)o : Long.parseLong(o.toString()), f)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
    //conversion - Date to TIMESTAMPMILLITZ
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _date_2_timestampMilliTZ = (o, f) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement._days_2_microsTZ.apply((o instanceof Number) ? (int)o : Integer.parseInt(o.toString()), f)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
    //conversion - Timestamp to TIMESTAMPSEC
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _timestamp_2_timestampSecTZ = (o, f) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement._micros_2_microsTZ.apply((o instanceof Number) ? (long)o : Long.parseLong(o.toString()), f)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    //conversion - Date to TIMESTAMPSEC
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _date_2_timestampSecTZ = (o, f) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement._days_2_microsTZ.apply((o instanceof Number) ? (int)o : Integer.parseInt(o.toString()), f)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    //conversion - Timestamp to TIMESTAMPNANO
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _timestamp_2_timestampNanoTZ = (o, f) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement._micros_2_microsTZ.apply((o instanceof Number) ? (long)o : Long.parseLong(o.toString()), f)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")));
    //conversion - Date to TIMESTAMPNANO
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _date_2_timestampNanoTZ = (o, f) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement._days_2_microsTZ.apply((o instanceof Number) ? (int)o : Integer.parseInt(o.toString()), f)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS")));
    //conversion - Timestamp to TIMESTAMPMICRO
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _timestamp_2_timestampMicroTZ = (o, f) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement._micros_2_microsTZ.apply((o instanceof Number) ? (long)o : Long.parseLong(o.toString()), f)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")));
    //conversion - Date to TIMESTAMPMICRO
    private static final WriteStatement.toConversion<Object, org.apache.arrow.vector.types.pojo.Field, String> _date_2_timestampMicroTZ = (o, f) -> String.format("cast('%s' as timestamp)", (o == null) ? "null" : DateTimeUtils.microsToLocalDateTime(WriteStatement._days_2_microsTZ.apply((o instanceof Number) ? (int)o : Integer.parseInt(o.toString()), f)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")));


    private void initialize() {
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DATEDAY.getClass().getTypeName(), TimestampType.class.getTypeName()), WriteStatement._timestamp_2_dateDay);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DATEDAY.getClass().getTypeName(), DateType.class.getTypeName()), WriteStatement._date_2_dateDay);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESTAMPNANOTZ.getClass().getTypeName(), TimestampType.class.getTypeName()), WriteStatement._timestamp_2_timestampNanoTZ);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESTAMPNANOTZ.getClass().getTypeName(), DateType.class.getTypeName()), WriteStatement._date_2_timestampNanoTZ);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESTAMPMICROTZ.getClass().getTypeName(), TimestampType.class.getTypeName()), WriteStatement._timestamp_2_timestampMicroTZ);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESTAMPMICROTZ.getClass().getTypeName(), DateType.class.getTypeName()), WriteStatement._date_2_timestampMicroTZ);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESTAMPSECTZ.getClass().getTypeName(), TimestampType.class.getTypeName()), WriteStatement._timestamp_2_timestampSecTZ);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESTAMPSECTZ.getClass().getTypeName(), DateType.class.getTypeName()), WriteStatement._date_2_timestampSecTZ);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESTAMPMILLITZ.getClass().getTypeName(), TimestampType.class.getTypeName()), WriteStatement._timestamp_2_timestampMilliTZ);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESTAMPMILLITZ.getClass().getTypeName(), DateType.class.getTypeName()), WriteStatement._date_2_timestampMilliTZ);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESTAMPNANO.getClass().getTypeName(), TimestampType.class.getTypeName()), WriteStatement._timestamp_2_timestampNano);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESTAMPNANO.getClass().getTypeName(), DateType.class.getTypeName()), WriteStatement._date_2_timestampNano);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESTAMPMICRO.getClass().getTypeName(), TimestampType.class.getTypeName()), WriteStatement._timestamp_2_timestampMicro);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESTAMPMICRO.getClass().getTypeName(), DateType.class.getTypeName()), WriteStatement._date_2_timestampMicro);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESTAMPSEC.getClass().getTypeName(), TimestampType.class.getTypeName()), WriteStatement._timestamp_2_timestampSec);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESTAMPSEC.getClass().getTypeName(), DateType.class.getTypeName()), WriteStatement._date_2_timestampSec);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESTAMPMILLI.getClass().getTypeName(), TimestampType.class.getTypeName()), WriteStatement._timestamp_2_timestampMilli);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESTAMPMILLI.getClass().getTypeName(), DateType.class.getTypeName()), WriteStatement._date_2_timestampMilli);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMEMICRO.getClass().getTypeName(), StringType.class.getTypeName()), WriteStatement._string_2_timeMicro);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMEMICRO.getClass().getTypeName(), TimestampType.class.getTypeName()), WriteStatement._timestamp_2_timeMicro);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESEC.getClass().getTypeName(), StringType.class.getTypeName()), WriteStatement._string_2_timeSec);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMESEC.getClass().getTypeName(), TimestampType.class.getTypeName()), WriteStatement._timestamp_2_timeSec);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMENANO.getClass().getTypeName(), StringType.class.getTypeName()), WriteStatement._string_2_timeNano);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMENANO.getClass().getTypeName(), TimestampType.class.getTypeName()), WriteStatement._timestamp_2_timeNano);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMEMILLI.getClass().getTypeName(), StringType.class.getTypeName()), WriteStatement._string_2_timeMilli);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TIMEMILLI.getClass().getTypeName(), TimestampType.class.getTypeName()), WriteStatement._timestamp_2_timeMilli);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DATEMILLI.getClass().getTypeName(), DateType.class.getTypeName()), WriteStatement._date_2_dateMilli);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DATEMILLI.getClass().getTypeName(), TimestampType.class.getTypeName()), WriteStatement._timestamp_2_dateMilli);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DECIMAL256.getClass().getTypeName(), DecimalType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DECIMAL256.getClass().getTypeName(), DoubleType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DECIMAL256.getClass().getTypeName(), FloatType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DECIMAL256.getClass().getTypeName(), ByteType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DECIMAL256.getClass().getTypeName(), ShortType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DECIMAL256.getClass().getTypeName(), IntegerType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DECIMAL256.getClass().getTypeName(), LongType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DECIMAL.getClass().getTypeName(), DecimalType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DECIMAL.getClass().getTypeName(), DoubleType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DECIMAL.getClass().getTypeName(), FloatType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DECIMAL.getClass().getTypeName(), ByteType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DECIMAL.getClass().getTypeName(), ShortType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DECIMAL.getClass().getTypeName(), IntegerType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.DECIMAL.getClass().getTypeName(), LongType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.FLOAT8.getClass().getTypeName(), DoubleType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.FLOAT8.getClass().getTypeName(), FloatType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.FLOAT8.getClass().getTypeName(), ByteType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.FLOAT8.getClass().getTypeName(), ShortType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.FLOAT8.getClass().getTypeName(), IntegerType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.FLOAT8.getClass().getTypeName(), LongType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.FLOAT4.getClass().getTypeName(), DoubleType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.FLOAT4.getClass().getTypeName(), FloatType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.FLOAT4.getClass().getTypeName(), ByteType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.FLOAT4.getClass().getTypeName(), ShortType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.FLOAT4.getClass().getTypeName(), IntegerType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.FLOAT4.getClass().getTypeName(), LongType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.BIT.getClass().getTypeName(), BooleanType.class.getTypeName()), WriteStatement._boolean_2_bit);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.BIT.getClass().getTypeName(), StringType.class.getTypeName()), WriteStatement._string_2_bit);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.BIT.getClass().getTypeName(), DecimalType.class.getTypeName()), WriteStatement._number_2_bit);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.BIT.getClass().getTypeName(), DoubleType.class.getTypeName()), WriteStatement._number_2_bit);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.BIT.getClass().getTypeName(), FloatType.class.getTypeName()), WriteStatement._number_2_bit);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.BIT.getClass().getTypeName(), ByteType.class.getTypeName()), WriteStatement._number_2_bit);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.BIT.getClass().getTypeName(), ShortType.class.getTypeName()), WriteStatement._number_2_bit);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.BIT.getClass().getTypeName(), IntegerType.class.getTypeName()), WriteStatement._number_2_bit);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.BIT.getClass().getTypeName(), LongType.class.getTypeName()), WriteStatement._number_2_bit);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.BIGINT.getClass().getTypeName(), ByteType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.BIGINT.getClass().getTypeName(), ShortType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.BIGINT.getClass().getTypeName(), IntegerType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.BIGINT.getClass().getTypeName(), LongType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.UINT8.getClass().getTypeName(), ByteType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.UINT8.getClass().getTypeName(), ShortType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.UINT8.getClass().getTypeName(), IntegerType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.UINT8.getClass().getTypeName(), LongType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.UINT4.getClass().getTypeName(), ByteType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.UINT4.getClass().getTypeName(), ShortType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.UINT4.getClass().getTypeName(), IntegerType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.INT.getClass().getTypeName(), ByteType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.INT.getClass().getTypeName(), ShortType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.INT.getClass().getTypeName(), IntegerType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.UINT2.getClass().getTypeName(), ByteType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.UINT2.getClass().getTypeName(), ShortType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.UINT1.getClass().getTypeName(), ByteType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.SMALLINT.getClass().getTypeName(), ShortType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.SMALLINT.getClass().getTypeName(), ByteType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.TINYINT.getClass().getTypeName(), ByteType.class.getTypeName()), WriteStatement._number_2_num);
        this._toObjectConverters.put(String.format("%s-%s", Types.MinorType.VARCHAR.getClass().getTypeName(), StringType.class.getTypeName()), WriteStatement._string_2_varchar);
    }

    //micros to micros-TZ
    private static final WriteStatement.toConversion<Long, org.apache.arrow.vector.types.pojo.Field, Long> _micros_2_microsTZ = (micros, field) -> {
        ArrowType.Timestamp arrowType = (ArrowType.Timestamp)field.getFieldType().getType();
        return DateTimeUtils.convertTz(micros, ZoneId.systemDefault(), ZoneId.of(arrowType.getTimezone()));
    };
    //days to micros-TZ
    private static final WriteStatement.toConversion<Integer, org.apache.arrow.vector.types.pojo.Field, Long> _days_2_microsTZ = (days, field) -> {
        ArrowType.Timestamp arrowType = (ArrowType.Timestamp)field.getFieldType().getType();
        return DateTimeUtils.daysToMicros(days, ZoneId.of(arrowType.getTimezone()));
    };
    //quote all fields in the collection
    private static final BiFunction<String[], String, String[]> _quote = (fields, quote) -> Arrays.stream(fields).map(field -> String.format("%s%s%s", quote, field, quote)).toArray(String[]::new);
}
