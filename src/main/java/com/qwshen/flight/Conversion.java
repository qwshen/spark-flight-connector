package com.qwshen.flight;

import com.google.common.collect.Streams;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.holders.*;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.IntervalUtils;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConverters;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Defines utility methods for transformation & conversion
 */
public final class Conversion implements Serializable {
    //the fromConversion interface
    @FunctionalInterface
    private interface fromConversion<X, Y, Z, R> extends Serializable {
        R apply(X x, Y y, Z z);
    }
    //the toConversion interface
    @FunctionalInterface
    private interface toConversion<A, B, C, D> extends Serializable {
        void apply(A a, B b, C c, D d);
    }
    //the cast method
    @SuppressWarnings("unchecked")
    private static <V extends org.apache.arrow.vector.FieldVector> V cast(org.apache.arrow.vector.FieldVector fv) {
        try {
            return (V)fv;
        } catch (Exception e) {
            throw new RuntimeException(String.format("ArrowVector casting to [%s] failed.", fv.getClass().getTypeName()), e);
        }
    }

    //the from-converter container
    private final java.util.Map<String, fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector>> _fromConverters = new java.util.HashMap<>();
    //the to-converter container
    private final java.util.Map<String, toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType>> _toConverters = new java.util.HashMap<>();
    //the to-object-converter container
    private final java.util.Map<String, toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType>> _toObjectConverters = new java.util.HashMap<>();

    /**
     * Construct a Vector object
     */
    private Conversion() {
        this.initializeFrom();
        this.initializeTo();
    }

    /**
     * Convert an arrow FieldVector into a custom FieldVector
     * @param vector - the arrow FieldVector
     * @param type - the type of all elements in the vector
     * @param rowCount - the number of rows in the vector
     * @return - a custom FieldVector hodling java-type objects
     */
    public FieldVector convert(org.apache.arrow.vector.FieldVector vector, FieldType type, int rowCount) {
        String key = String.format("%s-%s", vector.getClass().getTypeName(), type.getTypeID());
        if (!this._fromConverters.containsKey(key)) {
            throw new RuntimeException(String.format("THe %s doesn't have a converter defined.", key));
        }
        return this._fromConverters.get(key).apply(vector, rowCount, type);
    }

    /**
     * Populate an arrow FieldVector with data in rows
     * @param vector - the target arrow vector
     * @param rows - the rows containing source data
     * @param idxColumn - the index of column in rows whose data is to be used for the population
     * @param type - the data-type in the target column.
     */
    public void populate(org.apache.arrow.vector.FieldVector vector, InternalRow[] rows, int idxColumn, DataType type) {
        String key = vector.getClass().getTypeName();
        if (!this._toConverters.containsKey(key)) {
            throw new RuntimeException(String.format("The %s doesn't have a converter defined.", key));
        }
        this._toConverters.get(key).apply(vector, rows, idxColumn, type);
    }
    //populate the value object into the vector
    private void populateObject(org.apache.arrow.vector.FieldVector vector, int index, Object value, DataType type) {
        String key = vector.getClass().getTypeName();
        if (!this._toObjectConverters.containsKey(key)) {
            throw new RuntimeException(String.format("The %s doesn't have a converter defined.", key));
        }
        this._toObjectConverters.get(key).apply(vector, index, value, type);
    }

    /**
     * Initialize from-converters
     */
    private void initializeFrom() {
        this._fromConverters.put(String.format("%s-%s", TinyIntVector.class.getTypeName(), FieldType.IDs.BYTE), Conversion._fromTinyInt);
        this._fromConverters.put(String.format("%s-%s", SmallIntVector.class.getTypeName(), FieldType.IDs.SHORT), Conversion._fromSmallInt);
        this._fromConverters.put(String.format("%s-%s", IntVector.class.getTypeName(), FieldType.IDs.INT), Conversion._fromInt);
        this._fromConverters.put(String.format("%s-%s", BigIntVector.class.getTypeName(), FieldType.IDs.LONG), Conversion._fromBigInt);
        this._fromConverters.put(String.format("%s-%s", UInt1Vector.class.getTypeName(), FieldType.IDs.SHORT), Conversion._fromUInt1);
        this._fromConverters.put(String.format("%s-%s", UInt2Vector.class.getTypeName(), FieldType.IDs.INT), Conversion._fromUInt2);
        this._fromConverters.put(String.format("%s-%s", UInt4Vector.class.getTypeName(), FieldType.IDs.INT), Conversion._fromUInt4_INT);
        this._fromConverters.put(String.format("%s-%s", UInt4Vector.class.getTypeName(), FieldType.IDs.LONG), Conversion._fromUInt4_LONG);
        this._fromConverters.put(String.format("%s-%s", UInt8Vector.class.getTypeName(), FieldType.IDs.LONG), Conversion._fromUInt8_LONG);
        this._fromConverters.put(String.format("%s-%s", UInt8Vector.class.getTypeName(), FieldType.IDs.BIGINT), Conversion._fromUInt8_BIGINT);
        this._fromConverters.put(String.format("%s-%s", Float4Vector.class.getTypeName(), FieldType.IDs.FLOAT), Conversion._fromFloat4);
        this._fromConverters.put(String.format("%s-%s", Float8Vector.class.getTypeName(), FieldType.IDs.DOUBLE), Conversion._fromFloat8);
        this._fromConverters.put(String.format("%s-%s", DecimalVector.class.getTypeName(), FieldType.IDs.DECIMAL), Conversion._fromDecimal);
        this._fromConverters.put(String.format("%s-%s", Decimal256Vector.class.getTypeName(), FieldType.IDs.DECIMAL), Conversion._fromDecimal256);
        this._fromConverters.put(String.format("%s-%s", VarCharVector.class.getTypeName(), FieldType.IDs.VARCHAR), Conversion._fromVarChar);
        this._fromConverters.put(String.format("%s-%s", LargeVarCharVector.class.getTypeName(), FieldType.IDs.VARCHAR), Conversion._fromLargeVarChar);
        this._fromConverters.put(String.format("%s-%s", BitVector.class.getTypeName(), FieldType.IDs.BOOLEAN), Conversion._fromBit);
        this._fromConverters.put(String.format("%s-%s", DateDayVector.class.getTypeName(), FieldType.IDs.DATE), Conversion._fromDateDay);
        this._fromConverters.put(String.format("%s-%s", DateMilliVector.class.getTypeName(), FieldType.IDs.DATE), Conversion._fromDateMilli);
        this._fromConverters.put(String.format("%s-%s", TimeSecVector.class.getTypeName(), FieldType.IDs.TIME), Conversion._fromTimeSec);
        this._fromConverters.put(String.format("%s-%s", TimeMilliVector.class.getTypeName(), FieldType.IDs.TIME), Conversion._fromTimeMilli);
        this._fromConverters.put(String.format("%s-%s", TimeMicroVector.class.getTypeName(), FieldType.IDs.TIME), Conversion._fromTimeMicro);
        this._fromConverters.put(String.format("%s-%s", TimeNanoVector.class.getTypeName(), FieldType.IDs.TIME), Conversion._fromTimeNano);
        this._fromConverters.put(String.format("%s-%s", TimeStampMicroVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), Conversion._fromTimeStampMicro);
        this._fromConverters.put(String.format("%s-%s", TimeStampMicroTZVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), Conversion._fromTimeStampMicroTZ);
        this._fromConverters.put(String.format("%s-%s", TimeStampMilliVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), Conversion._fromTimeStampMilli);
        this._fromConverters.put(String.format("%s-%s", TimeStampMilliTZVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), Conversion._fromTimeStampMilliTZ);
        this._fromConverters.put(String.format("%s-%s", TimeStampSecVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), Conversion._fromTimeStampSec);
        this._fromConverters.put(String.format("%s-%s", TimeStampSecTZVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), Conversion._fromTimeStampSecTZ);
        this._fromConverters.put(String.format("%s-%s", TimeStampNanoVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), Conversion._fromTimeStampNano);
        this._fromConverters.put(String.format("%s-%s", TimeStampNanoTZVector.class.getTypeName(), FieldType.IDs.TIMESTAMP), Conversion._fromTimeStampNanoTZ);
        this._fromConverters.put(String.format("%s-%s", IntervalYearVector.class.getTypeName(), FieldType.IDs.PERIOD_YEAR_MONTH), Conversion._fromIntervalYear);
        this._fromConverters.put(String.format("%s-%s", IntervalDayVector.class.getTypeName(), FieldType.IDs.DURATION_DAY_TIME), Conversion._fromIntervalDay);
        this._fromConverters.put(String.format("%s-%s", DurationVector.class.getTypeName(), FieldType.IDs.DURATION_DAY_TIME), Conversion._fromDuration);
        this._fromConverters.put(String.format("%s-%s", IntervalMonthDayNanoVector.class.getTypeName(), FieldType.IDs.PERIOD_DURATION_MONTH_DAY_TIME), Conversion._fromMonthDay);
        this._fromConverters.put(String.format("%s-%s", NullVector.class.getTypeName(), FieldType.IDs.NULL), Conversion._fromNull);
        this._fromConverters.put(String.format("%s-%s", VarBinaryVector.class.getTypeName(), FieldType.IDs.BYTES), Conversion._fromVarBinary);
        this._fromConverters.put(String.format("%s-%s", LargeVarBinaryVector.class.getTypeName(), FieldType.IDs.BYTES), Conversion._fromLargeVarBinary);
        this._fromConverters.put(String.format("%s-%s", FixedSizeBinaryVector.class.getTypeName(), FieldType.IDs.BYTES), Conversion._fromFixedSizeBinary);
        this._fromConverters.put(String.format("%s-%s", LargeListVector.class.getTypeName(), FieldType.IDs.LIST), Conversion._fromLargeList);
        this._fromConverters.put(String.format("%s-%s", FixedSizeListVector.class.getTypeName(), FieldType.IDs.LIST), Conversion._fromFixedSizeList);
        this._fromConverters.put(String.format("%s-%s", MapVector.class.getTypeName(), FieldType.IDs.MAP), Conversion._fromMap);
        this._fromConverters.put(String.format("%s-%s", ListVector.class.getTypeName(), FieldType.IDs.LIST), Conversion._fromList);
        this._fromConverters.put(String.format("%s-%s", StructVector.class.getTypeName(), FieldType.IDs.STRUCT), Conversion._fromStruct);
    }
    //convert arrow TinyIntVector to FieldVector for BYTE
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromTinyInt = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<TinyIntVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow SmallIntVector to FieldVector for SHORT
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromSmallInt = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<SmallIntVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow IntVector to FieldVector for INT
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromInt = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<IntVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow BigIntVector to FieldVector for LONG
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromBigInt = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<BigIntVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow UInt1Vector to FieldVector for SHORT
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromUInt1 = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<UInt1Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow UInt2Vector to FieldVector for INT
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromUInt2 = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<UInt2Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow UInt4Vector to FieldVector for INT
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromUInt4_INT = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<UInt4Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow UInt4Vector to FieldVector for LONG
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromUInt4_LONG = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<UInt4Vector>cast(vector))::getObjectNoOverflow).toArray(Object[]::new));
    //convert arrow UInt8Vector to FieldVector for LONG
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromUInt8_LONG = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<UInt8Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow UInt8Vector to FieldVector for BIGINT
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromUInt8_BIGINT = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<UInt8Vector>cast(vector))::getObjectNoOverflow).toArray(Object[]::new));
    //convert arrow Float4Vector to FieldVector for FLOAT
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromFloat4 = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<Float4Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow Float8Vector to FieldVector for DOUBLE
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromFloat8 = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<Float8Vector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow DecimalVector to FieldVector for DECIMAL
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromDecimal = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<DecimalVector>cast(vector))::getObject).map(bd -> (bd == null) ? null : Conversion._bigDecimal_2_decimal.apply(bd)).toArray(Object[]::new));
    //convert arrow Decimal256Vector to FieldVector for DECIMAL
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromDecimal256 = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<Decimal256Vector>cast(vector))::getObject).map(bd -> (bd == null) ? null : Conversion._bigDecimal_2_decimal.apply(bd)).toArray(Object[]::new));
    //convert arrow VarCharVector to FieldVector for STRING
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromVarChar = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<VarCharVector>cast(vector))::getObject).map(s -> (s == null) ? null : Conversion._string_2_utf8String.apply(s.toString())).toArray(Object[]::new));
    //convert arrow LargeVarCharVector to FieldVector for STRING
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromLargeVarChar = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<LargeVarCharVector>cast(vector))::getObject).map(s -> (s == null) ? null : Conversion._string_2_utf8String.apply(s.toString())).toArray(Object[]::new));
    //convert arrow BitVector to FieldVector for STRING
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromBit = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<BitVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow DateDayVector to FieldVector for DATE
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromDateDay = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<DateDayVector>cast(vector))::getObject).map(dd -> (dd == null) ? null : Conversion._dateDay_2_int.apply(dd)).toArray(Object[]::new));
    //convert arrow DateMilliVector to FieldVector for DATE
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromDateMilli = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<DateMilliVector>cast(vector))::getObject).map(ldt -> (ldt == null) ? null : Conversion._localDateTime_2_int.apply(ldt)).toArray(Object[]::new));
    //convert arrow TimeSecVector to FieldVector for TIME
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromTimeSec = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<TimeSecVector>cast(vector))::getObject).map(ts -> (ts == null) ? null : Conversion._timeSec_2_string.apply(ts)).toArray(Object[]::new));
    //convert arrow TimeMilliVector to FieldVector for TIME
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromTimeMilli = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<TimeMilliVector>cast(vector))::getObject).map(tm -> (tm == null) ? null : Conversion._timeMilli_2_string.apply(tm)).toArray(Object[]::new));
    //convert arrow TimeMicroVector to FieldVector for TIME
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromTimeMicro = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<TimeMicroVector>cast(vector))::getObject).map(tm -> (tm == null) ? null : Conversion._timeMicro_2_string.apply(tm)).toArray(Object[]::new));
    //convert arrow TimeNanoVector to FieldVector for TIME
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromTimeNano = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<TimeNanoVector>cast(vector))::getObject).map(tn -> (tn == null) ? null : Conversion._timeNano_2_string.apply(tn)).toArray(Object[]::new));
    //convert arrow TimeStampMicroVector to FieldVector for TIMESTAMP
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromTimeStampMicro = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<TimeStampMicroVector>cast(vector))::getObject).map(tsm -> (tsm == null) ? null : Conversion._localDateTime_2_long.apply(tsm)).toArray(Object[]::new));
    //convert arrow TimeStampMicroTZVector to FieldVector for TIMESTAMP
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromTimeStampMicroTZ = (vector, size, type) -> {
        TimeStampMicroTZVector value = Conversion.cast(vector);
        return new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj(value::getObject).map(tsm -> (tsm == null) ? null : Conversion._timestampMicroTZ_2_long.apply(tsm, value.getTimeZone())).toArray(Object[]::new));
    };
    //convert arrow TimeStampMilliVector to FieldVector for TIMESTAMP
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromTimeStampMilli = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<TimeStampMilliVector>cast(vector))::getObject).map(tsm -> (tsm == null) ? null : Conversion._timestamp_2_long.apply(java.sql.Timestamp.valueOf(tsm))).toArray(Object[]::new));
    //convert arrow TimeStampMilliTZVector to FieldVector for TIMESTAMP
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromTimeStampMilliTZ = (vector, size, type) -> {
        TimeStampMilliTZVector value = Conversion.cast(vector);
        return new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj(value::getObject).map(tsm -> (tsm == null) ? null : Conversion._timestampMilliTZ_2_long.apply(tsm, value.getTimeZone())).toArray(Object[]::new));
    };
    //convert arrow TimeStampSecVector to FieldVector for TIMESTAMP
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromTimeStampSec = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<TimeStampSecVector>cast(vector))::getObject).map(ldt -> (ldt == null) ? null : Conversion._localDateTime_2_long.apply(ldt)).toArray(Object[]::new));
    //convert arrow TimeStampSecTZVector to FieldVector for TIMESTAMP
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromTimeStampSecTZ = (vector, size, type) -> {
        TimeStampSecTZVector value = Conversion.cast(vector);
        return new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj(value::getObject).map(l -> (l == null) ? null : Conversion._timestampSecTZ_2_long.apply(l, value.getTimeZone())).toArray(Object[]::new));
    };
    //convert arrow TimeStampNanoVector to FieldVector for TIMESTAMP
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromTimeStampNano = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<TimeStampNanoVector>cast(vector))::getObject).map(ldt -> (ldt == null) ? null : Conversion._localDateTime_2_long.apply(ldt)).toArray(Object[]::new));
    //convert arrow TimeStampNanoTZVector to FieldVector for TIMESTAMP
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromTimeStampNanoTZ = (vector, size, type) -> {
        TimeStampNanoTZVector value = Conversion.cast(vector);
        return new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj(value::getObject).map(l -> (l == null) ? null : Conversion._timestampNanoTZ_2_long.apply(l, value.getTimeZone())).toArray(Object[]::new));
    };
    //convert arrow IntervalYearVector to FieldVector for PERIOD_YEAR_MONTH
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromIntervalYear = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<IntervalYearVector>cast(vector))::getObject).map(p -> (p == null) ? null : Conversion._period_2_int.apply(p)).toArray(Object[]::new));
    //convert arrow IntervalDayVector to FieldVector for DURATION_DAY_TIME
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromIntervalDay = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<IntervalDayVector>cast(vector))::getObject).map(d -> (d == null) ? null : Conversion._duration_2_long.apply(d)).toArray(Object[]::new));
    //convert arrow DurationVector to FieldVector for DURATION_DAY_TIME
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromDuration = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<DurationVector>cast(vector))::getObject).map(d -> (d == null) ? null : Conversion._duration_2_long.apply(d)).toArray(Object[]::new));
    //convert arrow IntervalMonthDayNanoVector to FieldVector for PERIOD_DURATION_MONTH_DAY_TIME
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromMonthDay = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<IntervalMonthDayNanoVector>cast(vector))::getObject).map(pd -> (pd == null) ? null : Conversion._translatePeriodDuration.apply(pd)).toArray(Object[]::new));
    //convert arrow NullVector to FieldVector for NULL
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromNull = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<NullVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow VarBinaryVector to FieldVector for BYTES
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromVarBinary = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<VarBinaryVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow LargeVarBinaryVector to FieldVector for BYTES
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromLargeVarBinary = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<LargeVarBinaryVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow FixedSizeBinaryVector to FieldVector for BYTES
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromFixedSizeBinary = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<FixedSizeBinaryVector>cast(vector))::getObject).toArray(Object[]::new));
    //convert arrow LargeListVector to FieldVector for LIST
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromLargeList = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<LargeListVector>cast(vector))::getObject).map(e -> (e == null) ? null : Conversion._translateList.apply(e, Conversion.cast(vector), (FieldType.ListType)type)).toArray(Object[]::new));
    //convert arrow FixedSizeListVector to FieldVector for LIST
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromFixedSizeList = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<FixedSizeListVector>cast(vector))::getObject).map(e -> (e == null) ? null : Conversion._translateList.apply(e, Conversion.cast(vector), (FieldType.ListType)type)).toArray(Object[]::new));
    //convert arrow MapVector to FieldVector for MAP
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromMap = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<MapVector>cast(vector))::getObject).map(e -> (e == null) ? null : Conversion._translateMap.apply(e, Conversion.cast(vector), (FieldType.MapType)type)).toArray(Object[]::new));
    //convert arrow ListVector to FieldVector for LIST
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromList = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<ListVector>cast(vector))::getObject).map(e -> (e == null) ? null : Conversion._translateList.apply(e, Conversion.cast(vector), (FieldType.ListType)type)).toArray(Object[]::new));
    //convert arrow StructVector to FieldVector for STRUCT
    private static final fromConversion<org.apache.arrow.vector.FieldVector, Integer, FieldType, FieldVector> _fromStruct = (vector, size, type) -> new FieldVector(vector.getName(), type, IntStream.range(0, size).mapToObj((Conversion.<StructVector>cast(vector))::getObject).map(e -> (e == null) ? null : Conversion._map_else_struct.apply(e, Conversion.cast(vector), (FieldType.StructType)type)).toArray(Object[]::new));

    /**
     * Initialize to-converters
     */
    private void initializeTo() {
        this._toConverters.put(MapVector.class.getTypeName(), Conversion._toMap);
        this._toObjectConverters.put(MapVector.class.getTypeName(), Conversion._toMap_Object);
        this._toConverters.put(ListVector.class.getTypeName(), Conversion._toList);
        this._toObjectConverters.put(ListVector.class.getTypeName(), Conversion._toList_Object);
        this._toConverters.put(StructVector.class.getTypeName(), Conversion._toStruct);
        this._toObjectConverters.put(StructVector.class.getTypeName(), Conversion._toStruct_Object);
        this._toConverters.put(IntervalDayVector.class.getTypeName(), Conversion._toIntervalDay);
        this._toObjectConverters.put(IntervalDayVector.class.getTypeName(), Conversion._toIntervalDay_Object);
        this._toConverters.put(IntervalYearVector.class.getTypeName(), Conversion._toIntervalYear);
        this._toObjectConverters.put(IntervalYearVector.class.getTypeName(), Conversion._toIntervalYear_Object);
        this._toConverters.put(DurationVector.class.getTypeName(), Conversion._toDuration);
        this._toObjectConverters.put(DurationVector.class.getTypeName(), Conversion._toDuration_Object);
        this._toConverters.put(TimeStampNanoTZVector.class.getTypeName(), Conversion._toTimeStampNanoTZ);
        this._toObjectConverters.put(TimeStampNanoTZVector.class.getTypeName(), Conversion._toTimeStampNanoTZ_Object);
        this._toConverters.put(TimeStampNanoVector.class.getTypeName(), Conversion._toTimeStampNano);
        this._toObjectConverters.put(TimeStampNanoVector.class.getTypeName(), Conversion._toTimeStampNano_Object);
        this._toConverters.put(TimeStampSecTZVector.class.getTypeName(), Conversion._toTimeStampSecTZ);
        this._toObjectConverters.put(TimeStampSecTZVector.class.getTypeName(), Conversion._toTimeStampSecTZ_Object);
        this._toConverters.put(TimeStampSecVector.class.getTypeName(), Conversion._toTimeStampSec);
        this._toObjectConverters.put(TimeStampSecVector.class.getTypeName(), Conversion._toTimeStampSec_Object);
        this._toConverters.put(TimeStampMilliTZVector.class.getTypeName(), Conversion._toTimeStampMilliTZ);
        this._toObjectConverters.put(TimeStampMilliTZVector.class.getTypeName(), Conversion._toTimeStampMilliTZ_Object);
        this._toConverters.put(TimeStampMilliVector.class.getTypeName(), Conversion._toTimeStampMilli);
        this._toObjectConverters.put(TimeStampMilliVector.class.getTypeName(), Conversion._toTimeStampMilli_Object);
        this._toConverters.put(TimeStampMicroTZVector.class.getTypeName(), Conversion._toTimeStampMicroTZ);
        this._toObjectConverters.put(TimeStampMicroTZVector.class.getTypeName(), Conversion._toTimeStampMicroTZ_Object);
        this._toConverters.put(TimeStampMicroVector.class.getTypeName(), Conversion._toTimeStampMicro);
        this._toObjectConverters.put(TimeStampMicroVector.class.getTypeName(), Conversion._toTimeStampMicro_Object);
        this._toConverters.put(TimeNanoVector.class.getTypeName(), Conversion._toTimeNano);
        this._toObjectConverters.put(TimeNanoVector.class.getTypeName(), Conversion._toTimeNano_Object);
        this._toConverters.put(TimeMicroVector.class.getTypeName(), Conversion._toTimeMicro);
        this._toObjectConverters.put(TimeMicroVector.class.getTypeName(), Conversion._toTimeMicro_Object);
        this._toConverters.put(TimeMilliVector.class.getTypeName(), Conversion._toTimeMilli);
        this._toObjectConverters.put(TimeMilliVector.class.getTypeName(), Conversion._toTimeMilli_Object);
        this._toConverters.put(TimeSecVector.class.getTypeName(), Conversion._toTimeSec);
        this._toObjectConverters.put(TimeSecVector.class.getTypeName(), Conversion._toTimeSec_Object);
        this._toConverters.put(DateMilliVector.class.getTypeName(), Conversion._toDateMilli);
        this._toObjectConverters.put(DateMilliVector.class.getTypeName(), Conversion._toDateMilli_Object);
        this._toConverters.put(DateDayVector.class.getTypeName(), Conversion._toDateDay);
        this._toObjectConverters.put(DateDayVector.class.getTypeName(), Conversion._toDateDay_Object);
        this._toConverters.put(BitVector.class.getTypeName(), Conversion._toBit);
        this._toObjectConverters.put(BitVector.class.getTypeName(), Conversion._toBit_Object);
        this._toConverters.put(LargeVarCharVector.class.getTypeName(), Conversion._toLargeVarChar);
        this._toObjectConverters.put(LargeVarCharVector.class.getTypeName(), Conversion._toLargeVarChar_Object);
        this._toConverters.put(VarCharVector.class.getTypeName(), Conversion._toVarChar);
        this._toObjectConverters.put(VarCharVector.class.getTypeName(), Conversion._toVarChar_Object);
        this._toConverters.put(Decimal256Vector.class.getTypeName(), Conversion._toDecimal256);
        this._toObjectConverters.put(Decimal256Vector.class.getTypeName(), Conversion._toDecimal256_Object);
        this._toConverters.put(DecimalVector.class.getTypeName(), Conversion._toDecimal);
        this._toObjectConverters.put(DecimalVector.class.getTypeName(), Conversion._toDecimal_Object);
        this._toConverters.put(Float8Vector.class.getTypeName(), Conversion._toFloat8);
        this._toObjectConverters.put(Float8Vector.class.getTypeName(), Conversion._toFloat8_Object);
        this._toConverters.put(Float4Vector.class.getTypeName(), Conversion._toFloat4);
        this._toObjectConverters.put(Float4Vector.class.getTypeName(), Conversion._toFloat4_Object);
        this._toConverters.put(UInt8Vector.class.getTypeName(), Conversion._toUInt8);
        this._toObjectConverters.put(UInt8Vector.class.getTypeName(), Conversion._toUInt8_Object);
        this._toConverters.put(UInt4Vector.class.getTypeName(), Conversion._toUInt4);
        this._toObjectConverters.put(UInt4Vector.class.getTypeName(), Conversion._toUInt4_Object);
        this._toConverters.put(UInt2Vector.class.getTypeName(), Conversion._toUInt2);
        this._toObjectConverters.put(UInt2Vector.class.getTypeName(), Conversion._toUInt2_Object);
        this._toConverters.put(UInt1Vector.class.getTypeName(), Conversion._toUInt1);
        this._toObjectConverters.put(UInt1Vector.class.getTypeName(), Conversion._toUInt1_Object);
        this._toConverters.put(BigIntVector.class.getTypeName(), Conversion._toBigInt);
        this._toObjectConverters.put(BigIntVector.class.getTypeName(), Conversion._toBigInt_Object);
        this._toConverters.put(IntVector.class.getTypeName(), Conversion._toInt);
        this._toObjectConverters.put(IntVector.class.getTypeName(), Conversion._toInt_Object);
        this._toConverters.put(SmallIntVector.class.getTypeName(), Conversion._toSmallInt);
        this._toObjectConverters.put(SmallIntVector.class.getTypeName(), Conversion._toSmallInt_Object);
        this._toConverters.put(TinyIntVector.class.getTypeName(), Conversion._toTinyInt);
        this._toObjectConverters.put(TinyIntVector.class.getTypeName(), Conversion._toTinyInt_Object);
    }
    //convert BYTE to arrow TinyIntVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toTinyInt = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toTinyInt_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableTinyIntHolder _nullTinyInt = new NullableTinyIntHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toTinyInt_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<TinyIntVector>cast(vector).setSafe(row, Conversion._nullTinyInt);
        } else {
            Conversion.<TinyIntVector>cast(vector).setSafe(row, (value instanceof Number) ? (Byte)value : Byte.parseByte(value.toString()));
        }
    };
    //convert SHORT to arrow SmallIntVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toSmallInt = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toSmallInt_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableSmallIntHolder _nullSmallInt = new NullableSmallIntHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toSmallInt_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<SmallIntVector>cast(vector).setSafe(row, Conversion._nullSmallInt);
        } else {
            Conversion.<SmallIntVector>cast(vector).setSafe(row, (value instanceof Number) ? (Short)value : Short.parseShort(value.toString()));
        }
    };
    //convert INT to arrow IntVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toInt = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toInt_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableIntHolder _nullInt = new NullableIntHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toInt_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<IntVector>cast(vector).setSafe(row, Conversion._nullInt);
        } else {
            Conversion.<IntVector>cast(vector).setSafe(row, (value instanceof Number) ? (Integer)value : Integer.parseInt(value.toString()));
        }
    };
    //convert LONG to arrow BigIntVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toBigInt = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toBigInt_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableBigIntHolder _nullBigInt = new NullableBigIntHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toBigInt_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<BigIntVector>cast(vector).setSafe(row, Conversion._nullBigInt);
        } else {
            Conversion.<BigIntVector>cast(vector).setSafe(row, (value instanceof Number) ? (Long)value : Long.parseLong(value.toString()));
        };
    };
    //convert SHORT to arrow UInt1Vector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toUInt1 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toUInt1_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableUInt1Holder _nullUInt1 = new NullableUInt1Holder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toUInt1_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<UInt1Vector>cast(vector).setSafe(row, Conversion._nullUInt1);
        } else {
            Conversion.<UInt1Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Short)value : Short.parseShort(value.toString()));
        }
    };
    //convert INT to arrow UInt2Vector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toUInt2 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toUInt2_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableUInt2Holder _nullUInt2 = new NullableUInt2Holder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toUInt2_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<UInt2Vector>cast(vector).setSafe(row, Conversion._nullUInt2);
        } else {
            Conversion.<UInt2Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Integer)value : Integer.parseInt(value.toString()));
        }
    };
    //convert INT to arrow UInt4Vector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toUInt4 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toUInt4_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableUInt4Holder _nullUInt4 = new NullableUInt4Holder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toUInt4_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<UInt4Vector>cast(vector).setSafe(row, Conversion._nullUInt4);
        } else {
            Conversion.<UInt4Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Integer)value : Integer.parseInt(value.toString()));
        }
    };
    //convert LONG to arrow UInt8Vector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toUInt8 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toUInt8_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableUInt8Holder _nullUInt8 = new NullableUInt8Holder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toUInt8_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<UInt8Vector>cast(vector).setSafe(row, Conversion._nullUInt8);
        } else {
            Conversion.<UInt8Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Long)value : Long.parseLong(value.toString()));
        }
    };
    //convert FLOAT to arrow Float4Vector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toFloat4 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toFloat4_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableFloat4Holder _nullFloat4 = new NullableFloat4Holder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toFloat4_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<Float4Vector>cast(vector).setSafe(row, Conversion._nullFloat4);
        } else {
            Conversion.<Float4Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Float)value : Float.parseFloat(value.toString()));
        }
    };
    //convert DOUBLE to arrow Float8Vector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toFloat8 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toFloat8_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableFloat8Holder _nullFloat8 = new NullableFloat8Holder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toFloat8_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<Float8Vector>cast(vector).setSafe(row, Conversion._nullFloat8);
        } else {
            Conversion.<Float8Vector>cast(vector).setSafe(row, (value instanceof Number) ? (Double)value : Double.parseDouble(value.toString()));
        }
    };
    //convert DECIMAL to arrow DecimalVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toDecimal = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toDecimal_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableDecimalHolder _nullDecimal = new NullableDecimalHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toDecimal_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<DecimalVector>cast(vector).setSafe(row, Conversion._nullDecimal);
        } else {
            Conversion.<DecimalVector>cast(vector).setSafe(row, (value instanceof Decimal) ? ((Decimal)value).toJavaBigDecimal() : BigDecimal.valueOf(Double.parseDouble(value.toString())));
        }
    };
    //convert DECIMAL to arrow Decimal256Vector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toDecimal256 = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toDecimal256_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableDecimal256Holder _nullDecimal256 = new NullableDecimal256Holder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toDecimal256_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<Decimal256Vector>cast(vector).setSafe(row, Conversion._nullDecimal256);
        } else {
            Conversion.<Decimal256Vector>cast(vector).setSafe(row, (value instanceof Decimal) ? ((Decimal)value).toJavaBigDecimal() : BigDecimal.valueOf(Double.parseDouble(value.toString())));
        }
    };
    //convert STRING to arrow VarCharVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toVarChar = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toVarChar_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableVarCharHolder _nullVarChar = new NullableVarCharHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toVarChar_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<VarCharVector>cast(vector).setSafe(row, Conversion._nullVarChar);
        } else {
            Conversion.<VarCharVector>cast(vector).setSafe(row, new org.apache.arrow.vector.util.Text(value.toString()));
        }
    };
    //convert STRING to arrow LargeVarCharVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toLargeVarChar = (vector, rows, idxColumn, type) -> IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toLargeVarChar_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
    private static final NullableLargeVarCharHolder _nullLargeVarChar = new NullableLargeVarCharHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toLargeVarChar_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<LargeVarCharVector>cast(vector).setSafe(row, Conversion._nullLargeVarChar);
        } else {
            Conversion.<LargeVarCharVector>cast(vector).setSafe(row, new org.apache.arrow.vector.util.Text(value.toString()));
        }
    };
    //convert BOOLEAN to arrow BitVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toBit = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.BooleanType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toBit_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow Bit.");
        }
    };
    private static final NullableBitHolder _nullBit = new NullableBitHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toBit_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<BitVector>cast(vector).setSafe(row, Conversion._nullBit);
        } else {
            Conversion.<BitVector>cast(vector).setSafe(row, (Boolean)value ? 1 : 0);
        }
    };
    //convert DATE to arrow DateDayVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toDateDay = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.DateType || type == DataTypes.TimestampType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toDateDay_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow DateDay.");
        }
    };
    private static final NullableDateDayHolder _nullDateDay = new NullableDateDayHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toDateDay_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<DateDayVector>cast(vector).setSafe(row, Conversion._nullDateDay);
        } else if (type == DataTypes.DateType) {
            Conversion.<DateDayVector>cast(vector).setSafe(row, (value instanceof Number) ? (Integer)value : Integer.parseInt(value.toString()));
        } else if (type == DataTypes.TimestampType) {
            Conversion.<DateDayVector>cast(vector).setSafe(row, DateTimeUtils.microsToDays((value instanceof Number) ? (Long)value : Long.parseLong(value.toString()), ZoneId.systemDefault()));
        }
    };
    //convert DATE to arrow DateMilliVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toDateMilli = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.DateType || type == DataTypes.TimestampType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toDateMilli_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow DateMilli.");
        }
    };
    private static final NullableDateMilliHolder _nullDateMilli = new NullableDateMilliHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toDateMilli_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<DateMilliVector>cast(vector).setSafe(row, Conversion._nullDateMilli);
        } else {
            LocalDateTime ldt = (type == DataTypes.DateType) ? LocalDateTime.of(DateTimeUtils.daysToLocalDate((value instanceof Number) ? (Integer)value : Integer.parseInt(value.toString())), LocalTime.of(0, 0))
                : (type == DataTypes.TimestampType) ? DateTimeUtils.microsToLocalDateTime((value instanceof Number) ? (Long)value : Long.parseLong(value.toString())) : LocalDateTime.now();
            Conversion.<DateMilliVector>cast(vector).setSafe(row, Timestamp.valueOf(ldt).getTime());
        }
    };
    //convert TIME to arrow TimeSecVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toTimeSec = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.StringType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toTimeSec_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeSec.");
        }
    };
    private static final NullableTimeSecHolder _nullTimeSec = new NullableTimeSecHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toTimeSec_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<TimeSecVector>cast(vector).setSafe(row, Conversion._nullTimeSec);
        } else if (type == DataTypes.TimestampType) {
            Conversion.<TimeSecVector>cast(vector).setSafe(row, (int)(Conversion._micros_2_epochNanos.apply((value instanceof Number) ? (Long)value : Long.parseLong(value.toString()))/1000000000L));
        } else if (type == DataTypes.StringType) {
            Conversion.<TimeSecVector>cast(vector).setSafe(row, (int)(Conversion._timestr_2_nanos.apply(value.toString())/1000000000L));
        }
    };
    //convert TIME to arrow TimeMilliVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toTimeMilli = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.StringType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toTimeMilli_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeMilli.");
        }
    };
    private static final NullableTimeMilliHolder _nullTimeMilli = new NullableTimeMilliHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toTimeMilli_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<TimeMilliVector>cast(vector).setSafe(row, Conversion._nullTimeMilli);
        } else if (type == DataTypes.TimestampType) {
            Conversion.<TimeMilliVector>cast(vector).setSafe(row, (int)(Conversion._micros_2_epochNanos.apply((value instanceof Number) ? (Long)value : Long.parseLong(value.toString()))/1000000L));
        } else if (type == DataTypes.StringType) {
            Conversion.<TimeMilliVector>cast(vector).setSafe(row, (int)(Conversion._timestr_2_nanos.apply(value.toString())/1000000L));
        }
    };
    //convert TIME arrow TimeMicroVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toTimeMicro = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.StringType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toTimeMicro_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeMicro.");
        }
    };
    private static final NullableTimeMicroHolder _nullTimeMicro = new NullableTimeMicroHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toTimeMicro_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<TimeMicroVector>cast(vector).setSafe(row, Conversion._nullTimeMicro);
        } else if (type == DataTypes.TimestampType) {
           Conversion.<TimeMicroVector>cast(vector).setSafe(row, Conversion._micros_2_epochNanos.apply((value instanceof Number) ? (Long)value : Long.parseLong(value.toString()))/1000L);
        } else if (type == DataTypes.StringType) {
            Conversion.<TimeMicroVector>cast(vector).setSafe(row, Conversion._timestr_2_nanos.apply(value.toString())/1000L);
        }
    };
    //convert TIME to arrow TimeNanoVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toTimeNano = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.StringType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toTimeNano_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeNano.");
        }
    };
    private static final NullableTimeNanoHolder _nullTimeNano = new NullableTimeNanoHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toTimeNano_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<TimeNanoVector>cast(vector).setSafe(row, Conversion._nullTimeNano);
        } else if (type == DataTypes.TimestampType) {
            Conversion.<TimeNanoVector>cast(vector).setSafe(row, Conversion._micros_2_epochNanos.apply((value instanceof Number) ? (Long)value : Long.parseLong(value.toString())));
        } else if (type == DataTypes.StringType) {
            Conversion.<TimeNanoVector>cast(vector).setSafe(row, Conversion._timestr_2_nanos.apply(value.toString()));
        }
    };
    //convert TIMESTAMP to arrow TimeStampMicroVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toTimeStampMicro = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toTimeStampMicro_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeStampMicro.");
        }
    };
    private static final NullableTimeStampMicroHolder _nullTimeStampMicro = new NullableTimeStampMicroHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toTimeStampMicro_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<TimeStampMicroVector>cast(vector).setSafe(row, Conversion._nullTimeStampMicro);
        } else if (type == DataTypes.TimestampType) {
            Conversion.<TimeStampMicroVector>cast(vector).setSafe(row, ((value instanceof Number) ? (Long)value : Long.parseLong(value.toString())));
        } else if (type == DataTypes.DateType) {
            Conversion.<TimeStampMicroVector>cast(vector).setSafe(row, Conversion._days_2_micros.apply((value instanceof Number) ? (Integer)value : Integer.parseInt(value.toString()), ZoneId.systemDefault()));
        }
    };
    //convert TIMESTAMP to arrow TimeStampMicroTZVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toTimeStampMicroTZ = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toTimeStampMicroTZ_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeStampMicroTZ.");
        }
    };
    private static final NullableTimeStampMicroTZHolder _nullTimeStampMicroTZ = new NullableTimeStampMicroTZHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toTimeStampMicroTZ_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<TimeStampMicroTZVector>cast(vector).setSafe(row, Conversion._nullTimeStampMicroTZ);
        } else if (type == DataTypes.TimestampType) {
            Conversion.<TimeStampMicroTZVector>cast(vector).setSafe(row, DateTimeUtils.convertTz((value instanceof Number) ? (Long)value : Long.parseLong(value.toString()), ZoneId.systemDefault(), ZoneId.of(Conversion.<TimeStampMicroTZVector>cast(vector).getTimeZone())));
        } else if (type == DataTypes.DateType) {
            Conversion.<TimeStampMicroTZVector>cast(vector).setSafe(row, Conversion._days_2_micros.apply((value instanceof Number) ? (Integer)value : Integer.parseInt(value.toString()), ZoneId.of(Conversion.<TimeStampMicroTZVector>cast(vector).getTimeZone())));
        }
    };
    //convert TIMESTAMP to arrow TimeStampMilliVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toTimeStampMilli = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toTimeStampMilli_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeStampMilli.");
        }
    };
    private static final NullableTimeStampMilliHolder _nullTimeStampMilli = new NullableTimeStampMilliHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toTimeStampMilli_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<TimeStampMilliVector>cast(vector).setSafe(row, Conversion._nullTimeStampMilli);
        } else if (type == DataTypes.TimestampType) {
            Conversion.<TimeStampMilliVector>cast(vector).setSafe(row, Conversion._micros_2_millis.apply(((value instanceof Number) ? (Long)value : Long.parseLong(value.toString()))));
        } else if (type == DataTypes.DateType) {
            Conversion.<TimeStampMilliVector>cast(vector).setSafe(row, Conversion._micros_2_millis.apply(Conversion._days_2_micros.apply((value instanceof Number) ? (Integer)value : Integer.parseInt(value.toString()), ZoneId.systemDefault())));
        }
    };
    //convert TIMESTAMP to arrow TimeStampMilliTZVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toTimeStampMilliTZ = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toTimeStampMilliTZ_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeStampMilliTZ.");
        }
    };
    private static final NullableTimeStampMilliTZHolder _nullTimeStampMilliTZ = new NullableTimeStampMilliTZHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toTimeStampMilliTZ_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<TimeStampMilliTZVector>cast(vector).setSafe(row, Conversion._nullTimeStampMilliTZ);
        } else if (type == DataTypes.TimestampType) {
            Conversion.<TimeStampMilliTZVector>cast(vector).setSafe(row, Conversion._micros_2_millis.apply(DateTimeUtils.convertTz((value instanceof Number) ? (Long)value : Long.parseLong(value.toString()), ZoneId.systemDefault(), ZoneId.of(Conversion.<TimeStampMilliTZVector>cast(vector).getTimeZone()))));
        } else if (type == DataTypes.DateType){
            Conversion.<TimeStampMilliTZVector>cast(vector).setSafe(row, Conversion._micros_2_millis.apply(Conversion._days_2_micros.apply((value instanceof Number) ? (Integer)value : Integer.parseInt(value.toString()), ZoneId.of(Conversion.<TimeStampMilliTZVector>cast(vector).getTimeZone()))));
        }
    };
    //convert TIMESTAMP to arrow TimeStampSecVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toTimeStampSec = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toTimeStampSec_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeStampSec.");
        }
    };
    private static final NullableTimeStampSecHolder _nullTimeStampSec = new NullableTimeStampSecHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toTimeStampSec_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<TimeStampSecVector>cast(vector).setSafe(row, Conversion._nullTimeStampSec);
        } else if (type == DataTypes.TimestampType) {
            Conversion.<TimeStampSecVector>cast(vector).setSafe(row, Conversion._micros_2_secs.apply(((value instanceof Number) ? (Long)value : Long.parseLong(value.toString()))));
        } else if (type == DataTypes.DateType) {
            Conversion.<TimeStampSecVector>cast(vector).setSafe(row, Conversion._micros_2_secs.apply(Conversion._days_2_micros.apply((value instanceof Number) ? (Integer)value : Integer.parseInt(value.toString()), ZoneId.systemDefault())));
        }
    };
    //convert TIMESTAMP to arrow TimeStampSecTZVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toTimeStampSecTZ = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toTimeStampSecTZ_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeStampSecTZ.");
        }
    };
    private static final NullableTimeStampSecTZHolder _nullTimeStampSecTZ = new NullableTimeStampSecTZHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toTimeStampSecTZ_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<TimeStampSecTZVector>cast(vector).setSafe(row, Conversion._nullTimeStampSecTZ);
        } else if (type == DataTypes.TimestampType) {
            Conversion.<TimeStampSecTZVector>cast(vector).setSafe(row, Conversion._micros_2_secs.apply(DateTimeUtils.convertTz((value instanceof Number) ? (Long)value : Long.parseLong(value.toString()), ZoneId.systemDefault(), ZoneId.of(Conversion.<TimeStampSecTZVector>cast(vector).getTimeZone()))));
        } else if (type == DataTypes.DateType){
            Conversion.<TimeStampSecTZVector>cast(vector).setSafe(row, Conversion._micros_2_secs.apply(Conversion._days_2_micros.apply((value instanceof Number) ? (Integer)value : Integer.parseInt(value.toString()), ZoneId.of(Conversion.<TimeStampSecTZVector>cast(vector).getTimeZone()))));
        }
    };
    //convert TIMESTAMP to arrow TimeStampNanoVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toTimeStampNano = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toTimeStampNano_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeStampNano.");
        }
    };
    private static final NullableTimeStampNanoHolder _nullTimeStampNano = new NullableTimeStampNanoHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toTimeStampNano_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<TimeStampNanoVector>cast(vector).setSafe(row, Conversion._nullTimeStampNano);
        } else if (type == DataTypes.TimestampType) {
            Conversion.<TimeStampNanoVector>cast(vector).setSafe(row, Conversion._micros_2_nanos.apply(((value instanceof Number) ? (Long)value : Long.parseLong(value.toString()))));
        } else if (type == DataTypes.DateType) {
            Conversion.<TimeStampNanoVector>cast(vector).setSafe(row, Conversion._micros_2_nanos.apply(Conversion._days_2_micros.apply((value instanceof Number) ? (Integer)value : Integer.parseInt(value.toString()), ZoneId.systemDefault())));
        }
    };
    //convert TIMESTAMP to arrow TimeStampNanoTZVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toTimeStampNanoTZ = (vector, rows, idxColumn, type) -> {
        if (type == DataTypes.TimestampType || type == DataTypes.DateType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toTimeStampNanoTZ_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow TimeStampNanoTZ.");
        }
    };
    private static final NullableTimeStampNanoTZHolder _nullTimeStampNanoTZ = new NullableTimeStampNanoTZHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toTimeStampNanoTZ_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<TimeStampNanoTZVector>cast(vector).setSafe(row, Conversion._nullTimeStampNanoTZ);
        } else if (type == DataTypes.TimestampType) {
            Conversion.<TimeStampNanoTZVector>cast(vector).setSafe(row, Conversion._micros_2_nanos.apply(DateTimeUtils.convertTz((value instanceof Number) ? (Long)value : Long.parseLong(value.toString()), ZoneId.systemDefault(), ZoneId.of(Conversion.<TimeStampNanoTZVector>cast(vector).getTimeZone()))));
        } else {
            Conversion.<TimeStampNanoTZVector>cast(vector).setSafe(row, Conversion._micros_2_nanos.apply(Conversion._days_2_micros.apply((value instanceof Number) ? (Integer)value : Integer.parseInt(value.toString()), ZoneId.of(Conversion.<TimeStampNanoTZVector>cast(vector).getTimeZone()))));
        }
    };
    //convert DURATION_DAY_TIME to arrow DurationVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toDuration = (vector, rows, idxColumn, type) -> {
        if (type instanceof DayTimeIntervalType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toDuration_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow Duration.");
        }
    };
    private static final NullableDurationHolder _nullDuration = new NullableDurationHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toDuration_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<DurationVector>cast(vector).setSafe(row, Conversion._nullDuration);
        } else {
            Conversion.<DurationVector>cast(vector).setSafe(row, (value instanceof Number) ? (Long)value : Long.parseLong(value.toString()));
        }
    };
    //convert PERIOD_YEAR_MONTH to arrow IntervalYearVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toIntervalYear = (vector, rows, idxColumn, type) -> {
        if (type instanceof YearMonthIntervalType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toIntervalYear_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow IntervalYear.");
        }
    };
    private static final NullableIntervalYearHolder _nullIntervalYear = new NullableIntervalYearHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toIntervalYear_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<IntervalYearVector>cast(vector).setSafe(row, Conversion._nullIntervalYear);
        } else {
            Conversion.<IntervalYearVector>cast(vector).setSafe(row, (value instanceof Number) ? (Integer)value : Integer.parseInt(value.toString()));
        }
    };
    //convert DURATION_DAY_TIME to arrow IntervalDayVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toIntervalDay = (vector, rows, idxColumn, type) -> {
        if (type instanceof DayTimeIntervalType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toIntervalDay_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow IntervalDay.");
        }
    };
    private static final NullableIntervalDayHolder _nullIntervalDay = new NullableIntervalDayHolder();
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toIntervalDay_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<IntervalDayVector>cast(vector).setSafe(row, Conversion._nullIntervalDay);
        } else {
            long micros = (value instanceof Number) ? (Long)value : Long.parseLong(value.toString());
            Conversion.<IntervalDayVector>cast(vector).setSafe(row, IntervalUtils.getDays(micros), (int)(micros % 1000L));
        }
    };
    //convert StructType to arrow StructVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toStruct = (vector, rows, idxColumn, type) -> {
        if (vector instanceof StructVector && type instanceof StructType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toStruct_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else if (vector instanceof StructVector && type instanceof MapType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toStruct_Map.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow Struct.");
        }
    };
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toStruct_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<StructVector>cast(vector).setNull(row);
        } else if (type instanceof StructType){
            org.apache.arrow.vector.FieldVector[] vectorChildren = Conversion.<StructVector>cast(vector).getChildrenFromFields().toArray(new org.apache.arrow.vector.FieldVector[0]);
            DataType[] dataTypes = Arrays.stream(((StructType)type).fields()).map(StructField::dataType).collect(Collectors.toList()).toArray(new DataType[0]);
            if (vectorChildren.length == dataTypes.length) {
                UnsafeRow rowsChildren = (UnsafeRow)value;
                IntStream.range(0, vectorChildren.length).forEach(idx -> Conversion.getOrCreate().populateObject(vectorChildren[idx], 0, rowsChildren.get(idx, dataTypes[idx]), dataTypes[idx]));
            } else {
                throw new RuntimeException("The data cannot be converted to arrow Struct.");
            }
        }
    };
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toStruct_Map = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<StructVector>cast(vector).setNull(row);
        } else {
            boolean populated = false;
            if (type instanceof MapType) {
                org.apache.arrow.vector.FieldVector[] vectorChildren = Conversion.<StructVector>cast(vector).getChildrenFromFields().toArray(new org.apache.arrow.vector.FieldVector[0]);
                if (vectorChildren.length == 1 && vectorChildren[0] instanceof ListVector && vectorChildren[0].getName().equals("map")) {
                    org.apache.arrow.vector.FieldVector dataVector = Conversion.<ListVector>cast(vectorChildren[0]).getDataVector();
                    if (dataVector instanceof StructVector) {
                        org.apache.arrow.vector.FieldVector[] valueChildren = Conversion.<StructVector>cast(dataVector).getChildrenFromFields().toArray(new org.apache.arrow.vector.FieldVector[0]);
                        if (valueChildren.length == 2 && valueChildren[0].getName().equals("key") && valueChildren[1].getName().equals("value")) {
                            DataType keyType = ((MapType)type).keyType();
                            DataType valueType = ((MapType)type).valueType();
                            UnsafeMapData data = (UnsafeMapData)value;
                            UnsafeArrayData keyData = data.keyArray();
                            UnsafeArrayData valueData = data.valueArray();
                            IntStream.range(0, data.numElements()).forEach(idx -> {
                                Conversion.getOrCreate().populateObject(valueChildren[0], idx, keyData.get(idx, keyType), keyType);
                                Conversion.getOrCreate().populateObject(valueChildren[1], idx, valueData.get(idx, valueType), valueType);
                            });
                            populated = true;
                        }
                    }
                }
            }
            if (!populated) {
                throw new RuntimeException("The data cannot be converted to arrow Struct.");
            }
        }
    };
    //convert ArrayType to arrow ListVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toList = (vector, rows, idxColumn, type) -> {
        if (vector instanceof ListVector && type instanceof ArrayType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toList_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow List.");
        }
    };
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toList_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<ListVector>cast(vector).setNull(row);
        } else if (type instanceof ArrayType) {
            org.apache.arrow.vector.FieldVector dataVector = Conversion.<ListVector>cast(vector).getDataVector();
            DataType dataType = ((ArrayType)type).elementType();
            UnsafeArrayData data = (UnsafeArrayData)value;
            IntStream.range(0, data.numElements()).forEach(idx -> Conversion.getOrCreate().populateObject(dataVector, idx, data.get(idx, dataType), dataType));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow List.");
        }
    };
    //convert MapType to arrow MapVector
    private static final toConversion<org.apache.arrow.vector.FieldVector, InternalRow[], Integer, DataType> _toMap = (vector, rows, idxColumn, type) -> {
        if (vector instanceof MapVector && type instanceof MapType) {
            IntStream.range(0, rows.length).forEach(idxRow -> Conversion._toMap_Object.apply(vector, idxRow, rows[idxRow].get(idxColumn, type), type));
        } else {
            throw new RuntimeException("The data cannot be converted to arrow Map.");
        }
    };
    private static final toConversion<org.apache.arrow.vector.FieldVector, Integer, Object, DataType> _toMap_Object = (vector, row, value, type) -> {
        if (value == null) {
            Conversion.<MapVector>cast(vector).setNull(row);
        } else if (type instanceof MapType) {
            org.apache.arrow.vector.FieldVector[] valueChildren = Conversion.<MapVector>cast(vector).getChildrenFromFields().toArray(new org.apache.arrow.vector.FieldVector[0]);
            if (valueChildren.length == 2) {
                DataType keyType = ((MapType)type).keyType();
                DataType valueType = ((MapType)type).valueType();
                UnsafeMapData data = (UnsafeMapData)value;
                UnsafeArrayData keyData = data.keyArray();
                UnsafeArrayData valueData = data.valueArray();
                IntStream.range(0, data.numElements()).forEach(idx -> {
                    Conversion.getOrCreate().populateObject(valueChildren[0], idx, keyData.get(idx, keyType), keyType);
                    Conversion.getOrCreate().populateObject(valueChildren[1], idx, valueData.get(idx, valueType), valueType);
                });
            } else {
                throw new RuntimeException("The data cannot be converted to arrow Map.");
            }
        }
    };

    //base converters
    private static final Function<BigDecimal, Decimal> _bigDecimal_2_decimal = Decimal::apply;
    private static final Function<String, UTF8String> _string_2_utf8String = UTF8String::fromString;
    private static final Function<Integer, Integer> _dateDay_2_int = (dd) -> DateTimeUtils.fromJavaDate(java.sql.Date.valueOf(LocalDate.ofEpochDay(dd)));
    private static final Function<LocalDateTime, Integer> _localDateTime_2_int = (ldt) -> DateTimeUtils.fromJavaDate(java.sql.Date.valueOf(ldt.toLocalDate()));
    private static final Function<LocalDateTime, Long> _localDateTime_2_long = (ldt) -> DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf(ldt));
    private static final BiFunction<Long, String, Long> _timestampSecTZ_2_long = (ss, zone) -> DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf(LocalDateTime.from(Instant.ofEpochSecond(ss).atZone(ZoneId.of(zone)))));
    private static final BiFunction<Long, String, Long> _timestampMilliTZ_2_long = (mss, zone) -> DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf(LocalDateTime.from(Instant.ofEpochMilli(mss).atZone(ZoneId.of(zone)))));
    private static final BiFunction<Long, String, Long> _timestampNanoTZ_2_long = (ns, zone) -> DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf(LocalDateTime.from(Instant.ofEpochMilli(ns / 1000000L).atZone(ZoneId.of(zone)))));
    private static final BiFunction<Long, String, Long> _timestampMicroTZ_2_long = (ms, zone) -> DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf(LocalDateTime.from(Instant.ofEpochMilli(ms / 1000L).atZone(ZoneId.of(zone))).plusNanos(ms % 1_000 * 1000L)));
    private static final Function<java.sql.Timestamp, Long> _timestamp_2_long = DateTimeUtils::fromJavaTimestamp;
    private static final Function<Integer, UTF8String> _timeSec_2_string = (ts) -> {
        int hours = ts / 3600;
        int minutes = (ts - hours * 3600) / 60;
        int seconds = (ts - hours * 3600 - minutes * 60);
        return Conversion._string_2_utf8String.apply(String.format("%02d:%02d:%02d", hours, minutes, seconds));
    };
    private static final Function<LocalDateTime, UTF8String> _timeMilli_2_string = (ldt) -> Conversion._string_2_utf8String.apply(String.format("%02d:%02d:%02d.%03d", ldt.getHour(), ldt.getMinute(), ldt.getSecond(), ldt.getNano()/1000000L));
    private static final Function<Long, UTF8String> _timeMicro_2_string = (ms) -> {
        int totalSeconds = (int)(ms / 1000000L);
        long microSeconds = ms - totalSeconds * 1000000L;
        int hours = totalSeconds / 3600;
        int minutes = (totalSeconds - hours * 3600) / 60;
        int seconds = (totalSeconds - hours * 3600 - minutes * 60);
        return Conversion._string_2_utf8String.apply(String.format("%02d:%02d:%02d.%06d", hours, minutes, seconds, microSeconds));
    };
    private static final Function<Long, UTF8String> _timeNano_2_string = (ns) -> {
        int totalSeconds = (int)(ns / 1000000000L);
        long nanoSeconds = totalSeconds * 1000000000L;
        int hours = totalSeconds / 3600;
        int minutes = (totalSeconds - hours * 3600) / 60;
        int seconds = (totalSeconds - hours * 3600 - minutes * 60);
        return Conversion._string_2_utf8String.apply(String.format("%02d:%02d:%02d.%09d", hours, minutes, seconds, nanoSeconds));
    };
    private static final Function<java.time.Duration, Long> _duration_2_long = IntervalUtils::durationToMicros;
    private static final Function<Period, Integer> _period_2_int = IntervalUtils::periodToMonths;
    private static final Function<PeriodDuration, InternalRow> _translatePeriodDuration = (pd) -> InternalRow.fromSeq(JavaConverters.asScalaBuffer(Arrays.asList(new Object[] { Conversion._period_2_int.apply(pd.getPeriod()), Conversion._duration_2_long.apply(pd.getDuration()) })));
    @SuppressWarnings("unchecked")
    private static final fromConversion<FieldType, Object, ValueVector, Object> _translate = (t, o, v) -> {
        switch (t.getTypeID()) {
            case VARCHAR:
            case CHAR:
                return Conversion._string_2_utf8String.apply(o.toString());
            case TIMESTAMP:
                return (v instanceof TimeStampMilliVector) ? Conversion._timestamp_2_long.apply((java.sql.Timestamp)o)
                    : (v instanceof TimeStampMicroTZVector) ? Conversion._timestampMicroTZ_2_long.apply((Long)o, ((TimeStampMicroTZVector)v).getTimeZone())
                    : (v instanceof TimeStampSecTZVector) ? Conversion._timestampSecTZ_2_long.apply((Long)o, ((TimeStampSecTZVector)v).getTimeZone())
                    : (v instanceof TimeStampMilliTZVector) ? Conversion._timestampMilliTZ_2_long.apply((Long)o, ((TimeStampMilliTZVector)v).getTimeZone())
                    : (v instanceof TimeStampNanoTZVector) ? Conversion._timestampNanoTZ_2_long.apply((Long)o, ((TimeStampNanoTZVector)v).getTimeZone())
                    : (v instanceof TimeStampMicroVector || v instanceof TimeStampSecVector || v instanceof TimeStampNanoVector) ? Conversion._localDateTime_2_long.apply((java.time.LocalDateTime)o) : o;
            case TIME:
                return (v instanceof TimeSecVector) ? Conversion._timeSec_2_string.apply((Integer)o)
                    : (v instanceof TimeMilliVector) ? Conversion._timeMilli_2_string.apply((LocalDateTime)o)
                    : (v instanceof TimeMicroVector) ? Conversion._timeMicro_2_string.apply((Long)o)
                    : (v instanceof TimeNanoVector) ? Conversion._timeNano_2_string.apply((Long)o) : o;
            case DATE:
                return (v instanceof DateDayVector) ? Conversion._dateDay_2_int.apply((Integer)o) : (v instanceof DateMilliVector) ? Conversion._localDateTime_2_int.apply((LocalDateTime)o) : o;
            case DECIMAL:
                return Conversion._bigDecimal_2_decimal.apply((BigDecimal)o);
            case DURATION_DAY_TIME:
                return (v instanceof IntervalDayVector || v instanceof DurationVector) ? Conversion._duration_2_long.apply((java.time.Duration)o) : o;
            case PERIOD_YEAR_MONTH:
                return (v instanceof IntervalYearVector) ? Conversion._period_2_int.apply((Period)o) : o;
            case PERIOD_DURATION_MONTH_DAY_TIME:
                return (v instanceof IntervalMonthDayNanoVector) ? Conversion._translatePeriodDuration.apply((PeriodDuration)o) : o;
            case LIST:
                return (v instanceof ListVector) ? Conversion._translateList.apply((java.util.List<?>)o, (ListVector)v, (FieldType.ListType)t) : o;
            case MAP:
                return (v instanceof MapVector) ? Conversion._translateMap.apply((java.util.List<?>)o, (MapVector)v, (FieldType.MapType)t) : o;
            case STRUCT:
                return (v instanceof StructVector) ? Conversion._map_else_struct.apply((java.util.Map<String, ?>)o, (StructVector)v, (FieldType.StructType)t) : o;
            case NULL:
                return null;
        }
        return o;
    };
    @SuppressWarnings("UstableApiUsage")
    private static final fromConversion<Map<String, ?>, StructVector, FieldType.StructType, InternalRow> _translateStruct = (m, sv, t) -> {
        java.util.Map<String, FieldType> mt = t.getChildrenType();
        BiFunction<java.util.Map.Entry<String, ?>, Long, Object> setV = (e, i) -> {
            String k = e.getKey();
            Object v = e.getValue();
            return (v != null && mt.containsKey(k)) ? Conversion._translate.apply(mt.get(k), v, sv.getVectorById(i.intValue())) : null;
        };
        java.util.List<Object> nm = new java.util.ArrayList<>();
        Streams.mapWithIndex(m.entrySet().stream(), setV::apply).forEach(nm::add);
        return InternalRow.fromSeq(JavaConverters.asScalaBuffer(Arrays.asList(nm.toArray())).toSeq());
    };
    private static final fromConversion<Map<String, ?>, StructVector, FieldType.StructType, ArrayBasedMapData> _struct_2_map = (m, sv, t) -> {
        java.util.Map<String, FieldType> mt = t.getChildrenType();
        if (mt.size() == 1 && mt.containsKey("map") && mt.get("map").getTypeID() == FieldType.IDs.LIST && sv.getVectorById(0) instanceof ListVector) {
            ListVector lv = (ListVector)sv.getVectorById(0);
            FieldType.ListType lt = (FieldType.ListType)mt.get("map");
            if (lt.getChildType().getTypeID() == FieldType.IDs.STRUCT && lv.getDataVector() instanceof StructVector) {
                StructVector dv = (StructVector)lv.getDataVector();
                FieldType.StructType st = (FieldType.StructType)lt.getChildType();
                String[] childKeys = st.getChildrenType().keySet().toArray(new String[0]);
                if (childKeys.length == 2 && childKeys[0].equals("key") && childKeys[1].equals("value")) {
                    java.util.List<java.util.Map.Entry<String, ?>> kvs = new java.util.ArrayList<>(m.entrySet());
                    if (kvs.size() == 1 && kvs.get(0).getKey().equals("map") && kvs.get(0).getValue() instanceof java.util.List<?>) {
                        FieldType kt = st.getChildrenType().get("key");
                        FieldType vt = st.getChildrenType().get("value");
                        java.util.List<?> list = (java.util.List<?>)kvs.get(0).getValue();
                        java.util.List<Object> keys = new java.util.ArrayList<>();
                        java.util.List<Object> values = new java.util.ArrayList<>();
                        list.forEach(e -> {
                            java.util.Map<?, ?> mes = (java.util.Map<?, ?>)e;
                            mes.forEach((key, value) -> {
                                keys.add(Conversion._translate.apply(kt, key, dv.getVectorById(0)));
                                values.add(Conversion._translate.apply(vt, value, dv.getVectorById(1)));
                            });
                        });
                        return new ArrayBasedMapData(ArrayData.toArrayData(keys.toArray()), ArrayData.toArrayData(values.toArray()));
                    }
                }
            }
        }
        return null;
    };
    private static final fromConversion<Map<String, ?>, StructVector, FieldType.StructType, Object> _map_else_struct = (m, sv, t) -> Conversion._o1_else_o2.apply(Conversion._struct_2_map.apply(m, sv, t), Conversion._translateStruct.apply(m, sv, t));
    private static final fromConversion<List<?>, MapVector, FieldType.MapType, ArrayBasedMapData> _translateMap = (l, mv, mt) -> {
        java.util.List<Object> keys = new java.util.ArrayList<>();
        java.util.List<Object> values = new java.util.ArrayList<>();
        ValueVector kv = mv.getChildrenFromFields().get(0);
        ValueVector vv = mv.getChildrenFromFields().get(1);
        l.forEach(e -> {
            java.util.Map.Entry<?, ?> entry = (java.util.Map.Entry<?, ?>)e;
            keys.add(Conversion._translate.apply(mt.getKeyType(), entry.getKey(), kv));
            values.add(Conversion._translate.apply(mt.getValueType(), entry.getValue(), vv));
        });
        return new ArrayBasedMapData(ArrayData.toArrayData(keys.toArray()), ArrayData.toArrayData(values.toArray()));
    };
    private static final fromConversion<List<?>, ListVector, FieldType.ListType, ArrayData> _translateList = (l, v, t) -> ArrayData.toArrayData(l.stream().map(e -> Conversion._translate.apply(t.getChildType(), e, v.getDataVector())).toArray());
    private static final Function<Long, Long> _micros_2_nanos = (micros) -> Conversion._micros_2_millis.apply(micros) * 1000L;
    private static final Function<Long, Long> _micros_2_millis = DateTimeUtils::microsToMillis;
    private static final Function<Long, Long> _micros_2_secs = (micros) -> Conversion._micros_2_millis.apply(micros) / 1000L;
    private static final BiFunction<Integer, ZoneId, Long> _days_2_micros = (days, zone) -> DateTimeUtils.daysToMicros(days, ZoneId.systemDefault());
    private static final Function<Long, Long> _micros_2_epochNanos = (micros) -> {
        Instant t = DateTimeUtils.microsToLocalDateTime(micros).withYear(1970).withMonth(1).withDayOfMonth(1).atZone(ZoneId.systemDefault()).toInstant();
        return (long)t.toEpochMilli() * 1000000L + (long)t.getNano();
    };
    private static final Function<String, Long> _timestr_2_nanos = (ts) -> {
        Instant t = LocalDateTime.of(LocalDate.of(1970, 1, 1), LocalTime.parse(ts)).atZone(ZoneId.systemDefault()).toInstant();
        return (long)t.toEpochMilli() * 1000000L + (long)t.getNano();
    };
    private static final BiFunction<Object, Object, Object> _o1_else_o2 = (o1, o2) -> (o1 != null) ? o1 : o2;

    //the singleton instance
    private static Conversion _inst = null;
    /**
     * Get or create an instance of Vector
     * @return - the singleton instance of Vector
     */
    public static synchronized Conversion getOrCreate() {
        if (Conversion._inst == null) {
            Conversion._inst = new Conversion();
        }
        return Conversion._inst;
    }
}
