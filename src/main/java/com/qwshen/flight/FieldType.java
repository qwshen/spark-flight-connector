package com.qwshen.flight;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import java.io.Serializable;
import java.util.Arrays;
import java.util.function.Function;
import org.apache.spark.sql.types.YearMonthIntervalType;
import org.apache.spark.sql.types.DayTimeIntervalType;

/**
 * Describes the data-type of a Field
 */
public class FieldType implements Serializable {
    /**
     * The ID of each type
     */
    public enum IDs {
        NULL,                           //Null object
        BOOLEAN,                        //Boolean
        BYTE,                           //Byte
        CHAR,                           //Character
        SHORT,                          //Short
        INT,                            //Integer
        LONG,                           //Long
        BIGINT,                         //Big Integer
        FLOAT,                          //Float
        DOUBLE,                         //Double
        DECIMAL,                        //BidDecimal
        VARCHAR,                        //String
        BYTES,                          //byte[]
        DATE,                           //LocalDate
        TIME,                           //LocalTime
        TIMESTAMP,                      //TimeStamp
        PERIOD_YEAR_MONTH,              //Period - year with month
        DURATION_DAY_TIME,              //Period - day with time
        PERIOD_DURATION_MONTH_DAY_TIME, //Period - month with day
        LIST,                           //List<?>
        MAP,                            //Map<String, ?>
        STRUCT                          //Struct
    }

    /**
     * Decimal Type
     */
    public static class DecimalType extends FieldType {
        private final int _precision;
        private final int _scale;

        public DecimalType(int precision, int scale) {
            super(IDs.DECIMAL);
            this._precision = precision;
            this._scale = scale;
        }

        public int getPrecision() {
            return this._precision;
        }
        public int getScale() {
            return this._scale;
        }
    }

    /**
     * Binary Type
     */
    public static class BinaryType extends FieldType {
        private final int _byteWidth;

        public BinaryType(int byteWidth) {
            super(IDs.BYTES);
            this._byteWidth = byteWidth;
        }

        public int getByteWidth() {
            return this._byteWidth;
        }
    }

    /**
     * List Type
     */
    public static class ListType extends FieldType {
        private final int _length;
        private final FieldType _childType;

        public ListType(int length, FieldType childType) {
            super(IDs.LIST);
            this._length = length;
            this._childType = childType;
        }
        public ListType(FieldType childType) {
            //dynamic size of list
            this(-1, childType);
        }

        public int getLength() {
            return this._length;
        }
        public FieldType getChildType() {
            return this._childType;
        }
    }

    /**
     * May Type
     */
    public static class MapType extends FieldType {
        private final FieldType _keyType;
        private final FieldType _valueType;

        public MapType(FieldType keyType, FieldType valueType) {
            super(IDs.MAP);
            this._keyType = keyType;
            this._valueType = valueType;
        }

        public FieldType getKeyType() {
            return this._keyType;
        }
        public FieldType getValueType() {
            return this._valueType;
        }
    }

    /**
     * Struct Type
     */
    public static class StructType extends FieldType {
        private final java.util.Map<String, FieldType> _childrenType;

        public StructType(java.util.Map<String, FieldType> childrenType) {
            super(IDs.STRUCT);
            this._childrenType = childrenType;
        }

        public java.util.Map<String, FieldType> getChildrenType() {
            return this._childrenType;
        }
    }

    /**
     * Union Type
     */
    public static class UnionType extends StructType {
        public UnionType(java.util.Map<String, FieldType> childrenType) {
            super(childrenType);
        }
    }

    //the type value
    private final IDs _typeId;

    /**
     * Construct a FieldType object
     * @param typeId - the id of the type
     */
    public FieldType(IDs typeId) {
        this._typeId = typeId;
    }

    /**
     * Get the Type ID
     * @return - the type ID
     */
    public IDs getTypeID() {
        return this._typeId;
    }

    /**
     * Convert an arrow-type to field-type
     * @param at - the arrow type
     * @param children - any children of the arrow-type
     * @return - the converted field-type
     */
    public static FieldType fromArrow(ArrowType at, java.util.List<org.apache.arrow.vector.types.pojo.Field> children) {
        switch(at.getTypeID()) {
            case Int:
                ArrowType.Int it = (ArrowType.Int)at;
                switch (it.getBitWidth()) {
                    case 8:
                        return new FieldType(it.getIsSigned() ? IDs.BYTE : IDs.SHORT);
                    case 16:
                        return new FieldType(it.getIsSigned() ? IDs.SHORT : IDs.INT);
                    case 64:
                        return new FieldType(it.getIsSigned() ? IDs.LONG : IDs.BIGINT);
                    case 32:
                    default:
                        return new FieldType(it.getIsSigned() ? IDs.INT : IDs.LONG);
                }
            case Utf8:
            case LargeUtf8:
                return new FieldType(IDs.VARCHAR);
            case Decimal:
                ArrowType.Decimal d = (ArrowType.Decimal)at;
                return new FieldType.DecimalType(d.getPrecision(), d.getScale());
            case Date:
                return new FieldType(IDs.DATE);
            case Time:
                return new FieldType(IDs.TIME);
            case Timestamp:
                return new FieldType(IDs.TIMESTAMP);
            case FloatingPoint:
                switch(((ArrowType.FloatingPoint)at).getPrecision()) {
                    case HALF:
                    case SINGLE:
                        return new FieldType(IDs.FLOAT);
                    case DOUBLE:
                    default:
                        return new FieldType(IDs.DOUBLE);
                }
            case Interval:
                switch (((ArrowType.Interval)at).getUnit()) {
                    case YEAR_MONTH:
                        return new FieldType(IDs.PERIOD_YEAR_MONTH);
                    case DAY_TIME:
                        return new FieldType(IDs.DURATION_DAY_TIME);
                    case MONTH_DAY_NANO:
                    default:
                        return new FieldType(IDs.PERIOD_DURATION_MONTH_DAY_TIME);
                }
            case Duration:
                return new FieldType(IDs.PERIOD_DURATION_MONTH_DAY_TIME);
            case Bool:
                return new FieldType(IDs.BOOLEAN);
            case Struct:
            case Union:
                java.util.Map<String, FieldType> scType = new java.util.LinkedHashMap<>();
                if (children != null) {
                    children.forEach(c -> scType.put(c.getName(), FieldType.fromArrow(c.getType(), c.getChildren())));
                }
                return (at.getTypeID() == ArrowType.ArrowTypeID.Struct) ? new StructType(scType) : new UnionType(scType);
            case Map:
                FieldType keyType = null, valueType = null;
                if (children != null && children.size() > 1) {
                    keyType = FieldType.fromArrow(children.get(0).getType(), children.get(0).getChildren());
                    valueType = FieldType.fromArrow(children.get(1).getType(), children.get(1).getChildren());
                }
                return new MapType(keyType, valueType);
            case List:
            case LargeList:
            case FixedSizeList:
                FieldType lcType = (children != null && children.size() > 0) ? FieldType.fromArrow(children.get(0).getType(), children.get(0).getChildren()) : null;
                return (at.getTypeID() == ArrowType.ArrowTypeID.FixedSizeList) ? new ListType(((ArrowType.FixedSizeList)at).getListSize(), lcType) : new ListType(lcType);
            case Binary:
            case LargeBinary:
                return new BinaryType(-1);
            case FixedSizeBinary:
                return new BinaryType(((ArrowType.FixedSizeBinary)at).getByteWidth());
            case Null:
            case NONE:
            default:
                return new FieldType(IDs.NULL);
        }
    }

    //Convert to Spark DecimalType
    private static final Function<FieldType, org.apache.spark.sql.types.DecimalType> toDecimalType = t -> {
        FieldType.DecimalType dt = (FieldType.DecimalType)t;
        return new org.apache.spark.sql.types.DecimalType(dt.getPrecision(), dt.getScale());
    };
    private static final Function<StructType, org.apache.spark.sql.types.DataType> toStructType = t -> {
        org.apache.spark.sql.types.MapType mt = null;
        java.util.List<java.util.Map.Entry<String, FieldType>> entries = new java.util.ArrayList<>(t.getChildrenType().entrySet());
        if (entries.size() == 1 && entries.get(0).getKey().equals("map") && entries.get(0).getValue().getTypeID() == FieldType.IDs.LIST) {
            FieldType.ListType lt = (FieldType.ListType)entries.get(0).getValue();
            if (lt.getChildType().getTypeID() == FieldType.IDs.STRUCT) {
                FieldType.StructType st = (FieldType.StructType)lt.getChildType();
                if (st.getTypeID() == FieldType.IDs.STRUCT) {
                    java.util.List<java.util.Map.Entry<String, FieldType>> children = new java.util.ArrayList<>(st.getChildrenType().entrySet());
                    if (children.size() == 2 && children.get(0).getKey().equals("key") && children.get(1).getKey().equals("value")) {
                        mt = new org.apache.spark.sql.types.MapType(FieldType.toSpark(children.get(0).getValue()), FieldType.toSpark(children.get(1).getValue()), true);
                    }
                }
            }
        }
        return (mt != null) ? mt : new org.apache.spark.sql.types.StructType(t.getChildrenType().entrySet().stream().map(e -> new org.apache.spark.sql.types.StructField(e.getKey(), FieldType.toSpark(e.getValue()), true, Metadata.empty())).toArray(org.apache.spark.sql.types.StructField[]::new));
    };
    //convert to Spark ListType
    private static final Function<ListType, org.apache.spark.sql.types.ArrayType> toListType = t -> org.apache.spark.sql.types.ArrayType.apply(FieldType.toSpark(t.getChildType()));
    //convert to Spark MapType
    private static final Function<MapType, org.apache.spark.sql.types.MapType> toMapType = t -> org.apache.spark.sql.types.MapType.apply(FieldType.toSpark(t.getKeyType()), FieldType.toSpark(t.getValueType()));

    public static org.apache.spark.sql.types.DataType toSpark(FieldType ft) {
        switch (ft.getTypeID()) {
            case INT:
                return DataTypes.IntegerType;
            case CHAR:
            case VARCHAR:
            case TIME:
                return DataTypes.StringType;
            case LONG:
            case BIGINT:
                return DataTypes.LongType;
            case FLOAT:
                return DataTypes.FloatType;
            case DOUBLE:
                return DataTypes.DoubleType;
            case DECIMAL:
                return toDecimalType.apply(ft);
            case DATE:
                return DataTypes.DateType;
            case TIMESTAMP:
                return DataTypes.TimestampType;
            case BOOLEAN:
                return DataTypes.BooleanType;
            case BYTE:
                return DataTypes.ByteType;
            case SHORT:
                return DataTypes.ShortType;
            case BYTES:
                return DataTypes.BinaryType;
            case PERIOD_YEAR_MONTH:
                return new YearMonthIntervalType(YearMonthIntervalType.YEAR(), YearMonthIntervalType.MONTH());
            case DURATION_DAY_TIME:
                return new DayTimeIntervalType(DayTimeIntervalType.DAY(), DayTimeIntervalType.SECOND());
            case PERIOD_DURATION_MONTH_DAY_TIME:
                return new org.apache.spark.sql.types.StructType(Arrays.stream(new org.apache.spark.sql.types.StructField[] {
                    new org.apache.spark.sql.types.StructField("period", new YearMonthIntervalType(YearMonthIntervalType.YEAR(), YearMonthIntervalType.MONTH()), true, org.apache.spark.sql.types.Metadata.empty()),
                    new org.apache.spark.sql.types.StructField("duration", new DayTimeIntervalType(DayTimeIntervalType.DAY(), DayTimeIntervalType.SECOND()), true, org.apache.spark.sql.types.Metadata.empty())
                }).toArray(org.apache.spark.sql.types.StructField[]::new));
            case LIST:
                return toListType.apply((ListType)ft);
            case MAP:
                return toMapType.apply((MapType)ft);
            case STRUCT:
                return toStructType.apply((StructType)ft);
            case NULL:
            default:
                return org.apache.spark.sql.types.DataTypes.NullType;
        }
    }
}
