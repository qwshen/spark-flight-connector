package com.qwshen.flight;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Describes the partition behavior. If the data type of by-column is numberic or date-time, and lower-bound and upper-bound are specified, the
 * partitioning step is calculated upon partition size. Otherwise, hash-partitioning will be used.
 */
public class PartitionBehavior implements Serializable {
    /**
     * The internal Bound for organizing predicates
     */
    private static class Bound implements Serializable {
        private final double _lower;
        private final double _upper;

        public Bound(double lower, double upper) {
            this._lower = lower;
            this._upper = upper;
        }

        public String toLongPredicate(String name) {
            return toPredicate(name, Long.toString((long)this._lower), Long.toString((long)this._upper));
        }

        public String toDoublePredicate(String name) {
            return toPredicate(name, Double.toString(this._lower), Double.toString(this._upper));
        }

        public String toDateTimePredicate(String name, DateTimeFormatter dtFormat) {
            return toPredicate(name, String.format("'%s'", dtFormat.print(new DateTime((long)this._lower))), String.format("'%s'", dtFormat.print(new DateTime((long)this._upper))));
        }

        private String toPredicate(String name, String lower, String upper) {
            return !lower.equalsIgnoreCase(upper) ? String.format("%s <= %s and %s < %s", lower, name, name, upper) : String.format("%s = %s", name, lower);
        }
    }

    //the name of hash-func in remote flight service
    private final String _hashFunc;
    //the name of partition-by column
    private final String _byColumn;
    //the number of partitions
    private final int _size;
    //the lower bound
    private final String _lowerBound;
    //the upper bound
    private final String _upperBound;

    //explicit predicates
    private final String[] _predicates;

    /**
     * Construct a partition behavior
     * @param hashFunc - the name of the hash-func
     * @param byColumn - the column used for partitioning
     * @param size - the partition size
     * @param lowerBound - the lower bound used for partitioning
     * @param upperBound - the upper bound used for partitioning
     * @param predicates - the explicit predicates
     */
    public PartitionBehavior(String hashFunc, String byColumn, int size, String lowerBound, String upperBound, String[] predicates) {
        this._hashFunc = hashFunc;
        this._byColumn = byColumn;
        this._size = size;
        this._lowerBound = lowerBound;
        this._upperBound = upperBound;

        this._predicates = predicates;
    }

    /**
     * Get the by-column
     * @return - the name of the column used for partitioning
     */
    public String getByColumn() {
        return this._byColumn;
    }

    /**
     * Get the predicates
     * @return - the collection of predicates for partitioning
     */
    public String[] getPredicates() {
        return this._predicates;
    }

    /**
     * Calculate the predicates upon by-column, size, lower-bound & upper-bound
     * @param dataFields - The fields from the select-list. The column for partitioning may or may not on the select-list.
     * @return - the predicates which partitions the rows
     */
    public String[] calculatePredicates(StructField[] dataFields) {
        String[] predicates = null;
        if (this._lowerBound != null && this._lowerBound.length() > 0 && this._upperBound != null && this._upperBound.length() > 0 && dataFields != null) {
            StructField partitionColumn = Arrays.stream(dataFields).filter(field -> field.name().equalsIgnoreCase(this._byColumn)).findFirst().orElse(null);
            if (partitionColumn != null) {
                DataType dataType = partitionColumn.dataType();
                if (dataType.equals(DataTypes.ByteType) || dataType.equals(DataTypes.ShortType) || dataType.equals(DataTypes.IntegerType) || dataType.equals(DataTypes.LongType)) {
                    predicates = probeLongPredicates().orElse(probeDoublePredicates().orElse(null));
                } else if (dataType.equals(DataTypes.FloatType) || dataType.equals(DataTypes.DoubleType) || dataType instanceof DecimalType) {
                    predicates = probeDoublePredicates().orElse(null);
                } else if (dataType.equals(DataTypes.DateType) || dataType.equals(DataTypes.TimestampType)) {
                    predicates = probeDateTimePredicates().orElse(null);
                }
            }
        }
        if (predicates == null) {
            //by default, hash-partitioning is applied
            Function<Integer, String> hashPredicate = (idx) -> String.format("(%s(%s) %% %d + %d) %% %d = %d", this._hashFunc, this._byColumn, this._size, this._size, this._size, idx);
            predicates = IntStream.range(0, this._size).mapToObj(hashPredicate::apply).toArray(String[]::new);
        }
        return predicates;
    }

    //probe Long predicates
    private Optional<String[]> probeLongPredicates() {
        try {
            long lower = Long.parseLong(this._lowerBound.replace(",", ""));
            long upper = Long.parseLong(this._upperBound.replace(",", ""));
            double step = (double)(upper - lower) / (double)this._size;
            return Optional.of(IntStream.range(0, this._size).mapToObj(i -> new Bound(lower + i * step, lower + (i + 1) * step)).map(b -> b.toLongPredicate(this._byColumn)).toArray(String[]::new));
        } catch (Exception e) {
            return Optional.empty();
        }
    }
    //probe Double predicates
    private Optional<String[]> probeDoublePredicates() {
        try {
            double lower = Double.parseDouble(this._lowerBound.replace(",", ""));
            double upper = Double.parseDouble(this._upperBound.replace(",", ""));
            double step = (upper - lower) / (double)this._size;
            return Optional.of(IntStream.range(0, this._size).mapToObj(i -> new Bound(lower + i * step, lower + (i + 1) * step)).map(b -> b.toDoublePredicate(this._byColumn)).toArray(String[]::new));
        } catch (Exception e) {
            return Optional.empty();
        }
    }
    //probe DateTime predicates
    private Optional<String[]> probeDateTimePredicates() {
        String[] dtFormats = new String[] {
            "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss", "MM/dd/yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ", "yyyy.MM.dd HH:mm:ss", "yyyyMMdd HH:mm:ss",
            "yyyy-MM-dd", "yyyy/MM/d", "MM/dd/yyyy", "dd/MM/yyyy", "yyyyMMdd"
        };
        Optional<String[]> predicates = Optional.empty();
        for (int i = 0; i < dtFormats.length && !predicates.isPresent(); i++) {
            predicates = tryDateTimePredicates(DateTimeFormat.forPattern(dtFormats[i]));
        }
        return predicates;
    }
    private Optional<String[]> tryDateTimePredicates(DateTimeFormatter dtFormat) {
        try {
            long lower = DateTime.parse(this._lowerBound, dtFormat).getMillis();
            long upper = DateTime.parse(this._upperBound, dtFormat).getMillis();
            double step = (double)(upper - lower) / (double)this._size;
            return Optional.of(IntStream.range(0, this._size).mapToObj(i -> new Bound(lower + i * step, lower + (i + 1) * step)).map(b -> b.toDateTimePredicate(this._byColumn, dtFormat)).toArray(String[]::new));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /**
     * Check if the behavior is defined for partitioning
     * @return - true when partitioning is defined
     */
    public Boolean enabled() {
        return ((this._byColumn != null && this._byColumn.length() > 0) || (this._predicates != null && this._predicates.length > 0));
    }

    /**
     * Flg to indicate whether pre-defined predicates have been given
     * @return - true if partition predicates provided
     */
    public Boolean predicateDefined() {
        return (this._predicates != null && this._predicates.length > 0);
    }
}
