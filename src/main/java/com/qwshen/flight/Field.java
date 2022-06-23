package com.qwshen.flight;

import org.apache.arrow.vector.types.pojo.Schema;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;

/**
 * Describes a field data-structure, such as the field name and date-type
 */
public class Field implements Serializable {
    //the name of the field
    private final String _name;
    //the data type
    private final FieldType _type;

    /**
     * Construct a Field
     * @param name - the name of the field
     * @param type - the data type of the field
     */
    public Field(String name, FieldType type) {
        this._name = name;
        this._type = type;
    }

    /**
     * Get the name of the field
     * @return - the name
     */
    public String getName() {
        return this._name;
    }

    /**
     * Get the datatype of the field
     * @return - the type
     */
    public FieldType getType() {
        return this._type;
    }

    /**
     * Get the hash-code of the field
     * @return - the value of the hash-code
     */
    public int hashCode() {
        return this._name.hashCode();
    }

    /**
     * Find the field-type of a field by name
     * @param fields - the field collection from which to search
     * @param name - the name of a field to be searched
     * @return - the field type matching the input name
     */
    public static FieldType find(Field[] fields, String name) {
        Optional<Field> fs = Arrays.stream(fields).filter(s -> s.getName().equalsIgnoreCase(name)).findFirst();
        if (!fs.isPresent()) {
            throw new RuntimeException("The field with " + name + " doesn't exist.");
        }
        return fs.get().getType();
    }

    /**
     * Extract all fields from the arrow-flight schema
     * @param schema - the arrow schema
     * @return - fields from the schema
     */
    public static Field[] from(Schema schema) {
        return schema.getFields().stream().map(f -> new Field(f.getName(), FieldType.fromArrow(f.getType(), f.getChildren()))).toArray(Field[]::new);
    }
}
