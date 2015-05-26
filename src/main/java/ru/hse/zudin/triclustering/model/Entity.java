package ru.hse.zudin.triclustering.model;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Sergey Zudin
 * @since 02.04.15.
 */
public class Entity implements Writable {
    private String value;
    private EntityType type;

    protected Entity() {
        this(null, null);
    }

    public Entity(String value, EntityType type) {
        this.value = value;
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public EntityType getType() {
        return type;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setType(EntityType type) {
        this.type = type;
    }

    // Storage methods

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Entity entity = (Entity) o;

        return value.equals(entity.value) && type == entity.type;
    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        if (type != null) result = result * 31 + type.hashCode();
        return result;
    }

    // Hadoop methods

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        new Text(value).write(dataOutput);
        new Text(type.name()).write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        Text text = new Text();
        text.readFields(dataInput);
        value = text.toString();
        text = new Text();
        text.readFields(dataInput);
        type = EntityType.valueOf(text.toString());
    }
}
