package ru.zudin.triclustering.model;

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
    private Text value;
    private EntityType type;
    private String description;

    protected Entity() {
        this(null, null);
    }

    public Entity(Text value, EntityType type) {
        this(value, type, "empty description");
    }

    public Entity(Text value, EntityType type, String description) {
        this.value = value;
        this.type = type;
        this.description = description;
    }

    public Text getValue() {
        return value;
    }

    public String getDescription() {
        return description;
    }

    public EntityType getType() {
        return type;
    }

    public void setValue(Text value) {
        this.value = value;
    }

    public void setType(EntityType type) {
        this.type = type;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    // Storage methods

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Entity entity = (Entity) o;
        return description.equals(entity.description) &&
                !(value != null ? !value.equals(entity.value) : entity.value != null) &&
                !(type != null ? !type.equals(entity.type) : entity.type != null);
    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        if (type != null) result = result * 31 + type.hashCode();
        result = 31 * result + description.hashCode();
        return result;
    }

    // Hadoop methods

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        value.write(dataOutput);
        new Text(type.name()).write(dataOutput);
        new Text(description).write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        value = new Text();
        value.readFields(dataInput);
        Text text = new Text();
        text.readFields(dataInput);
        type = EntityType.valueOf(text.toString());
        text.readFields(dataInput);
        description = text.toString();
    }
}
