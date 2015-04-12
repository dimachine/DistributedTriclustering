package ru.zudin.triclustering.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Sergey Zudin
 * @since 02.04.15.
 */
public class Entity<T extends Writable> implements Writable {
    private T value;
    private EntityType type;
    private String description;

    protected Entity() {
        this(null, null);
    }

    public Entity(T value, EntityType type) {
        this(value, type, "empty description");
    }

    public Entity(T value, EntityType type, String description) {
        this.value = value;
        this.type = type;
        this.description = description;
    }

    public T getValue() {
        return value;
    }

    public String getDescription() {
        return description;
    }

    public EntityType getType() {
        return type;
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
        dataOutput.writeChars(value.getClass().getName());
        value.write(dataOutput);
        dataOutput.writeChars(type.name());
        dataOutput.writeChars(description);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String cls = dataInput.readLine();
        try {
            value = (T) Class.forName(cls).newInstance();
            value.readFields(dataInput);
            type = EntityType.valueOf(dataInput.readLine());
            description = dataInput.readLine();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}
