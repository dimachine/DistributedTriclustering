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
    private String description;

    protected Entity() {
        this(null);
    }

    public Entity(T value) {
        this(value, "empty description");
    }

    public Entity(T value, String description) {
        this.value = value;
        this.description = description;
    }

    public T getValue() {
        return value;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Entity entity = (Entity) o;
        return description.equals(entity.description) &&
                !(value != null ? !value.equals(entity.value) : entity.value != null);
    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + description.hashCode();
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeChars(value.getClass().getName());
        value.write(dataOutput);
        dataOutput.writeChars(description);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String cls = dataInput.readLine();
        try {
            value = (T) Class.forName(cls).newInstance();
            value.readFields(dataInput);
            description = dataInput.readLine();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}
