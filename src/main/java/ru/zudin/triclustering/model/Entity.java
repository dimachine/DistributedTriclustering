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
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Entity && ((Entity) obj).value.equals(value);
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
            e.printStackTrace();
        }
    }
}
