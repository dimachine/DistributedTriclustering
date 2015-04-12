package ru.zudin.triclustering.model;

/**
 * @author Sergey Zudin
 * @since 12.04.15.
 */
public class UnsupportedSizeExeption extends UnsupportedOperationException {
    public UnsupportedSizeExeption(String message) {
        super(message);
    }
}
