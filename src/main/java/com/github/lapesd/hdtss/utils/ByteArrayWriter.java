package com.github.lapesd.hdtss.utils;

import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class ByteArrayWriter extends OutputStreamWriter {
    private static final int DEFAULT_CAPACITY = 512;
    private final @NonNull ByteArrayOutputStream bOS;

    public ByteArrayWriter() {
        this(new ByteArrayOutputStream(DEFAULT_CAPACITY), StandardCharsets.UTF_8);
    }
    public ByteArrayWriter(int capacity) {
        this(new ByteArrayOutputStream(capacity), StandardCharsets.UTF_8);
    }
    public ByteArrayWriter(int capacity, @NonNull Charset charset) {
        this(new ByteArrayOutputStream(capacity), charset);
    }
    public ByteArrayWriter(@NonNull Charset charset) {
        this(new ByteArrayOutputStream(DEFAULT_CAPACITY), charset);
    }
    protected ByteArrayWriter(@NonNull ByteArrayOutputStream out, @NonNull Charset charset)  {
        super(out, charset);
        this.bOS = out;
    }

    /**
     * Erase all bytes written from the internal buffer
     *
     * @return this same {@link ByteArrayWriter} instance
     */
    public @NonNull ByteArrayWriter reset() {
        bOS.reset();
        return this;
    }

    /**
     * Return a byte[] with a copy of all data written to this {@link Writer}.
     *
     * @return a new byte[] with all written data (its length is the same as the written data).
     */
    @SneakyThrows public byte[] toByteArray() {
        flush();
        return bOS.toByteArray();
    }

    @SneakyThrows @Override public ByteArrayWriter append(CharSequence csq, int start, int end)  {
        super.append(csq, start, end);
        return this;
    }

    @SneakyThrows @Override public ByteArrayWriter append(CharSequence csq)  {
        super.append(csq);
        return this;
    }

    @SneakyThrows @Override public ByteArrayWriter append(char c)  {
        super.append(c);
        return this;
    }
}
