package com.github.lapesd.hdtss.utils;

import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.*;
import java.util.Arrays;

public final class ByteArrayWriter extends Writer {
    static final int DEFAULT_CAPACITY = 1024;
    static final int ENCODER_CAPACITY = DEFAULT_CAPACITY;
    private final @NonNull CharsetEncoder encoder;
    private final Charset charset;
    private final boolean isUtf8;
    private final char @NonNull [] ca = new char[ENCODER_CAPACITY];
    private final @NonNull CharBuffer cb = CharBuffer.wrap(ca);
    private final @NonNull ByteBuffer bb = ByteBuffer.allocate(ENCODER_CAPACITY);
    private byte @NonNull [] bytes;
    private int size = 0;


    public ByteArrayWriter() {
        this(DEFAULT_CAPACITY, StandardCharsets.UTF_8);
    }

    public ByteArrayWriter(@NonNull Charset charset) {
        this(DEFAULT_CAPACITY, charset);
    }
    public ByteArrayWriter(int capacity, @NonNull Charset charset) {
        this.charset = charset;
        isUtf8 = StandardCharsets.UTF_8.equals(charset);
        encoder = charset.newEncoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        bytes = new byte[capacity];
    }

    /**
     * Erase all bytes written from the internal buffer
     *
     * @return this same {@link ByteArrayWriter} instance
     */
    public @NonNull ByteArrayWriter reset() {
        size = 0;
        return this;
    }

    /**
     * Return a byte[] with a copy of all data written to this {@link Writer}.
     *
     * @return a new byte[] with all written data (its length is the same as the written data).
     */
    @SneakyThrows public byte[] toByteArray() {
        return Arrays.copyOf(bytes, size);
    }

    @Override public void write(char @NonNull [] chars, int off, int len) throws IOException {
        CharBuffer cb = CharBuffer.wrap(chars, off, len);
        CoderResult cr;
        do {
            cr = encoder.encode(cb, bb, false);
            flushBytes();
        } while (cr.isOverflow());
        if (cr.isUnderflow() && cb.hasRemaining())
            throw new IllegalArgumentException("Orphan surrogates are not supported");
    }

    @Override public void flush() { /* nothing */ }

    @Override public void close()  { /* nothing */ }

    @Override @SneakyThrows public ByteArrayWriter append(CharSequence csq) {
        return append(csq, 0, csq.length());
    }

    @Override @SneakyThrows public ByteArrayWriter append(CharSequence csq, int start, int end) {
        for (int i = start, batchEnd; i < end; i = batchEnd) {
            batchEnd = i + Math.min(end-i, ca.length);
            csq.toString().getChars(i, batchEnd, ca, cb.position());
            cb.limit(cb.position()+batchEnd-i);
            CoderResult cr;
            do {
                cr = encoder.encode(cb, bb, false);
                flushBytes();
            } while (cr.isOverflow());
            if (cr.isUnderflow()) {
                if (cb.hasRemaining()) {
                    char leftover = cb.get();
                    cb.clear().put(leftover);
                } else {
                    cb.clear();
                }
            }
        }
        return this;
    }

    private void ensureCapacity(int additional) {
        int required = size + additional;
        if (required >= bytes.length)
            bytes = Arrays.copyOf(bytes, Math.min(required+64, bytes.length<<1));
    }

    private void flushBytes() {
        bb.flip();
        int remaining = bb.remaining();
        ensureCapacity(remaining);
        bb.get(bytes, size, remaining);
        size += remaining;
        bb.clear();
    }

    public ByteArrayWriter revert(int bytes) {
        if (bytes > size)
            throw new IndexOutOfBoundsException("bytes > size: "+bytes+" > "+size);
        size -= bytes;
        return this;
    }

    @SneakyThrows public ByteArrayWriter append(char c)  {
        return append((int)c);
    }

    @SneakyThrows public ByteArrayWriter append(int b)  {
        if (isUtf8)
            bytes[size++] = (byte)b;
        else
            append(Character.toString(b));
        return this;
    }

    @SneakyThrows public ByteArrayWriter append(byte[] encodedBytes, int off, int len) {
        ensureCapacity(len);
        System.arraycopy(encodedBytes, off, bytes, size, len);
        size += len;
        return this;
    }

    @SneakyThrows public ByteArrayWriter append(byte[] encodedBytes) {
        return append(encodedBytes, 0, encodedBytes.length);
    }

    @Override public String toString() {
        return getClass().getSimpleName()+"("+new String(toByteArray(), charset)+")";
    }
}
