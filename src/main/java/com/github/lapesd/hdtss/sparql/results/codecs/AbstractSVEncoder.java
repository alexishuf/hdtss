package com.github.lapesd.hdtss.sparql.results.codecs;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import io.micronaut.core.io.buffer.ByteBuffer;
import io.micronaut.core.io.buffer.ByteBufferFactory;
import io.micronaut.core.type.Argument;
import io.micronaut.http.MediaType;
import io.micronaut.http.codec.CodecConfiguration;
import io.micronaut.http.codec.CodecException;
import io.micronaut.http.codec.MediaTypeCodec;
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractSVEncoder implements MediaTypeCodec {
    private final char sep;
    private final @NonNull String varSymbol;
    private final @NonNull String eol;
    private final @NonNull List<@NonNull MediaType> mediaTypes;

    protected AbstractSVEncoder(char sep, @NonNull String varSymbol, @NonNull String eol,
                                @NonNull MediaType mediaType,
                                @Nullable CodecConfiguration configuration) {
        this.sep = sep;
        this.varSymbol = varSymbol;
        this.eol = eol;
        mediaTypes = new ArrayList<>(1);
        mediaTypes.add(mediaType);
        if (configuration != null)
            mediaTypes.addAll(configuration.getAdditionalTypes());
    }

    @SneakyThrows protected void encode(@NonNull Term term, @NonNull ByteBuffer<?> bb) {
        OutputStream os = new OutputStream() {
            @Override public void write(int b) { bb.write((byte) (b & 0xFF)); }
            @Override public void write(byte @NonNull [] b, int off, int len) { bb.write(b, off, len); }
        };
        try (OutputStreamWriter w = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
            encode(term, w);
        }
    }
    abstract protected void encode(@NonNull Term term, @NonNull Writer writer) throws IOException;

    @Override public boolean supportsType(Class<?> type) {
        return QuerySolutions.class.isAssignableFrom(type);
    }

    @Override public @NonNull Collection<@NonNull MediaType> getMediaTypes() {
        return mediaTypes;
    }

    @Override public <T> T decode(Argument<T> type, InputStream inputStream) throws CodecException {
        throw new CodecException(this+"does not support decode()");
    }

    @Override public <T> void encode(T object, OutputStream outputStream) throws CodecException {
        if (!QuerySolutions.class.isAssignableFrom(object.getClass()))
            throw new CodecException(this+" only encodes QuerySolutions instances");
        QuerySolutions solutions = (QuerySolutions) object;
        try (Writer w = new OutputStreamWriter(outputStream, UTF_8)) {
            int i = 0;
            for (String name : solutions.varNames())
                (i++ != 0 ? w.append(sep) : w).append(varSymbol).append(name);
            w.append(eol);
            for (@Nullable Term @NonNull[] row : solutions) {
                for (int j = 0; j < row.length; j++) {
                    if (j > 0) w.append(sep);
                    Term term = row[j];
                    if (term != null)
                        encode(term, w);
                }
                w.append(eol);
            }
        } catch (IOException e) {
            throw new CodecException(e.getMessage(), e);
        }
    }

    @Override public <T> byte[] encode(T object) throws CodecException {
        ByteArrayOutputStream bOS = new ByteArrayOutputStream();
        encode(object, bOS);
        return bOS.toByteArray();
    }

    @Override
    public <T, B> ByteBuffer<B> encode(T object, ByteBufferFactory<?, B> allocator) throws CodecException {
        if (!QuerySolutions.class.isAssignableFrom(object.getClass()))
            throw new CodecException(this+" only encodes QuerySolutions instances");
        QuerySolutions solutions = (QuerySolutions) object;
        ByteBuffer<B> b = allocator.buffer();
        byte[] varSymbolBytes = varSymbol.getBytes(UTF_8), eolBytes = eol.getBytes(UTF_8);
        int i = 0;
        for (String name : solutions.varNames())
            (i++ == 0 ? b : b.write((byte) sep)).write(varSymbolBytes).write(name.getBytes(UTF_8));
        b.write(eolBytes);
        for (@Nullable Term @NonNull[] row : solutions) {
            boolean first = true;
            for (Term term : row) {
                if (term != null)
                    encode(term, first ? b : b.write((byte) sep));
                first = false;
            }
            b.write(eolBytes);
        }
        return b;
    }
}
