package com.github.lapesd.hdtss.sparql.results.chunked;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.utils.ByteArrayWriter;
import io.micronaut.http.MediaType;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractSVChunkedPublisher extends SimpleChunkedPublisher {
    private static final byte @NonNull [] EMPTY = new byte[0];
    private static final @NonNull AtomicReference<@Nullable ByteArrayWriter> FREE_WRITER
            = new AtomicReference<>();
    private final byte @NonNull [] varSymbol, sep, eol;
    private final ByteArrayWriter writer;

    public AbstractSVChunkedPublisher(@NonNull MediaType mediaType,
                                      @NonNull QuerySolutions solutions,
                                      @NonNull String varSymbol,
                                      @NonNull String sep, @NonNull String eol) {
        super(solutions);
        Charset cs = mediaType.getCharset().orElse(UTF_8);
        this.varSymbol = varSymbol.getBytes(cs);
        this.sep = sep.getBytes(cs);
        this.eol = eol.getBytes(cs);
        var observed = FREE_WRITER.get();
        if (observed != null && FREE_WRITER.compareAndSet(observed, null))
            this.writer = observed;
        else
            this.writer = new ByteArrayWriter();
    }

    @Override protected byte[]  askPrologue() { return eol; }
    @Override protected byte[] rowsEpilogue() { return EMPTY; }
    @Override protected byte[] askBodyAndPrologue(boolean result) { return result ? eol : EMPTY; }
    @Override protected void   release() { FREE_WRITER.compareAndSet(null, writer); }

    @Override protected byte[] rowsPrologue() {
        List<@NonNull String> names = solutions.varNames();
        if (names.isEmpty())
            return eol;
        writer.reset();
        for (String name : names)
            writer.append(varSymbol).append(name).append(sep);
        return writer.revert(sep.length).append(eol).toByteArray();
    }

    @Override protected byte[] rowBytes(@Nullable Term @NonNull[] row) {
        if (row.length == 0)
            return eol;
        writer.reset();
        for (Term term : row) {
            if (term != null)
                writeTerm(writer, term);
            writer.append(sep);
        }
        return writer.revert(sep.length).append(eol).toByteArray();
    }

    protected abstract void writeTerm(ByteArrayWriter writer, Term term);
}
