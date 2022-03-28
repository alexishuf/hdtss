package com.github.lapesd.hdtss.sparql.results.chunked;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.results.codecs.JSONCodec;
import com.github.lapesd.hdtss.utils.ByteArrayWriter;
import io.micronaut.http.MediaType;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes.RESULTS_JSON_TYPE;
import static java.nio.charset.StandardCharsets.UTF_8;

@Singleton
@Slf4j
public class JsonChunkedEncoder implements ChunkedEncoder {
    private static final @NonNull List<MediaType> mediaTypes = List.of(RESULTS_JSON_TYPE);

    @Override public @NonNull List<@NonNull MediaType> mediaTypes() {
        return mediaTypes;
    }

    @Override
    public @NonNull ChunkedPublisher encode(@NonNull MediaType mediaType,
                                            @NonNull QuerySolutions solutions) {
        return new JsonChunkedPublisher(solutions);
    }

    private static final class JsonChunkedPublisher extends SimpleChunkedPublisher {
        /* --- --- --- helper fields --- --- --- */
        private final List<@NonNull String> varNames = solutions.varNames();
        private static final @NonNull AtomicReference<@Nullable ByteArrayWriter> FREE_WRITER
                = new AtomicReference<>();
        private final ByteArrayWriter writer;

        /* --- --- --- state --- --- --- */
        private boolean firstBinding = true;

        /* --- --- --- constant pre-encoded segments --- --- --- */
        private static final byte @NonNull [] ASK_PROLOGUE = "{\"head\":{\"vars\":[]},\"boolean\":".getBytes(UTF_8);
        private static final byte @NonNull [] ASK_TRUE = "true}".getBytes(UTF_8);
        private static final byte @NonNull [] ASK_FALSE = "false}".getBytes(UTF_8);
        private static final byte @NonNull [] ROWS_EPILOGUE = "]}}".getBytes(UTF_8);
        private static final byte @NonNull [] HEAD_VARS = "{\"head\":{\"vars\":[".getBytes(UTF_8);
        private static final byte @NonNull [] EMPTY_BINDINGS = "]},\"results\":{\"bindings\":[]}}".getBytes(UTF_8);
        private static final byte @NonNull [] BINDINGS = "]},\"results\":{\"bindings\":[".getBytes(UTF_8);

        /* --- --- --- implementation --- --- --- */

        public JsonChunkedPublisher(@NonNull QuerySolutions solutions) {
            super(solutions);
            ByteArrayWriter observed = FREE_WRITER.get();
            if (observed != null && FREE_WRITER.compareAndSet(observed, null))
                this.writer = observed;
            else
                this.writer = new ByteArrayWriter();
        }

        @Override protected byte[]  askPrologue() {return  ASK_PROLOGUE;}
        @Override protected byte[] rowsEpilogue() {return ROWS_EPILOGUE;}
        @Override protected byte[] askBodyAndPrologue(boolean v) {return v ? ASK_TRUE : ASK_FALSE;}
        @Override protected void   release() {FREE_WRITER.compareAndSet(null, writer);}

        @Override protected byte[] rowsPrologue() {
            writer.reset().append(HEAD_VARS);
            if (varNames.isEmpty()) {
                writer.append(EMPTY_BINDINGS);
            } else {
                writer.append('"').append(varNames.get(0)).append('"');
                for (int i = 1, size = varNames.size(); i < size; i++)
                    writer.append(',').append('"').append(varNames.get(i)).append('"');
                writer.append(BINDINGS);
            }
            return writer.toByteArray();
        }

        @Override protected byte[] rowBytes(@Nullable Term @NonNull[] row) {
            writer.reset();
            if (firstBinding)
                firstBinding = false;
            else
                writer.append(',');
            JSONCodec.writeRowToBytes(varNames, row, writer);
            return writer.toByteArray();
        }
    }
}
