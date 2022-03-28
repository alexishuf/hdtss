package com.github.lapesd.hdtss.sparql.results.chunked;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import com.github.lapesd.hdtss.sparql.results.codecs.XMLEncoder;
import com.github.lapesd.hdtss.utils.ByteArrayWriter;
import io.micronaut.http.MediaType;
import jakarta.inject.Singleton;
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;

@Singleton
public class XMLChunkedEncoder implements ChunkedEncoder {
    private static final @NonNull List<@NonNull MediaType> mediaTypes
            = List.of(SparqlMediaTypes.RESULTS_XML_TYPE);



    @Override public @NonNull List<@NonNull MediaType> mediaTypes() {
        return mediaTypes;
    }

    @Override
    public @NonNull ChunkedPublisher encode(@NonNull MediaType mediaType,
                                            @NonNull QuerySolutions solutions) {
        return new XMLChunkedPublisher(solutions);
    }

    private static final class XMLChunkedPublisher extends SimpleChunkedPublisher {
        /* --- --- --- helper fields --- --- --- */
        private final @NonNull List<@NonNull String> names = solutions.varNames();
        private static final @NonNull AtomicReference<@Nullable ByteArrayWriter> FREE_WRITER
                = new AtomicReference<>();
        private final ByteArrayWriter writer;

        /* --- --- --- constant pre-encoded segments --- --- --- */
        private static final byte @NonNull[] BEGIN_HEAD =
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><sparql xmlns=\"http://www.w3.org/2005/sparql-results#\"><head>".getBytes(UTF_8);
        private static final byte @NonNull [] ASK_PROLOGUE;
        private static final byte @NonNull [] ASK_TRUE  =  "true</boolean></sparql>".getBytes(UTF_8);
        private static final byte @NonNull [] ASK_FALSE = "false</boolean></sparql>".getBytes(UTF_8);
        private static final byte @NonNull [] END_EPILOGUE = "</results></sparql>".getBytes(UTF_8);
        private static final byte @NonNull [] OPEN_VAR = "<variable name=\"".getBytes(UTF_8);
        private static final byte @NonNull [] CLOSE_VAR = "\"/>".getBytes(UTF_8);
        private static final byte @NonNull [] CLOSE_HEAD = "</head><results>".getBytes(UTF_8);

        static {
            byte[] suffix = "</head><boolean>".getBytes(UTF_8);
            byte[] copy = Arrays.copyOf(BEGIN_HEAD, BEGIN_HEAD.length + suffix.length);
            System.arraycopy(suffix, 0, copy, BEGIN_HEAD.length, suffix.length);
            ASK_PROLOGUE = copy;
        }

        /* --- --- --- implementation --- --- --- */

        public XMLChunkedPublisher(@NonNull QuerySolutions solutions) {
            super(solutions);
            ByteArrayWriter observed = FREE_WRITER.get();
            if (observed != null && FREE_WRITER.compareAndSet(observed, null))
                this.writer = observed;
            else
                this.writer = new ByteArrayWriter();
        }

        @Override protected byte[]  askPrologue() {return ASK_PROLOGUE;}
        @Override protected byte[] rowsEpilogue() {return END_EPILOGUE;}
        @Override protected byte[] askBodyAndPrologue(boolean v) {return v ? ASK_TRUE : ASK_FALSE;}
        @Override protected void   release() {FREE_WRITER.compareAndSet(null, writer);}

        @Override protected byte[] rowsPrologue() {
            writer.reset().append(BEGIN_HEAD);
            for (String name : names)
                writer.append(OPEN_VAR).append(name).append(CLOSE_VAR);
            return writer.append(CLOSE_HEAD).toByteArray();
        }

        @SneakyThrows @Override protected byte[] rowBytes(@Nullable Term @NonNull[] row) {
            XMLEncoder.writeRow(names, row, writer.reset());
            return writer.toByteArray();
        }
    }
}
