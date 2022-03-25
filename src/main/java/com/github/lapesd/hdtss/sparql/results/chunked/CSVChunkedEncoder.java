package com.github.lapesd.hdtss.sparql.results.chunked;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import com.github.lapesd.hdtss.sparql.results.codecs.CSVEncoder;
import com.github.lapesd.hdtss.utils.ByteArrayWriter;
import io.micronaut.http.MediaType;
import jakarta.inject.Singleton;
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

@Singleton
public class CSVChunkedEncoder implements ChunkedEncoder {
    private static final @NonNull List<@NonNull MediaType> MEDIA_TYPES =
            List.of(SparqlMediaTypes.RESULTS_CSV_TYPE);

    @Override public @NonNull List<@NonNull MediaType> mediaTypes() {
        return MEDIA_TYPES;
    }

    @Override
    public @NonNull ChunkedPublisher encode(@NonNull MediaType mediaType, @NonNull QuerySolutions solutions) {
        return new CSVChunkedPublisher(mediaType, solutions);
    }

    private static final class CSVChunkedPublisher extends AbstractSVChunkedPublisher {
        public CSVChunkedPublisher(@NonNull MediaType mt, @NonNull QuerySolutions solutions) {
            super(mt, solutions, "", ",", "\r\n");
        }

        @SneakyThrows @Override protected void writeTerm(ByteArrayWriter writer, Term term) {
            CSVEncoder.writeTerm(writer, term);
        }
    }
}
