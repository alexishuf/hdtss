package com.github.lapesd.hdtss.sparql.results.chunked;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import com.github.lapesd.hdtss.sparql.results.codecs.CSVEncoder;
import com.github.lapesd.hdtss.utils.ByteArrayWriter;
import jakarta.inject.Singleton;
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Singleton
public class CSVChunkedEncoder extends AbstractSVChunkedEncoder {
    public CSVChunkedEncoder() {
        super(SparqlMediaTypes.RESULTS_CSV_TYPE, ',', "\r\n", "");
    }

    @Override @SneakyThrows protected void writeTerm(@Nullable Term term, @NonNull ByteArrayWriter builder) {
        if (term != null)
            CSVEncoder.writeTerm(builder, term);
    }
}
