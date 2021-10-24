package com.github.lapesd.hdtss.sparql.results.chunked;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import com.github.lapesd.hdtss.sparql.results.codecs.TSVCodec;
import com.github.lapesd.hdtss.utils.ByteArrayWriter;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Singleton
public class TSVChunkedEncoder extends AbstractSVChunkedEncoder {
    public TSVChunkedEncoder() {
        super(SparqlMediaTypes.RESULTS_TSV_TYPE, '\t', "\n", "?");
    }

    @Override protected void writeTerm(@Nullable Term term, @NonNull ByteArrayWriter builder) {
        if (term != null)
            builder.append(TSVCodec.sanitize(term));
    }
}
