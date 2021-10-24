package com.github.lapesd.hdtss.sparql.results.chunked;

import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import io.micronaut.http.MediaType;
import org.checkerframework.checker.nullness.qual.NonNull;
import reactor.core.publisher.Flux;

import java.util.List;

public interface ChunkedEncoder {
    /**
     * Which {@link MediaType}s this encoder can encode
     *
     * @return a non-null list of distinct non-null {@link MediaType}s without parameters
     */
    @NonNull List<@NonNull MediaType> mediaTypes();

    /**
     * Creates a {@link Flux} over chunks of the encoding of the given solutions.
     *
     * Each chunk will fall into one of the following types:
     * <ol>
     *     <li>A header chunk: contains all the initial encoding including the whole header section</li>
     *     <li>An optional introduction chunk with encoding boilerplate separating the header section from the results section</li>
     *     <li>Zero or more chunks containing at least the serialization of at least one {@link SolutionRow}</li>
     *     <li>An optional final chunk with encoding boilerplate (e.g., closing braces and tags)</li>
     * </ol>
     *
     * @param mediaType the serialization format for the results.
     * @param solutions the query solutions to be encoded.
     * @return a non-empty Flux of non-null and non-empty String chunks, as described above.
     */
    @NonNull Flux<byte[]> encode(@NonNull MediaType mediaType, @NonNull QuerySolutions solutions);
}
