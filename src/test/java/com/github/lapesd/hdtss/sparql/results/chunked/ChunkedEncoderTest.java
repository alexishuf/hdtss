package com.github.lapesd.hdtss.sparql.results.chunked;

import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.results.CodecTestBase;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import io.micronaut.http.MediaType;
import io.micronaut.http.codec.MediaTypeCodecRegistry;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes.RESULTS_CSV_TYPE;
import static com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes.RESULTS_TSV_TYPE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;


@MicronautTest(startApplication = false)
@Tag("fast")
class ChunkedEncoderTest {
    @Inject
    List<ChunkedEncoder> encoders;

    @Inject
    MediaTypeCodecRegistry codecRegistry;

    @Test
    void selfTestHasInjection() {
        assertFalse(encoders.isEmpty());
        assertFalse(codecRegistry.getCodecs().isEmpty());
    }

    @Test
    void selfTestAllTypesCovered() {
        var typesWithoutChunkedEncoder = SparqlMediaTypes.RESULT_TYPES.stream()
                .filter(t -> encoders.stream().noneMatch(e -> e.mediaTypes().contains(t)))
                .collect(toList());
        var typesWithoutCodec = SparqlMediaTypes.RESULT_TYPES.stream()
                .filter(t -> codecRegistry.findCodec(t).isEmpty()).collect(toList());
        var typesWithoutCodecForQuerySolutions = SparqlMediaTypes.RESULT_TYPES.stream()
                .filter(t -> codecRegistry.findCodec(t, QuerySolutions.class).isEmpty())
                .collect(toList());
        assertEquals(List.of(), typesWithoutChunkedEncoder);
        assertEquals(List.of(), typesWithoutCodec);
        assertEquals(List.of(), typesWithoutCodecForQuerySolutions);
    }

    static Stream<Arguments> testSameSerialization() {
        return Stream.of(
                arguments(CodecTestBase.ASK_FALSE),
                arguments(CodecTestBase.ASK_TRUE),
                arguments(CodecTestBase.ONE_ONE),
                arguments(CodecTestBase.ONE_ROW),
                arguments(CodecTestBase.TWO_ROWS)
        );
    }

    @ParameterizedTest @MethodSource
    public void testSameSerialization(@NonNull BatchQuerySolutions solutions) {
        Set<MediaType> svTypes = Set.of(RESULTS_TSV_TYPE, RESULTS_CSV_TYPE);
        for (ChunkedEncoder cEncoder : encoders) {
            for (MediaType mediaType : cEncoder.mediaTypes()) {

                var msgPrefix = "cEncoder="+cEncoder+", mediaType="+mediaType;
                var codec = codecRegistry.findCodec(mediaType, QuerySolutions.class).orElse(null);
                if (codec == null) {
                    continue;
                }
                var expected = new String(codec.encode(solutions), UTF_8);
                List<String> chunks = cEncoder.encode(mediaType, solutions)
                                              .map(b -> new String(b, UTF_8))
                                              .collectList().block();
                assertNotNull(chunks, msgPrefix);
                int min = svTypes.contains(mediaType) && !solutions.askResult() ? 1 : 2 ;
                assertTrue(chunks.size() >= min,
                           msgPrefix+", size()="+chunks.size()+", expected >= "+min);
                assertEquals(expected, String.join("", chunks), msgPrefix);
            }
        }
    }
}