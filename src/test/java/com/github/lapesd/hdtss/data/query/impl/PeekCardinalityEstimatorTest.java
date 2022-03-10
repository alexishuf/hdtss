package com.github.lapesd.hdtss.data.query.impl;

import com.github.lapesd.hdtss.TempFile;
import com.github.lapesd.hdtss.TestUtils;
import com.github.lapesd.hdtss.data.query.HdtQueryService;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import io.micronaut.context.ApplicationContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.data.query.impl.PeekCardinalityEstimator.Lookup.ALWAYS;
import static com.github.lapesd.hdtss.data.query.impl.PeekCardinalityEstimator.Lookup.CACHED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class PeekCardinalityEstimatorTest {

    private static TempFile hdtFile;
    private ApplicationContext appCtx;
    private HdtQueryService queryService;

    @BeforeAll
    static void beforeAll() throws IOException {
        String path = "data/query/peek.hdt";
        hdtFile = new TempFile(".hdt").initFromResource(TempFile.class, path);
    }

    @AfterAll
    static void afterAll() throws IOException {
        if (hdtFile != null)
            hdtFile.close();
    }

    @BeforeEach
    void setUp() {
        appCtx = ApplicationContext.builder()
                .args("-hdt.location", hdtFile.getAbsolutePath())
                .start();
        queryService = appCtx.getBean(HdtQueryService.class);
    }

    @AfterEach
    void tearDown() {
        appCtx.close();
    }

    @SuppressWarnings("unused")  static Stream<Arguments> testEstimate() {
        return Stream.of(
                /* less frequent predicate wins on ?s <p> ?o */
        /*  1 */arguments("?s <:p2> ?o . ?x <:p1> ?y",   "?s <:p2> ?o . ?x <:p1> ?y", ALWAYS),
        /*  2 */arguments("?x <:p1> ?y . ?s <:p2> ?o",   "?s <:p2> ?o . ?x <:p1> ?y", ALWAYS),
                //reverse input
        /*  3 */arguments("?s <:p2> ?o . ?x <:p1> ?y",   "?s <:p2> ?o . ?x <:p1> ?y", CACHED),
        /*  4 */arguments("?x <:p1> ?y . ?s <:p2> ?o",   "?s <:p2> ?o . ?x <:p1> ?y", CACHED),

                /* check cardinality of SP? triple */
        /*  5 */arguments("<:S11> <:p3> ?o . <:S10> <:p3> ?o",
                          "<:S11> <:p3> ?o . <:S10> <:p3> ?o", ALWAYS),
        /*  6 */arguments("<:S10> <:p3> ?o . <:S11> <:p3> ?o", //reverse input
                          "<:S11> <:p3> ?o . <:S10> <:p3> ?o", ALWAYS),

                /* check cardinality of ?PO triple */
        /*  7 */arguments("?s <:p4> <:O11> . ?s <:p4> <:O10>",
                          "?s <:p4> <:O11> . ?s <:p4> <:O10>", ALWAYS),
        /*  8 */arguments("?s <:p4> <:O10> . ?s <:p4> <:O11>", // reverse input
                          "?s <:p4> <:O11> . ?s <:p4> <:O10>", ALWAYS),

                /* check cardinality of ?PO triple falling back to ?P? cardinality */
        /*  9 */arguments("?s <:p6> <:O13> . ?s <:p5> <:O12>",
                          "?s <:p6> <:O13> . ?s <:p5> <:O12>", ALWAYS),
        /* 10 */arguments("?s <:p6> <:O13> . ?s <:p5> <:O12>",
                          "?s <:p6> <:O13> . ?s <:p5> <:O12>", CACHED), // works bcs p6 < p5
                // reverse input order
        /* 11 */arguments("?s <:p5> <:O12> . ?s <:p6> <:O13>",
                          "?s <:p6> <:O13> . ?s <:p5> <:O12>", ALWAYS),
        /* 12 */arguments("?s <:p5> <:O12> . ?s <:p6> <:O13>",
                          "?s <:p6> <:O13> . ?s <:p5> <:O12>", CACHED)
        );
    }

    @ParameterizedTest @MethodSource
    void testEstimate(String inputString, String expectedString,
                      PeekCardinalityEstimator.Lookup lookup) {
        var input = parseTriplePatterns(inputString);
        var expected = parseTriplePatterns(expectedString);
        for (boolean prefetch : List.of(false, true)) {
            for (int i = 0, repetitions = prefetch ? 100 : 1; i < repetitions; i++) {
                var copy = new ArrayList<>(input);
                var estimator = new PeekCardinalityEstimator(queryService, prefetch, lookup);
                record W(TriplePattern tp, long estimate)  { }
                var weighed = input.stream().map(tp -> new W(tp, estimator.estimate(tp)))
                                   .sorted(Comparator.comparing(W::estimate)).toList();
                List<TriplePattern> actual = weighed.stream().map(W::tp).toList();
                String msg = "prefetch=" + prefetch + ", i=" + i;
                assertEquals(expected, actual, msg);
                assertEquals(copy, input, msg);
                estimator.abortPrefetch();
            }
        }
    }

    private List<TriplePattern> parseTriplePatterns(String inString) {
        return Arrays.stream(inString.replace("<:", "<http://example.org/").split(" +\\. +"))
                     .map(TestUtils::parseTriplePattern).toList();
    }

}