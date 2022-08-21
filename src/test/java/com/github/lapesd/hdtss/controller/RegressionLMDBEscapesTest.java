package com.github.lapesd.hdtss.controller;

import com.github.lapesd.hdtss.TempFile;
import com.github.lapesd.hdtss.TestConfigPermutations;
import com.github.lapesd.hdtss.TestVocab;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.vocab.FOAF;
import com.github.lapesd.hdtss.vocab.XSD;
import org.apache.jena.vocabulary.OWL2;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.model.Term.fromEscaped;
import static com.github.lapesd.hdtss.model.Term.fromURI;
import static java.util.Arrays.asList;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class RegressionLMDBEscapesTest extends ControllerTestBase {
    private static TestConfigPermutations permutations;

    @BeforeAll
    static void beforeAll() throws IOException {
        var path = "data/lmdb-S5-subset.hdt";
        permutations = new TestConfigPermutations(TempFile.class, path);
    }

    @AfterAll
    static void afterAll() throws IOException {
        if (permutations != null)
            permutations.close();
    }

    @Override protected @NonNull TestConfigPermutations getPermutations() {
        return permutations;
    }

    static Stream<Arguments> test() {
        String prolog = String.format("""
                PREFIX : <%s>
                PREFIX xsd: <%s>
                PREFIX foaf: <%s>
                PREFIX owl: <%s>
                SELECT * WHERE {
                """, TestVocab.EX, XSD.NS, FOAF.NS, OWL2.NS);
        return Stream.of(
                // get genre of a film
    /* 1 */     arguments("<http://data.linkedmdb.org/resource/film/15188> <http://data.linkedmdb.org/resource/movie/genre> ?genre",
                          List.of(List.of(fromURI("http://data.linkedmdb.org/resource/film_genre/4")))),
    /* 2 */     // get title of movie
                arguments("<http://data.linkedmdb.org/resource/film/15188> <http://purl.org/dc/terms/title> ?title",
                         List.of(List.of(fromEscaped("\"L'ultimo bacio\"")))),
                // get objects with percent-encodings
    /* 3 */     arguments("<http://data.linkedmdb.org/resource/film/15188> owl:sameAs ?o",
                        asList(List.of(fromURI("http://mpii.de/yago/resource/L%27ultimo_bacio")),
                               List.of(fromURI("http://dbpedia.org/resource/L%27ultimo_bacio")))),
                // give percent-escaped URI in query
    /* 4 */     arguments("?s owl:sameAs <http://dbpedia.org/resource/L%27ultimo_bacio>",
                          List.of(List.of(fromURI("http://data.linkedmdb.org/resource/film/15188"))))
        ).map(a -> arguments(prolog+a.get()[0]+"}", a.get()[1]));
    }

    @ParameterizedTest @MethodSource
    public void test(@NonNull String query, @NonNull List<List<Term>> expected) {
        doTest(expected, c -> c.get(query));
    }
}
