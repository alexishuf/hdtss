package com.github.lapesd.hdtss.sparql;

import com.github.lapesd.hdtss.TestVocab;
import com.github.lapesd.hdtss.model.nodes.*;
import com.github.lapesd.hdtss.vocab.FOAF;
import com.github.lapesd.hdtss.vocab.RDF;
import com.github.lapesd.hdtss.vocab.RDFS;
import com.github.lapesd.hdtss.vocab.XSD;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.*;
import static java.util.Arrays.asList;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public abstract class SparqlParserTestBase {

    protected abstract @NonNull SparqlParser createParser();

    static Stream<Arguments> testParse() {
        String prolog = "PREFIX foaf: <"+FOAF.NS+">\n" +
                "PREFIX : <"+TestVocab.EX+">\n" +
                "PREFIX rdf: <"+RDF.NS+">\n" +
                "PREFIX rdfs: <"+RDFS.NS+">\n" +
                "PREFIX xsd: <"+XSD.NS+">\n";
        return Stream.of(
    /*  1 */    arguments(prolog+"SELECT DISTINCT ?x WHERE {?x foaf:knows ?y.} LIMIT 5",
                          new Limit(5, new Distinct(new Project(List.of("x"),
                                  new TriplePattern(x, knowsTerm, y))))),
    /*  2 */    arguments(prolog+"SELECT * WHERE { { { ?x foaf:knows ?y } } }",
                          new TriplePattern(x, knowsTerm, y)),
    /*  3 */    arguments(prolog+"SELECT * WHERE { { { ?x foaf:knows ?y . } . }. }",
                          new TriplePattern(x, knowsTerm, y)),
    /*  4 */    arguments(prolog+"SELECT ?y ?x WHERE { ?x foaf:knows ?y }",
                          new Project(asList("y", "x"), new TriplePattern(x, knowsTerm, y))),
    /*  5 */    arguments(prolog+"SELECT ?x WHERE { {?x foaf:knows :Alice} UNION { ?x foaf:knows :Bob } }",
                          new Union(new TriplePattern(x, knowsTerm, Alice),
                                    new TriplePattern(x, knowsTerm, Bob))),
    /*  6 */    arguments(prolog+"SELECT * WHERE { ?x foaf:age ?y FILTER(?y < 25) }",
                          new Filter(new TriplePattern(x, ageTerm, y), "?y < 25")),
    /*  7 */    arguments(prolog+"SELECT * WHERE { ?x foaf:knows ?y; foaf:age ?w. FILTER(?w > 23) }",
                          new Filter(new Join(new TriplePattern(x, knowsTerm, y),
                                              new TriplePattern(x, ageTerm, w)),
                                     "?w > 23")),
    /*  8 */    arguments(prolog+"SELECT * WHERE {\n" +
                          "  ?x foaf:age ?y FILTER(?y > 23).\n" +
                          "  ?x foaf:knows ?z FILTER NOT EXISTS { ?z foaf:age ?w }\n" +
                          "}",
                          new NotExists(new Filter(new Join(new TriplePattern(x, ageTerm, y),
                                                            new TriplePattern(x, knowsTerm, z)),
                                                   "?y > 23"),
                                        new TriplePattern(z, ageTerm, w))),
    /*  9 */    arguments(prolog+"ASK {?x ?y ?z}", new Ask(new TriplePattern(x, y, z))),
    /* 10 */    arguments(prolog+"ASK {\n" +
                          "  {?x foaf:mbox ?y} UNION {?x foaf:age ?y FILTER(?y > 23)}\n"+
                          "}",
                          new Ask(new Union(
                                  new TriplePattern(x, mboxTerm, y),
                                  new Filter(new TriplePattern(x, ageTerm, y),
                                             "?y > 23"))))
        );
    }

    @ParameterizedTest @MethodSource
    void testParse(@NonNull String string, @NonNull Op expected) {
        Op actual = createParser().parse(string);
        String msg = "trees differ:\nExpected:" + expected + "\n  Actual:" + actual + "\n";
        Assertions.assertTrue(actual.deepEquals(expected), msg);
        Assertions.assertTrue(expected.deepEquals(actual), msg);
    }

}