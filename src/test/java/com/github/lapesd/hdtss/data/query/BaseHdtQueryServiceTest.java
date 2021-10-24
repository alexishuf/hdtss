package com.github.lapesd.hdtss.data.query;

import com.github.lapesd.hdtss.TestUtils;
import com.github.lapesd.hdtss.model.FlowType;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.github.lapesd.hdtss.vocab.FOAF;
import com.github.lapesd.hdtss.vocab.XSD;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.scheduler.Schedulers;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.EX;
import static com.github.lapesd.hdtss.TestUtils.fixEquals;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public abstract class BaseHdtQueryServiceTest {
    public static record Implementation(@NonNull String name, @NonNull HdtQueryService service) {}

    protected abstract @NonNull List<Implementation> createImplementations();

    @SuppressWarnings("unchecked") static @NonNull Stream<Arguments> testQuery() {
        return Stream.of(
                // no results
                arguments("<"+EX+"Charlie> <"+FOAF.age+"> ?x", emptyList()),
                // negative ask query
                arguments("<"+EX+"Charlie> <"+FOAF.age+"> \"charlie\"", emptyList()),
                // positive ask query
                arguments("<"+EX+"Alice> <"+FOAF.name+"> \"Alice\"@en",
                          singletonList(emptyList())),
                // query two objects
                arguments("<"+EX+"Alice> <"+FOAF.name +"> ?x", asList(
                        singletonList("\"Alice\"@en"),
                        singletonList("\"Alícia\"@pt-BR")
                )),
                // query with two variables
                arguments("?x <"+FOAF.age+"> ?age", asList(
                        asList("<"+EX+"Alice>", "\"23\"^^<"+XSD.integer+">"),
                        asList("<"+EX+"Bob>", "\"25\"^^<"+XSD.integer+">")
                )),
                // query reflexive triple
                arguments("?x <"+FOAF.knows+"> ?x",
                        singletonList(singletonList("<"+EX+"Bob>"))),
                // all vars to query reflexive triple
                arguments("?x ?pred ?x",
                        singletonList(asList("<"+EX+"Bob>", "<"+FOAF.knows+">")))
        ).map(a -> {
            Object queryString = a.get()[0];
            var results = (List<List<String>>)a.get()[1];
            var expected = results.stream().map(row ->
                    new SolutionRow(row.stream().map(Term::new).toArray(Term[]::new))
            ).collect(toSet());
            return arguments(queryString, expected);
        });
    }

    @ParameterizedTest
    @MethodSource("testQuery")
    void testIterateQuery(@NonNull String queryString, @NonNull Set<SolutionRow> expected) {
        TriplePattern query = TestUtils.parseTriplePattern(queryString);
        for (Implementation impl : createImplementations()) {
            for (FlowType flowType : FlowType.values()) {
                QuerySolutions sols = impl.service.query(query, flowType);
                for (int i = 0, rounds = sols.isHot() ? 1 : 2; i < rounds; i++) {
                    var msg = format("flowType=%s, round=%d, impl=%s", flowType, i, impl.name);
                    Set<SolutionRow> actual = new HashSet<>();
                    sols.forEach(actual::add);
                    assertEquals(expected, fixEquals(actual), msg);
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("testQuery")
    void testListQuery(@NonNull String queryString, @NonNull Set<SolutionRow> expected) {
        TriplePattern query = TestUtils.parseTriplePattern(queryString);
        for (Implementation impl : createImplementations()) {
            for (FlowType flowType : FlowType.values()) {
                QuerySolutions sols = impl.service.query(query, flowType);
                for (int i = 0, rounds = sols.isHot() ? 1 : 2; i < rounds; i++) {
                    var msg = format("flowType=%s, round=%d, impl=%s", flowType, i, impl.name);
                    assertEquals(expected, new HashSet<>(fixEquals(sols.list())), msg);
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("testQuery")
    void testStreamQuery(@NonNull String queryString, @NonNull Set<SolutionRow> expected) {
        TriplePattern query = TestUtils.parseTriplePattern(queryString);
        for (Implementation impl : createImplementations()) {
            for (FlowType flowType : FlowType.values()) {
                QuerySolutions sols = impl.service.query(query, flowType);
                for (int i = 0, rounds = sols.isHot() ? 1 : 2; i < rounds; i++) {
                    var msg = format("flowType=%s, round=%d, impl=%s", flowType, i, impl.name);
                    var ac = sols.stream().map(TestUtils::fixEquals).collect(Collectors.toSet());
                    assertEquals(expected, ac, msg);
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("testQuery")
    void testSubscribeQuery(@NonNull String queryString, @NonNull Set<SolutionRow> expected){
        TriplePattern query = TestUtils.parseTriplePattern(queryString);
        for (Implementation impl : createImplementations()) {
            for (FlowType flowType : FlowType.values()) {
                QuerySolutions solutions = impl.service.query(query, flowType);
                for (int i = 0, rounds = solutions.isHot() ? 1 : 2; i < rounds; i++) {
                    var msg = format("flowType=%s, round=%d, impl=%s", flowType, i, impl.name);
                    var actual = solutions.flux()
                            .subscribeOn(Schedulers.single())
                            .collect(toSet()).block();
                    assertNotNull(actual);
                    assertEquals(expected, fixEquals(actual), msg);
                }
            }
        }
    }
}