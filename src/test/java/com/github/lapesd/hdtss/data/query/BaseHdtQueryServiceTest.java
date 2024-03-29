package com.github.lapesd.hdtss.data.query;

import com.github.lapesd.hdtss.TestUtils;
import com.github.lapesd.hdtss.model.FlowType;
import com.github.lapesd.hdtss.model.Row;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.vocab.FOAF;
import com.github.lapesd.hdtss.vocab.XSD;
import io.micronaut.context.ApplicationContext;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.scheduler.Schedulers;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestUtils.fixEquals;
import static com.github.lapesd.hdtss.TestVocab.EX;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public abstract class BaseHdtQueryServiceTest {
    public record Implementation(@NonNull String name,
                                 @NonNull HdtQueryService service,
                                 @NonNull ApplicationContext appCtx) implements  AutoCloseable {
        @Override public void close() { appCtx.close(); }
    }

    protected abstract @NonNull List<Implementation> createImplementations();

    @SuppressWarnings("unchecked") static @NonNull Stream<Arguments> testQuery() {
        return Stream.of(
                // no results
                arguments("<"+EX+"Charlie> <"+FOAF.age+"> ?x", List.of()),
                // negative ask query
                arguments("<"+EX+"Charlie> <"+FOAF.age+"> \"charlie\"", List.of()),
                // positive ask query
                arguments("<"+EX+"Alice> <"+FOAF.name+"> \"Alice\"@en",
                          List.of(List.of())),
                // query two objects
                arguments("<"+EX+"Alice> <"+FOAF.name +"> ?x", asList(
                        List.of("\"Alice\"@en"),
                        List.of("\"Alícia\"@pt-BR")
                )),
                // query with two variables
                arguments("?x <"+FOAF.age+"> ?age", asList(
                        asList("<"+EX+"Alice>", "\"23\"^^<"+XSD.integer+">"),
                        asList("<"+EX+"Bob>", "\"25\"^^<"+XSD.integer+">")
                )),
                // query reflexive triple
                arguments("?x <"+FOAF.knows+"> ?x",
                        List.of(List.of("<"+EX+"Bob>"))),
                // all vars to query reflexive triple
                arguments("?x ?pred ?x",
                        List.of(asList("<"+EX+"Bob>", "<"+FOAF.knows+">")))
        ).map(a -> {
            Object queryString = a.get()[0];
            var results = (List<List<String>>)a.get()[1];
            var expected = results.stream().map(row ->
                    new Row(row.stream().map(Term::new).toArray(Term[]::new))
            ).collect(toSet());
            return arguments(queryString, expected);
        });
    }

    @ParameterizedTest
    @MethodSource("testQuery")
    void testIterateQuery(@NonNull String queryString, @NonNull Set<Row> expected) {
        TriplePattern query = TestUtils.parseTriplePattern(queryString);
        for (Implementation impl : createImplementations()) {
            try (impl) {
                for (FlowType flowType : FlowType.values()) {
                    QuerySolutions sols = impl.service.query(query, flowType);
                    for (int i = 0, rounds = sols.isHot() ? 1 : 2; i < rounds; i++) {
                        var msg = format("flowType=%s, round=%d, impl=%s", flowType, i, impl.name);
                        Set<Row> actual = sols.toSet();
                        assertEquals(expected, fixEquals(actual), msg);
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("testQuery")
    void testListQuery(@NonNull String queryString, @NonNull Set<Row> expected) {
        TriplePattern query = TestUtils.parseTriplePattern(queryString);
        for (Implementation impl : createImplementations()) {
            try (impl) {
                for (FlowType flowType : FlowType.values()) {
                    QuerySolutions sols = impl.service.query(query, flowType);
                    for (int i = 0, rounds = sols.isHot() ? 1 : 2; i < rounds; i++) {
                        var msg = format("flowType=%s, round=%d, impl=%s", flowType, i, impl.name);
                        assertEquals(expected, new HashSet<>(fixEquals(sols.wrappedList())), msg);
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("testQuery")
    void testStreamQuery(@NonNull String queryString, @NonNull Set<Row> expected) {
        TriplePattern query = TestUtils.parseTriplePattern(queryString);
        for (Implementation impl : createImplementations()) {
            try (impl) {
                for (FlowType flowType : FlowType.values()) {
                    QuerySolutions sols = impl.service.query(query, flowType);
                    for (int i = 0, rounds = sols.isHot() ? 1 : 2; i < rounds; i++) {
                        var msg = format("flowType=%s, round=%d, impl=%s", flowType, i, impl.name);
                        var ac = sols.stream().map(TestUtils::fixEquals).map(Row::new).collect(toSet());
                        assertEquals(expected, ac, msg);
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("testQuery")
    void testSubscribeQuery(@NonNull String queryString, @NonNull Set<Row> expected){
        TriplePattern query = TestUtils.parseTriplePattern(queryString);
        for (Implementation impl : createImplementations()) {
            try (impl) {
                for (FlowType flowType : FlowType.values()) {
                    QuerySolutions solutions = impl.service.query(query, flowType);
                    for (int i = 0, rounds = solutions.isHot() ? 1 : 2; i < rounds; i++) {
                        var msg = format("flowType=%s, round=%d, impl=%s", flowType, i, impl.name);
                        var actual = solutions.flux()
                                .subscribeOn(Schedulers.single())
                                .map(Row::new).collect(toSet()).block();
                        assertNotNull(actual);
                        assertEquals(expected, fixEquals(actual), msg);
                    }
                }
            }
        }
    }
}