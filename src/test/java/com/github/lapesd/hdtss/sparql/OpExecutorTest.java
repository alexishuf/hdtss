package com.github.lapesd.hdtss.sparql;

import com.github.lapesd.hdtss.TempFile;
import com.github.lapesd.hdtss.TestUtils;
import com.github.lapesd.hdtss.data.query.HdtQueryService;
import com.github.lapesd.hdtss.model.Row;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Project;
import com.github.lapesd.hdtss.model.nodes.*;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.sparql.impl.IdentityExecutor;
import com.github.lapesd.hdtss.sparql.impl.ask.AskFluxExecutor;
import com.github.lapesd.hdtss.sparql.impl.ask.AskItExecutor;
import com.github.lapesd.hdtss.sparql.impl.assign.JenaAssignFluxExecutor;
import com.github.lapesd.hdtss.sparql.impl.assign.JenaAssignItExecutor;
import com.github.lapesd.hdtss.sparql.impl.distinct.DistinctFluxExecutor;
import com.github.lapesd.hdtss.sparql.impl.distinct.DistinctItExecutor;
import com.github.lapesd.hdtss.sparql.impl.exists.ExistsFluxExecutor;
import com.github.lapesd.hdtss.sparql.impl.exists.ExistsItExecutor;
import com.github.lapesd.hdtss.sparql.impl.filter.JenaFilterFluxExecutor;
import com.github.lapesd.hdtss.sparql.impl.filter.JenaFilterItExecutor;
import com.github.lapesd.hdtss.sparql.impl.join.BindJoinFluxExecutor;
import com.github.lapesd.hdtss.sparql.impl.join.BindJoinItExecutor;
import com.github.lapesd.hdtss.sparql.impl.join.BindLeftJoinFluxExecutor;
import com.github.lapesd.hdtss.sparql.impl.join.BindLeftJoinItExecutor;
import com.github.lapesd.hdtss.sparql.impl.limit.LimitFluxExecutor;
import com.github.lapesd.hdtss.sparql.impl.limit.LimitItExecutor;
import com.github.lapesd.hdtss.sparql.impl.minus.MinusFluxExecutor;
import com.github.lapesd.hdtss.sparql.impl.minus.MinusItExecutor;
import com.github.lapesd.hdtss.sparql.impl.offset.OffsetFluxExecutor;
import com.github.lapesd.hdtss.sparql.impl.offset.OffsetItExecutor;
import com.github.lapesd.hdtss.sparql.impl.project.ProjectFluxExecutor;
import com.github.lapesd.hdtss.sparql.impl.project.ProjectItExecutor;
import com.github.lapesd.hdtss.sparql.impl.union.UnionFluxExecutor;
import com.github.lapesd.hdtss.sparql.impl.union.UnionItExecutor;
import com.github.lapesd.hdtss.sparql.impl.values.ValuesFluxExecutor;
import com.github.lapesd.hdtss.sparql.impl.values.ValuesItExecutor;
import com.github.lapesd.hdtss.vocab.RDF;
import com.github.lapesd.hdtss.vocab.XSD;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Named;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestUtils.fixEquals;
import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.*;
import static com.github.lapesd.hdtss.vocab.RDF.PropertyTerm;
import static com.github.lapesd.hdtss.vocab.RDF.typeTerm;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class OpExecutorTest {
    private static final List<Object> flowChoices = asList("REACTIVE", "ITERATOR");
    private static TempFile hdtFile;
    private static Map<String, List<Object>> sharedPropertyChoices;

    @BeforeAll
    static void beforeAll() throws IOException {
        String path = "data/query/foaf-graph.hdt";
        hdtFile = new TempFile(".hdt").initFromResource(TempFile.class, path);
        sharedPropertyChoices = Map.of(
                "sparql.hdt.flow", asList("REACTIVE", "ITERATOR"),
                "hdt.location", List.of(hdtFile.getAbsolutePath()),
                "hdt.estimator", asList("PATTERN", "PEEK")
        );
    }

    @AfterAll
    static void afterAll() throws IOException {
        if (hdtFile != null)
            hdtFile.close();
    }

    private @NonNull Iterable<ApplicationContext>
    applicationContexts(@NonNull String operator,
                        @NonNull Map<String, List<Object>> propertyChoices) {
        var opChoices = Map.of("sparql." + operator + ".flow", flowChoices);
        var mapsList = asList(sharedPropertyChoices, opChoices, propertyChoices);
        return TestUtils.listApplicationContext(mapsList);
    }

    private void testInContexts(@NonNull Op op, @NonNull Collection<@NonNull List<Term>> expected) {
        testInContexts(op, expected, Map.of());
    }

    private String opConfigName(Op op) {
        return opConfigName(op.type());
    }
    private String opConfigName(Op.Type type) {
        String lower = type.name().toLowerCase();
        return (lower.contains("_")) ? lower.split("_")[1] : lower;
    }

    private void testInContexts(@NonNull Op op, @NonNull Collection<@NonNull List<Term>> expected,
                                @NonNull Map<String, List<Object>> propertyChoices) {
        String opConfigName = opConfigName(op);
        for (ApplicationContext ctx : applicationContexts(opConfigName, propertyChoices)) {
            try (ctx) {
                var actual = ctx.getBean(OpExecutorDispatcher.class).execute(op)
                                .stream().map(Arrays::asList).collect(toList());
                assertEquals(new HashSet<>(expected), new HashSet<>(fixEquals(actual)));
                assertEquals(expected.size(), actual.size());
            }
        }
    }

    private ApplicationContext createFlowSelectionContext(Op.Type type, String flow) {
        return ApplicationContext.builder().args(
                "-hdt.location=" + hdtFile.getAbsolutePath(),
                "-hdt.estimator=NONE",
                "-sparql." + opConfigName(type) + ".flow="+flow.toUpperCase()
        ).start();
    }

    private @NonNull OpExecutor findExecutorFor(Op.Type op, ApplicationContext ctx) {
        OpExecutor single = null;
        for (OpExecutor ex : ctx.getBeansOfType(OpExecutor.class)) {
            if (ex.supportedTypes().contains(op)) {
                Named named = ex.getClass().getAnnotation(Named.class);
                assertNotNull(named, "Bean "+ex+" not annotated with @Named");
                if (named.value().toLowerCase().equals(op.name().toLowerCase().replace("_", ""))) {
                    String msg = "More than one candidate executor for " + op + ". old=" + single +
                            ", new candidate=" + ex;
                    assertNull(single, msg);
                    single = ex;
                }
            }
        }
        assertNotNull(single, "No executor for "+op);
        return single;
    }

    @Test
    void testFlowSelection() {
        //this will index the temp HDT file, subsequent calls will reuse the index
        try (var ctx = createFlowSelectionContext(Op.Type.ASK, "ITERATOR")) {
            ctx.getBean(HdtQueryService.class);
        }

        record Params(Op.Type op, Class<?> expectedIterator, Class<?> expectedFlux) {}
        List<Params> paramsList = List.of(
                new Params(Op.Type.ASK,        AskItExecutor.class,           AskFluxExecutor.class),
                new Params(Op.Type.ASSIGN,     JenaAssignItExecutor.class,    JenaAssignFluxExecutor.class),
                new Params(Op.Type.DISTINCT,   DistinctItExecutor.class,      DistinctFluxExecutor.class),
                new Params(Op.Type.EXISTS,     ExistsItExecutor.class,        ExistsFluxExecutor.class),
                new Params(Op.Type.FILTER,     JenaFilterItExecutor.class,    JenaFilterFluxExecutor.class),
                new Params(Op.Type.IDENTITY,   IdentityExecutor.class,        IdentityExecutor.class),
                new Params(Op.Type.JOIN,       BindJoinItExecutor.class,      BindJoinFluxExecutor.class),
                new Params(Op.Type.LEFT_JOIN,  BindLeftJoinItExecutor.class,  BindLeftJoinFluxExecutor.class),
                new Params(Op.Type.LIMIT,      LimitItExecutor.class,         LimitFluxExecutor.class),
                new Params(Op.Type.MINUS,      MinusItExecutor.class,         MinusFluxExecutor.class),
                new Params(Op.Type.OFFSET,     OffsetItExecutor.class,        OffsetFluxExecutor.class),
                new Params(Op.Type.PROJECT,    ProjectItExecutor.class,       ProjectFluxExecutor.class),
                new Params(Op.Type.UNION,      UnionItExecutor.class,         UnionFluxExecutor.class),
                new Params(Op.Type.VALUES,     ValuesItExecutor.class,        ValuesFluxExecutor.class)
        );
        paramsList.parallelStream().forEach(p -> {
            try (ApplicationContext ctx = createFlowSelectionContext(p.op, "ITERATOR")) {
                var actual = findExecutorFor(p.op, ctx).getClass();
                assertTrue(p.expectedIterator.isAssignableFrom(actual),
                           "Expected "+p.expectedIterator+", got "+actual);
            }
            try (ApplicationContext ctx = createFlowSelectionContext(p.op, "REACTIVE")) {
                var actual = findExecutorFor(p.op, ctx).getClass();
                assertTrue(p.expectedFlux.isAssignableFrom(actual),
                           "Expected "+p.expectedFlux+", got "+actual);
            }
        });
    }

    @SuppressWarnings("unused") static @NonNull Stream<Arguments> testProjection() {
        return Stream.of(
                arguments(new Project(List.of("x"), new TriplePattern(Alice, knowsTerm, x)),
                          List.of(List.of(Bob))),
                arguments(new Project(List.of("x"), new TriplePattern(x, knowsTerm, y)),
                          asList(List.of(Alice), List.of(Bob), List.of(Bob), List.of(Charlie))),
                arguments(new Project(List.of("y"), new TriplePattern(x, knowsTerm, y)),
                          asList(List.of(Bob), List.of(Alice), List.of(Bob), List.of(Alice))),
                arguments(new Project(List.of("y"), new TriplePattern(x, ageTerm, y)),
                          asList(List.of(i23), List.of(i25))),
                arguments(new Project(List.of("y", "x"), new TriplePattern(x, ageTerm, y)),
                          asList(List.of(i23, Alice), List.of(i25, Bob)))
        );
    }

    @ParameterizedTest @MethodSource
    void testProjection(@NonNull Project in, @NonNull Collection<List<Term>> expected) {
        testInContexts(in, expected);
    }

    @Test
    void testIdentity() {
        testInContexts(IdentityNode.INSTANCE, List.of(List.of()));
    }

    @SuppressWarnings("unused") static @NonNull Stream<@NonNull Arguments> testDistinct() {
        return Stream.of(
                arguments(new TriplePattern(Alice, nameTerm, x),
                          List.of(List.of(AliceEN), List.of(Alicia))),
                arguments(new TriplePattern(Bob, knowsTerm, x),
                          List.of(List.of(Alice), List.of(Bob))),
                arguments(new TriplePattern(x, knowsTerm, y),
                          asList(asList(Alice, Bob), asList(Bob, Alice),
                                 asList(Bob, Bob), asList(Charlie, Alice))),
                arguments(new Project(List.of("x"), new TriplePattern(x, knowsTerm, y)),
                          asList(List.of(Alice), List.of(Bob), List.of(Charlie))),
                arguments(new Project(List.of("y"), new TriplePattern(x, knowsTerm, y)),
                          asList(List.of(Alice), List.of(Bob)))
        ).map(a -> arguments(new Distinct((Op)a.get()[0]), a.get()[1]));
    }
    @ParameterizedTest @MethodSource
    void testDistinct(@NonNull Distinct in, @NonNull Collection<List<Term>> expected) {
        testInContexts(in, expected,
                       Map.of("sparql.distinct.strategy", List.of("HASH", "WINDOW")));
    }

    @SuppressWarnings("unused") static @NonNull Stream<@NonNull Arguments> testTriplePattern() {
        return Stream.of(
                arguments(new TriplePattern(x, ageTerm, i23), List.of(List.of(Alice))),
                arguments(new TriplePattern(x, knowsTerm, Alice),
                          List.of(List.of(Bob), List.of(Charlie))),
                arguments(new TriplePattern(x, knowsTerm, y),
                          List.of(List.of(Alice, Bob), List.of(Bob,     Alice),
                                  List.of(Bob,   Bob), List.of(Charlie, Alice))),
                arguments(new TriplePattern(Alice, knowsTerm, Bob), List.of(List.of())),
                arguments(new TriplePattern(Alice, knowsTerm, Alice), List.of())
        );
    }

    @ParameterizedTest @MethodSource
    void testTriplePattern(@NonNull TriplePattern in, @NonNull Collection<List<Term>> expected) {
        testInContexts(in, expected);
    }

    @SuppressWarnings("unused") static @NonNull Stream<@NonNull Arguments> testUnion() {
        return Stream.of(
                arguments(new Union(new TriplePattern(Alice, ageTerm, x),
                                    new TriplePattern(Bob, ageTerm, x)),
                         asList(List.of(i23), List.of(i25))),
                arguments(new Union(new TriplePattern(Alice, ageTerm, x),
                                    new TriplePattern(Bob, ageTerm, x),
                                    new TriplePattern(Alice, ageTerm, x)),
                          asList(List.of(i23), List.of(i25), List.of(i23))),
                arguments(new Union(new TriplePattern(Alice, knowsTerm, x),
                                    new TriplePattern(x, ageTerm, y)),
                          asList(asList(Bob, null),
                                 asList(Alice, i23), asList(Bob, i25))),
                arguments(new Union(new TriplePattern(y, x, bob)),
                          List.of(asList(Bob, nameTerm))),
                arguments(new Union(new TriplePattern(x, y, AliceEN),
                                    new TriplePattern(y, x, bob)),
                        asList(asList(Alice, nameTerm), asList(nameTerm, Bob))),
                arguments(new Union(new TriplePattern(Alice, knowsTerm, x),
                                    new TriplePattern(Charlie, knowsTerm, y),
                                    new TriplePattern(x, y, AliceEN),
                                    new TriplePattern(y, x, bob)),
                        asList(asList(Bob, null),
                               asList(null, Alice),
                               asList(Alice, nameTerm),
                               asList(nameTerm, Bob)))
        );
    }

    @ParameterizedTest @MethodSource
    void testUnion(@NonNull Union in, @NonNull Collection<List<Term>> expected) {
        testInContexts(in, expected);
    }

    @SuppressWarnings("unused") static @NonNull Stream<@NonNull Arguments> testFilter() {
        return Stream.of(
                arguments(new Filter(new TriplePattern(x, ageTerm, y), List.of("?y > 23")),
                          List.of(asList(Bob, i25))),
                arguments(new Filter(new TriplePattern(x, ageTerm, y),
                                     List.of("?y > \"23\"^^<"+XSD.integer+">")),
                          List.of(asList(Bob, i25))),
                arguments(new Filter(new TriplePattern(x, ageTerm, y), List.of("?y >= 23")),
                          List.of(asList(Alice, i23), asList(Bob, i25))),
                arguments(new Filter(new TriplePattern(x, ageTerm, y), List.of("?y < 23")),
                          List.of()),
                arguments(new Filter(new TriplePattern(x, ageTerm, y), List.of("regex(str(abs(?y)), \".*3$\")")),
                          List.of(asList(Alice, i23)))
        );
    }

    @ParameterizedTest @MethodSource
    void testFilter(@NonNull Filter in, @NonNull Collection<List<Term>> expected) {
        testInContexts(in, expected);
    }

    @SuppressWarnings("unused") static @NonNull Stream<@NonNull Arguments> testJoin() {
        return Stream.of(
    /* 1  */    arguments(new Join(new TriplePattern(x, knowsTerm, y),
                                   new TriplePattern(y, knowsTerm, Alice)),
                          List.of(asList(Alice, Bob), asList(Bob, Bob))),
                // no multiplication cartesian
    /* 2  */    arguments(new Join(new TriplePattern(Alice, RDF.typeTerm, x),
                                   new TriplePattern(Alice, ageTerm, y)),
                          List.of(asList(PersonTerm, i23))),
                // multiplication cartesian
    /* 3  */    arguments(new Join(new TriplePattern(Alice, nameTerm, x),
                                   new TriplePattern(Alice, ageTerm, y)),
                          asList(asList(AliceEN, i23), asList(Alicia, i23))),
                // cycles in join
    /* 4  */    arguments(new Join(new TriplePattern(x, knowsTerm, y),
                                   new TriplePattern(y, knowsTerm, y),
                                   new TriplePattern(y, knowsTerm, x)),
                          asList(asList(Alice, Bob), asList(Bob, Bob))),
                // cycles in join, different order
    /* 5  */    arguments(new Join(new TriplePattern(x, knowsTerm, y),
                                   new TriplePattern(y, knowsTerm, x),
                                   new TriplePattern(y, knowsTerm, y)),
                          asList(asList(Alice, Bob), asList(Bob, Bob))),
    /* 6  */    // two-var path
                arguments(new Join(new TriplePattern(x, knowsTerm, y),
                                   new TriplePattern(y, ageTerm, z)),
                          asList(asList(Alice, Bob, i25),
                                 asList(Bob, Alice, i23),
                                 asList(Bob, Bob, i25),
                                 asList(Charlie, Alice, i23))),
    /* 7  */    // two-var path more solutions on 2nd triple
                arguments(new Join(new TriplePattern(x, knowsTerm, y),
                                   new TriplePattern(y, nameTerm, z)),
                        asList(asList(Alice, Bob, bob),
                               asList(Alice, Bob, roberto),
                               asList(Bob, Alice, AliceEN),
                               asList(Bob, Alice, Alicia),
                               asList(Bob, Bob, bob),
                               asList(Bob, Bob, roberto),
                               asList(Charlie, Alice, AliceEN),
                               asList(Charlie, Alice, Alicia))),
                // path with two tails
    /* 8  */    arguments(new Join(new TriplePattern(x, knowsTerm, y),
                                   new TriplePattern(y, nameTerm, z),
                                   new TriplePattern(y, ageTerm, w)),
                          asList(asList(Alice, Bob, bob, i25),
                                 asList(Alice, Bob, roberto, i25),
                                 asList(Bob, Bob, bob, i25),
                                 asList(Bob, Bob, roberto, i25),
                                 asList(Bob, Alice, AliceEN, i23),
                                 asList(Bob, Alice, Alicia, i23),
                                 asList(Charlie, Alice, AliceEN, i23),
                                 asList(Charlie, Alice, Alicia, i23))),
                // path with two cycles and  two tails
    /* 9  */    arguments(new Join(new TriplePattern(x, knowsTerm, y),
                                   new TriplePattern(y, knowsTerm, y),
                                   new TriplePattern(y, knowsTerm, x),
                                   new TriplePattern(y, nameTerm, z),
                                   new TriplePattern(y, ageTerm, w)),
                        asList(asList(Alice, Bob, bob, i25),
                               asList(Alice, Bob, roberto, i25),
                               asList(Bob, Bob, bob, i25),
                               asList(Bob, Bob, roberto, i25))),
                // paths to a bound object
    /* 10 */    arguments(new Join(new TriplePattern(y, knowsTerm, Bob),
                                   new TriplePattern(x, knowsTerm, y)),
                        asList(asList(Alice, Bob),
                               asList(Alice, Charlie),
                               asList(Bob, Alice),
                               asList(Bob, Bob))),
                // paths to a bound object with any predicate
    /* 11 */    arguments(new Join(new TriplePattern(y, w, Bob),
                                   new TriplePattern(x, w, y)),
                        asList(asList(Alice, knowsTerm, Bob),
                               asList(Alice, knowsTerm, Charlie),
                               asList(Bob, knowsTerm, Alice),
                               asList(Bob, knowsTerm, Bob)))
        );
    }

    @ParameterizedTest @MethodSource
    void testJoin(@NonNull Join in, @NonNull Collection<List<Term>> expected) {
        testInContexts(in, expected, Map.of(
                "sparql.join.reorder", List.of("NONE"),
                "sparql.join.strategy", List.of("BIND")));
    }

    @SuppressWarnings("unused") static @NonNull Stream<@NonNull Arguments> testLeftJoin() {
        return Stream.of(
                // optional always present
    /* 1 */     arguments(new LeftJoin(new TriplePattern(x, knowsTerm, Alice),
                                       new TriplePattern(x, nameTerm, y)),
                          asList(asList(Bob, bob),
                                 asList(Bob, roberto),
                                 asList(Charlie, charlie))),
                // optional adding variable
    /* 2 */     arguments(new LeftJoin(new TriplePattern(y, knowsTerm, Alice),
                                       new TriplePattern(x, knowsTerm, y)),
                          asList(asList(Bob, Alice),
                                 asList(Bob, Bob),
                                 asList(Charlie, null))),
                // optional TP dow not add variable
    /* 3 */     arguments(new LeftJoin(new TriplePattern(x, knowsTerm, y),
                                       new TriplePattern(y, knowsTerm, Alice)),
                        asList(asList(Alice, Bob),
                               asList(Bob, Alice),
                               asList(Bob, Bob),
                               asList(Charlie, Alice))),
                // use optionality of right operand
    /* 4 */     arguments(new LeftJoin(new TriplePattern(x, nameTerm, charlie),
                                       new TriplePattern(x, ageTerm, y)),
                          List.of(asList(Charlie, null))),
                // use optionality on right operand which is a join
    /* 5 */     arguments(new LeftJoin(new TriplePattern(y, knowsTerm, Alice),
                                       new Join(new TriplePattern(x, knowsTerm, y),
                                                new TriplePattern(x, nameTerm, z),
                                                new TriplePattern(x, ageTerm, w))),
                          asList(//     y         x       z          w
                                  asList(Bob,     Bob,    bob,     i25),
                                  asList(Bob,     Bob,    roberto, i25),
                                  asList(Bob,     Alice,  AliceEN, i23),
                                  asList(Bob,     Alice,  Alicia,  i23),
                                  asList(Charlie, null,   null,    null))
                )
        );
    }

    @ParameterizedTest @MethodSource
    void testLeftJoin(@NonNull LeftJoin in, @NonNull Collection<List<Term>> expected) {
        testInContexts(in, expected);
    }

    @SuppressWarnings("unused") static Stream<Arguments> testLimit() {
        return Stream.of(
                arguments(new Limit(23, new TriplePattern(x, ageTerm, y)),
                          asList(asList(Alice, i23), asList(Bob, i25))),
                arguments(new Limit(2,
                                new Project(List.of("x"), new TriplePattern(x, ageTerm, y))),
                          asList(List.of(Alice), List.of(Bob))),
                arguments(new Limit(0, new TriplePattern(x, ageTerm, y)),
                          List.of())
        );
    }

    @ParameterizedTest @MethodSource
    void testLimit(@NonNull Limit in, @NonNull Collection<List<Term>> expected) {
        testInContexts(in, expected);
    }

    @SuppressWarnings("unused") static Stream<Arguments> testOffset() {
        return Stream.of(
                arguments(new Offset(0, new TriplePattern(Alice, ageTerm, x)),
                          List.of(List.of(i23))),
                arguments(new Offset(1, new TriplePattern(x, ageTerm, y)),
                        List.of(asList(Bob, i25)))
        );
    }

    @ParameterizedTest @MethodSource
    void testOffset(@NonNull Offset in, @NonNull Collection<List<Term>> expected) {
        testInContexts(in, expected);
    }

    @SuppressWarnings("unused") static Stream<Arguments> testValues() {
        return Stream.of(
                arguments(new Values(
                                new BatchQuerySolutions(
                                        List.of("x"),
                                        List.of(new Term[][]{{Alice}})),
                                new TriplePattern(x, ageTerm, y)),
                          List.of(List.of(i23))),
                arguments(new Values(
                                new BatchQuerySolutions(
                                        List.of("x", "z"),
                                        List.of(new Term[][]{{Alice, i23}, {Bob, i23}})),
                                new Join(
                                        new TriplePattern(x, knowsTerm, y),
                                        new TriplePattern(y, ageTerm, z),
                                        new TriplePattern(x, nameTerm, w)
                                )),
                          asList(asList(Alice, bob),
                                 asList(Alice, roberto))),
                arguments(new Values(
                                new BatchQuerySolutions(
                                        List.of("x", "z"),
                                        List.of(new Term[][]{{Alice, i25}, {Bob, i23}})),
                                new Join(
                                        new TriplePattern(x, knowsTerm, y),
                                        new TriplePattern(y, ageTerm, z),
                                        new TriplePattern(x, nameTerm, w)
                                )),
                          asList(asList(Bob, AliceEN),
                                 asList(Bob, Alicia),
                                 asList(Alice, bob),
                                 asList(Alice, roberto)))
        );
    }

    @ParameterizedTest @MethodSource
    void testValues(@NonNull Values in, @NonNull Collection<List<Term>> expected) {
        testInContexts(in, expected);
    }

    @SuppressWarnings("unused") static Stream<Arguments> testAssign() {
        return Stream.of(
                arguments(new Assign(new TriplePattern(x, ageTerm, y), "z", "?y * 2"),
                          asList(asList(Alice, i23, i46), asList(Bob, i25, i50))),
                arguments(new Assign(new TriplePattern(x, ageTerm, y), "y", "?y * 2"),
                          asList(asList(Alice, i46), asList(Bob, i50))),
                arguments(new Assign(new Filter(new TriplePattern(x, ageTerm, y), "?y > 23"),
                                     "y", "?y * 2"),
                          List.of(asList(Bob, i50)))
        );
    }

    @ParameterizedTest @MethodSource
    void testAssign(@NonNull Assign in, @NonNull Collection<List<Term>> expected) {
        testInContexts(in, expected);
    }

    @SuppressWarnings("unused") static Stream<Arguments> testExists() {
        List<Term @NonNull[]> has = Row.SINGLE_EMPTY, hasNot = List.of();
        return Stream.of(
        /* 1 */ arguments(Exists.create(new TriplePattern(Alice, knowsTerm, x),
                                        new TriplePattern(x, ageTerm, y)),
                          List.of(List.of(Bob))),
        /* 2 */ arguments(Exists.create(new TriplePattern(Alice, knowsTerm, x),
                                        IdentityNode.INSTANCE),
                          List.of(List.of(Bob))),
        /* 3 */ arguments(Exists.create(new TriplePattern(Alice, knowsTerm, Bob),
                                        IdentityNode.INSTANCE),
                          List.of(List.of())),
        /* 4 */ arguments(Exists.create(new TriplePattern(x, knowsTerm, y),
                                        new TriplePattern(y, ageTerm, z)),
                          asList(asList(Alice, Bob), asList(Bob, Alice), asList(Bob, Bob),
                                 asList(Charlie, Alice))),
        /* 5 */ arguments(Exists.create(new TriplePattern(x, knowsTerm, y),
                                        new TriplePattern(x, ageTerm, z)),
                          asList(asList(Alice, Bob), asList(Bob, Alice), asList(Bob, Bob)))
        );
    }

    @ParameterizedTest @MethodSource
    void testExists(@NonNull Exists in, @NonNull Collection<List<Term>> expected) {
        testInContexts(in, expected);
    }

    @SuppressWarnings("unused") static Stream<Arguments> testNotExists() {
        List<Term @NonNull[]> yes = Row.SINGLE_EMPTY, no = List.of();
        return Stream.of(
                arguments(Exists.not(new TriplePattern(Alice, knowsTerm, y),
                                        new TriplePattern(y, typeTerm, PropertyTerm)),
                          List.of(List.of(Bob))),
                arguments(Exists.not(new TriplePattern(Alice, knowsTerm, Bob),
                                        new TriplePattern(Bob, typeTerm, PropertyTerm)),
                          List.of(List.of())),
                arguments(Exists.not(new TriplePattern(Alice, knowsTerm, Bob),
                                        new TriplePattern(Bob, ageTerm, z)),
                          List.of()),
                arguments(Exists.not(new TriplePattern(x, knowsTerm, y),
                                        new TriplePattern(y, ageTerm, z)),
                          List.of()),
                arguments(Exists.not(new TriplePattern(x, knowsTerm, y),
                                        new TriplePattern(x, ageTerm, z)),
                          List.of(asList(Charlie, Alice))),
                arguments(Exists.not(new TriplePattern(Alice, knowsTerm, x),
                                        IdentityNode.INSTANCE),
                          List.of(List.of(Bob)))
        );
    }

    @ParameterizedTest @MethodSource
    void testNotExists(@NonNull Exists in, @NonNull Collection<List<Term>> expected) {
        testInContexts(in, expected);
    }

    @SuppressWarnings("unused") static Stream<Arguments> testMinus() {
        return Stream.of(
        /* 1 */ arguments(new Minus(new TriplePattern(x, knowsTerm, y),
                                    new TriplePattern(x, ageTerm, z)),
                          List.of(asList(Charlie, Alice))),
        /* 2 */ arguments(new Minus(new TriplePattern(x, knowsTerm, y),
                                    new TriplePattern(y, ageTerm, z)),
                          List.of()),
        /* 3 */ arguments(new Minus(new TriplePattern(x, knowsTerm, y),
                                    new TriplePattern(y, knowsTerm, x)),
                          List.of(asList(Charlie, Alice))),
        /* 4 */ arguments(new Minus(new TriplePattern(x, knowsTerm, y),
                                    new Filter(new TriplePattern(y, knowsTerm, z), "?z = ?y")),
                          asList(asList(Bob, Alice), asList(Charlie, Alice))),
                // per § 8.3.3 of http://www.w3.org/TR/sparql11-query/, y is unbound
                // during execution of the MINUS right operand, making the filter vacuously true
        /* 5 */ arguments(new Minus(new TriplePattern(x, knowsTerm, y),
                        new Filter(new TriplePattern(y, knowsTerm, z), "?z = ?x")),
                asList(asList(Alice, Bob),
                       asList(Bob, Alice),
                       asList(Bob, Bob),
                       asList(Charlie, Alice)))
        );
    }

    @ParameterizedTest @MethodSource
    void testMinus(@NonNull Minus in, @NonNull Collection<List<Term>> expected) {
        testInContexts(in, expected,
                       Map.of("sparql.minus.strategy", List.of("BIND", "SET")));
    }

    @SuppressWarnings("unused") static Stream<Arguments> testAsk() {
        List<List<Term>> positive = List.of(List.of());
        List<List<Term>> negative = List.of();
        return Stream.of(
                arguments(new Ask(new TriplePattern(x, knowsTerm, y)), positive),
                arguments(new Ask(new TriplePattern(x, mboxTerm, y)), negative),
                arguments(new Ask(new Filter(new TriplePattern(x, ageTerm, y),
                                             "?y > 23")),
                          positive),
                arguments(new Ask(new Filter(new TriplePattern(x, ageTerm, y),
                                             "?y < 23")),
                          negative),
                arguments(new Ask(new Union(new TriplePattern(x, mboxTerm, y),
                                            new TriplePattern(x, nameTerm, bob))),
                          positive)
        );
    }

    @ParameterizedTest @MethodSource
    void testAsk(@NonNull Ask in, @NonNull Collection<List<Term>> expected) {
        testInContexts(in, expected);
    }
}