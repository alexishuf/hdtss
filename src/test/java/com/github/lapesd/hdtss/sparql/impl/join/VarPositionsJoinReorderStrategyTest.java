package com.github.lapesd.hdtss.sparql.impl.join;

import com.github.lapesd.hdtss.model.nodes.Join;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.github.lapesd.hdtss.model.nodes.Union;
import com.github.lapesd.hdtss.sparql.impl.JenaSparqlParser;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.*;
import static com.github.lapesd.hdtss.vocab.RDF.typeTerm;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class VarPositionsJoinReorderStrategyTest {
    @ParameterizedTest @ValueSource(strings = {
            "0      :: <a> <b> <c>",
            "10     :: <a> ?x  <c>",
            "20     :: <a> <b> ?x",
            "10     :: <a> ?x  <c>",
            "100    :: <a> ?y  ?z",
            "1000   :: ?x  <b> <c>",
            "2000   :: ?x  ?y  <c>",
            "10000  :: ?x  <b> ?z",
            "100000 :: ?x  ?y  ?z",

    })
    void testEstimate(String data) {
        String[] parts = data.split(" +:: +");
        assertEquals(2, parts.length);
        int expected = Integer.parseInt(parts[0]);
        Op op = new JenaSparqlParser().parse("SELECT * WHERE { " + parts[1] + " }");
        assertEquals(expected, VarPositionsJoinReorderStrategy.estimate(op));
    }

    static Stream<Arguments> testReorder() {
        return Stream.of(
                // trivial
                arguments(new Join(new TriplePattern(Alice, knowsTerm, Bob)), null),
                // no problem with literals
                arguments(new Join(new TriplePattern(Alice, nameTerm, AliceEN)), null),
                // swap two triples
                arguments(Join.of(new TriplePattern(x, nameTerm, y),
                                  new TriplePattern(x, knowsTerm, Alice)),
                          new JoinReorder(asList(new TriplePattern(x, knowsTerm, Alice),
                                                 new TriplePattern(x, nameTerm, y)),
                                          null)),
                // swap two triples requiring a projection
                arguments(Join.of(new TriplePattern(x, nameTerm, y),
                                  new TriplePattern(y, knowsTerm, Alice)),
                          new JoinReorder(asList(new TriplePattern(y, knowsTerm, Alice),
                                                 new TriplePattern(x, nameTerm, y)),
                                          new int[] {1, 0})),
                // two triples and no change
                arguments(Join.of(new TriplePattern(x, knowsTerm, Alice),
                                  new TriplePattern(x, nameTerm, y)),
                          null),
                // three triples and no change
                arguments(Join.of(new TriplePattern(x, typeTerm, PersonTerm),
                                  new TriplePattern(x, knowsTerm, Bob),
                                  new TriplePattern(x, nameTerm, y)),
                          null),
                // three triples: move first last and do not change other two
                arguments(Join.of(new TriplePattern(x, nameTerm, y),
                                  new TriplePattern(x, typeTerm, PersonTerm),
                                  new TriplePattern(x, knowsTerm, Bob)),
                          new JoinReorder(asList(new TriplePattern(x, typeTerm, PersonTerm),
                                                 new TriplePattern(x, knowsTerm, Bob),
                                                 new TriplePattern(x, nameTerm, y)),
                                          null)),
                // three triples: move first to last, do not change other two and project
                arguments(Join.of(new TriplePattern(x, nameTerm, y),
                                  new TriplePattern(y, typeTerm, PersonTerm),
                                  new TriplePattern(y, knowsTerm, Bob)),
                        new JoinReorder(asList(new TriplePattern(y, typeTerm, PersonTerm),
                                               new TriplePattern(y, knowsTerm, Bob),
                                               new TriplePattern(x, nameTerm, y)),
                                       new int[] {1, 0})),
                // three triples: move first last and do not change other two
                arguments(Join.of(new TriplePattern(x, nameTerm, y),
                                  new TriplePattern(x, knowsTerm, Bob),
                                  new TriplePattern(x, typeTerm, PersonTerm)),
                          new JoinReorder(asList(new TriplePattern(x, knowsTerm, Bob),
                                                 new TriplePattern(x, typeTerm, PersonTerm),
                                                 new TriplePattern(x, nameTerm, y)),
                                          null)),
                // prioritize non-union
                arguments(Join.of(Union.of(new TriplePattern(x, nameTerm, bob),
                                           new TriplePattern(x, nameTerm, roberto)),
                                  new TriplePattern(x, knowsTerm, Alice)),
                          new JoinReorder(asList(new TriplePattern(x, knowsTerm, Alice),
                                                 Union.of(new TriplePattern(x, nameTerm, bob),
                                                          new TriplePattern(x, nameTerm, roberto))),
                                          null)),
                // prioritize non-union: no change
                arguments(Join.of(new TriplePattern(x, knowsTerm, Alice),
                                  Union.of(new TriplePattern(x, nameTerm, bob),
                                           new TriplePattern(x, nameTerm, roberto))),
                          null),
                // do not introduce cartesian products
                arguments(Join.of(new TriplePattern(x, knowsTerm, y),
                                  new TriplePattern(Alice, knowsTerm, x),
                                  new TriplePattern(Bob, knowsTerm, y)),
                          new JoinReorder(asList(new TriplePattern(Alice, knowsTerm, x),
                                                 new TriplePattern(x, knowsTerm, y),
                                                 new TriplePattern(Bob, knowsTerm, y)),
                                          null)),
                // do not introduce cartesian products, requiring projection
                arguments(Join.of(new TriplePattern(x, knowsTerm, y),
                                  new TriplePattern(Bob, knowsTerm, y),
                                  new TriplePattern(Alice, knowsTerm, x)),
                        new JoinReorder(asList(new TriplePattern(Bob, knowsTerm, y),
                                               new TriplePattern(x, knowsTerm, y),
                                               new TriplePattern(Alice, knowsTerm, x)),
                                new int[]{1, 0})),
                //same as above but more triples and no projection
                arguments(Join.of(new TriplePattern(x, knowsTerm, y),
                                  new TriplePattern(x, nameTerm, AliceEN),
                                  new TriplePattern(y, ageTerm, i23),
                                  new TriplePattern(x, ageTerm, i23)),
                          new JoinReorder(asList(new TriplePattern(x, nameTerm, AliceEN),
                                                 new TriplePattern(x, ageTerm, i23),
                                                 new TriplePattern(x, knowsTerm, y),
                                                 new TriplePattern(y, ageTerm, i23)),
                                          null)),
                // count all previous variables when preventing cartesian products
                arguments(Join.of(new TriplePattern(x, knowsTerm, y),
                                  new TriplePattern(y, knowsTerm, z),
                                  new TriplePattern(x, knowsTerm, w)),
                          null)
        );
    }

    @ParameterizedTest @MethodSource
    void testReorder(@NonNull Join in, @Nullable JoinReorder expected) {
        var reorder = new VarPositionsJoinReorderStrategy().reorder(in);
        if (expected == null) {
            assertNull(reorder);
        } else {
            assertNotNull(reorder);
            assertArrayEquals(expected.projection(), reorder.projection());
            assertEquals(expected.operands().size(), reorder.operands().size());
            for (int i = 0; i < expected.operands().size(); i++) {
                Op ex = expected.operands().get(i), ac = reorder.operands().get(i);
                assertTrue(ex.deepEquals(ac), "i="+i);
                assertTrue(ac.deepEquals(ex), "i="+i);
            }
        }
    }

    static Stream<Arguments> testProjection() {
        return Stream.of(
                arguments(List.of("x"),
                          List.of(new TriplePattern(x, knowsTerm, Bob)),
                          null),
                arguments(asList("x", "y"),
                          List.of(new TriplePattern(x, knowsTerm, y)),
                          null),
                arguments(asList("x", "y"),
                          asList(new TriplePattern(x, knowsTerm, Bob),
                                 new TriplePattern(y, knowsTerm, Bob)),
                          null),
                arguments(asList("x", "y"),
                          asList(new TriplePattern(x, nameTerm, AliceEN),
                                 new TriplePattern(x, knowsTerm, y)),
                          null),
                arguments(asList("x", "y"),
                          asList(new TriplePattern(y, nameTerm, bob),
                                 new TriplePattern(x, knowsTerm, y)),
                          new int[]{1, 0}),
                arguments(asList("x", "y", "z"),
                          asList(new TriplePattern(y, nameTerm, bob),
                                 new TriplePattern(y, ageTerm, z),
                                 new TriplePattern(x, knowsTerm, y)),
                          new int[] {2, 0, 1})
        );
    }

    @ParameterizedTest @MethodSource
    void testProjection(List<String> exposed, List<Op> reordered, int[] expected) {
        int[] actual = VarPositionsJoinReorderStrategy.getProjection(exposed, reordered);
        assertArrayEquals(expected, actual);
    }

    @ParameterizedTest @ValueSource(strings = {
            "true  :: ",
            "true  :: 0",
            "false :: 1",
            "true  :: 0, 1",
            "false :: 1, 0",
            "true  :: 0, 1, 2",
            "false :: 0, 2, 1",
    })
    void testIsNoOp(String data) {
        String[] parts = data.split(" +:: +");
        boolean expected = Boolean.parseBoolean(parts[0]);
        long[] input = Arrays.stream((parts.length > 1 ? parts[1] : "").split(" *, *"))
                            .filter(s -> !s.isBlank())
                            .mapToLong(Integer::parseInt).toArray();
        assertEquals(expected, VarPositionsJoinReorderStrategy.isNoOp(input));
    }

}