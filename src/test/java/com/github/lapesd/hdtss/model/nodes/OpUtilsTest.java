package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.vocab.FOAF;
import com.github.lapesd.hdtss.vocab.XSD;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestUtils.parseTriplePattern;
import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.knowsTerm;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class OpUtilsTest {
    static @NonNull Stream<Arguments> testDeepEquals() {
        var tp1 = parseTriplePattern("<" + EX + "Alice> <" + FOAF.name + "> \"alice\"@en");
        var tp1_ = parseTriplePattern("<" + EX + "Alice> <" + FOAF.name + "> \"alice\"@en");
        var tp2 = parseTriplePattern("<" + EX + "Alice> <" + FOAF.name + "> \"alice\"@pt");
        var tp3 = parseTriplePattern("?x <"+FOAF.knows+"> ?y");
        var tp3_ = parseTriplePattern("?x <"+FOAF.knows+"> ?y");
        var tp4 = parseTriplePattern("?y <"+FOAF.name+"> \"bob\"^^<"+ XSD.string+">");
        var tp5 = parseTriplePattern("?x <"+FOAF.age+"> \"23\"^^<"+ XSD.integer+">");
        var tp5_ = parseTriplePattern("?x <"+FOAF.age+"> \"23\"^^<"+ XSD.integer+">");
        return Stream.of(
                arguments(true, tp1, tp1),
                arguments(true, tp1, tp1_),
                arguments(false, tp1, tp2),
                arguments(true, IdentityNode.get(), IdentityNode.INSTANCE),
                arguments(false, IdentityNode.get(), tp1),
                arguments(true, new Union(tp1, tp2), new Union(tp1_, tp2)),
                arguments(true, new Union(tp1, tp2), new Union(tp1_, tp2)),
                arguments(false, new Union(tp1, tp2), new Union(tp1_, tp3)),
                arguments(true, new Join(tp3, tp4), new Join(tp3, tp4)),
                arguments(true, new Join(tp3, tp4), new Join(tp3_, tp4)),
                arguments(true,  new Union(new Join(tp3, tp4), tp5), new Union(new Join(tp3_, tp4), tp5)),
                arguments(true,  new Union(new Join(tp3, tp4), tp5), new Union(new Join(tp3_, tp4), tp5_)),
                arguments(false, new Union(new Join(tp3, tp4), tp5), new Union(new Join(tp3_, tp5), tp4)),
                arguments(true,  new Filter(new Union(new Join(tp3, tp4), tp5 ), List.of("?x = ?y")),
                                 new Filter(new Union(new Join(tp3, tp4), tp5 ), List.of("?x = ?y"))),
                arguments(true,  new Filter(new Union(new Join(tp3, tp4), tp5_), List.of("?x = ?y")),
                                 new Filter(new Union(new Join(tp3, tp4), tp5 ), List.of("?x = ?y"))),
                arguments(false, new Filter(new Union(new Join(tp3, tp4), tp5 ), List.of("?x = ?y")),
                                 new Filter(new Union(new Join(tp3, tp4), tp5 ), List.of("?y = ?x"))),
                arguments(false, new Filter(new Union(new Join(tp3, tp4), tp5 ), List.of("?x = ?y")),
                                 new Filter(new Union(new Join(tp3, tp5), tp4 ), List.of("?x = ?y")))
        );
    }

    @ParameterizedTest @MethodSource
    void testDeepEquals(boolean expected, @NonNull Op a, @NonNull Op b) {
        assertEquals(expected, a.deepEquals(b));
        assertEquals(expected, b.deepEquals(a));
    }

    static @NonNull Stream<Arguments> flattenSingleNodes() {
        var tp1 = parseTriplePattern("<" + EX + "Alice> <" + FOAF.name + "> \"alice\"@en");
        var tp2 = parseTriplePattern("?x <"+FOAF.knows+"> ?y");
        var tp3 = parseTriplePattern("?y <"+FOAF.name+"> \"bob\"^^<"+ XSD.string+">");
        var tp4 = parseTriplePattern("?x <"+FOAF.age+"> \"23\"^^<"+ XSD.integer+">");

        return Stream.of(
                arguments(tp1, tp1),
                arguments(new Join(tp2, tp3), new Join(tp2, tp3)),
                arguments(new Join(tp2, new Join(tp3, tp4)), new Join(tp2, tp3, tp4)),
                arguments(new Join(tp1, IdentityNode.get()), tp1),
                arguments(new Union(IdentityNode.get(), tp1), tp1),
                arguments(new Union(new Join(tp2, tp3), tp4), new Union(new Join(tp2, tp3), tp4)),
                arguments(new Union(tp2, tp4, IdentityNode.get()), new Union(tp2, tp4)),
                arguments(new Union(tp2, new Union(tp4, tp3)), new Union(tp2, tp4, tp3)),
                arguments(new Filter(tp2, List.of("?x = ?y")),
                          new Filter(tp2, List.of("?x = ?y"))),
                arguments(new Filter(new Filter(tp2, List.of("str(?x) > str(?y)")),
                                     List.of("?x = ?y")),
                          new Filter(tp2, asList("?x = ?y", "str(?x) > str(?y)")))
        );
    }

    @ParameterizedTest @MethodSource
    void flattenSingleNodes(@NonNull Op in, @NonNull Op expected) {
        Op actual = OpUtils.flatten(in);
        assertTrue(expected.deepEquals(actual), "expected "+expected+", got "+actual);
        assertTrue(actual.deepEquals(expected), "expected "+expected+", got "+actual);
        if (expected.deepEquals(in))
            assertSame(actual, in);
    }

    static Stream<Arguments> flattenList() {
        IdentityNode id = IdentityNode.INSTANCE;
        return Stream.of(
        /* 1 */ arguments(Join.class,
                          List.of(new TriplePattern(x, knowsTerm, y)),
                          List.of(new TriplePattern(x, knowsTerm, y))),
        /* 2 */ arguments(Join.class,
                          List.of(new Join(new TriplePattern(x, knowsTerm, y))),
                          List.of(new TriplePattern(x, knowsTerm, y))),
        /* 3 */ arguments(Join.class, List.of(id), List.of(id)),
        /* 4 */ arguments(Join.class, asList(id, id), List.of(id)),
        /* 5 */ arguments(Join.class, List.of(new Join(id)), List.of(id)),
        /* 6 */ arguments(Join.class, asList(new Join(new TriplePattern(x, knowsTerm, y)), id),
                          List.of(new TriplePattern(x, knowsTerm, y))),
        /* 7 */ arguments(Join.class, asList(id, new Join(new TriplePattern(x, knowsTerm, y))),
                          List.of(new TriplePattern(x, knowsTerm, y))),
        /* 8 */ arguments(Join.class, asList(new Join(new TriplePattern(x, knowsTerm, y),
                                                      new TriplePattern(y, knowsTerm, z)),
                                             new Join(new TriplePattern(z, knowsTerm, w))),
                          asList(new TriplePattern(x, knowsTerm, y),
                                 new TriplePattern(y, knowsTerm, z),
                                 new TriplePattern(z, knowsTerm, w))),
        /* 8 */ arguments(Join.class, asList(new Union(new TriplePattern(Alice, knowsTerm, x),
                                                       new TriplePattern(Bob, knowsTerm, x)),
                                             new TriplePattern(x, knowsTerm, y)),
                          asList(new Union(new TriplePattern(Alice, knowsTerm, x),
                                           new TriplePattern(Bob, knowsTerm, x)),
                                 new TriplePattern(x, knowsTerm, y)))
        );
    }

    @ParameterizedTest @MethodSource
    void flattenList(@NonNull Class<? extends Op> parentCls, @NonNull List<@NonNull Op> list,
                     @NonNull List<Op> expected) {
        List<@NonNull Op> actual = OpUtils.flatten(parentCls, list);
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < actual.size(); i++) {
            assertTrue(  actual.get(i).deepEquals(expected.get(i)), "i="+i);
            assertTrue(expected.get(i).deepEquals(  actual.get(i)), "i="+i);
        }
    }

}