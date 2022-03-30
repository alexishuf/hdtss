package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.ageTerm;
import static com.github.lapesd.hdtss.vocab.FOAF.knowsTerm;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class UnionTest {
    static Stream<Arguments> testInputFilterVarNames() {
        return Stream.of(
                arguments(new Union(new TriplePattern(Alice, knowsTerm, x),
                                    new TriplePattern(Bob, knowsTerm, x)),
                         Set.of()),
                arguments(new Union(new TriplePattern(Alice, ageTerm, x),
                                    new Filter(new TriplePattern(Bob, ageTerm, x), "?x > 23")),
                        Set.of()),
                arguments(new Union(new TriplePattern(Alice, ageTerm, x),
                                    new Filter(new TriplePattern(Bob, ageTerm, x), "?x > ?y")),
                        Set.of("y")),
                arguments(new Union(new Filter(new TriplePattern(Bob, ageTerm, x), "?x > ?y"),
                                    new TriplePattern(Alice, ageTerm, x)),
                        Set.of("y")),
                arguments(new Union(new Filter(new TriplePattern(Bob, ageTerm, x), "?x > ?y"),
                                    new Filter(new TriplePattern(Alice, ageTerm, x), "?x > ?z")),
                        Set.of("y", "z")),
                // one operand do not feed another, thus both ?y and ?z remain unbound in filters
                arguments(new Union(new Filter(new TriplePattern(z, ageTerm, x), "?x > ?y"),
                                    new Filter(new TriplePattern(y, ageTerm, x), "?x > ?z")),
                        Set.of("y", "z"))
        );
    }

    @ParameterizedTest @MethodSource
    void testInputFilterVarNames(@NonNull Union op, @NonNull Set<@NonNull String> expected) {
        assertEquals(op.inputVars(), expected);
    }

}