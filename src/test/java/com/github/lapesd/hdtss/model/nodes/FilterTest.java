package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.vocab.FOAF;
import com.github.lapesd.hdtss.vocab.XSD;
import com.google.common.collect.Collections2;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestUtils.parseTriplePattern;
import static com.github.lapesd.hdtss.TestVocab.EX;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class FilterTest {
    static @NonNull Stream<@NonNull Arguments> testBind() {
        TriplePattern tp = parseTriplePattern("?x <" + FOAF.knows + "> $y");
        Term alice = new Term("<" + EX + "Alice>");
        return Stream.of(
                arguments(new Filter(tp, singletonList("?x = ?y")),
                          Map.of("x", alice),
                          new Filter(parseTriplePattern("<"+EX+"Alice> <"+FOAF.knows+"> $y"),
                                     singletonList("<"+EX+"Alice> = ?y"))),
                arguments(new Filter(tp, singletonList("str(?x) > str($y)")),
                          Map.of("y", alice),
                          new Filter(parseTriplePattern("?x <"+FOAF.knows+"> <"+EX+"Alice>"),
                                  singletonList("str(?x) > str(<"+EX+"Alice>)")))
        );
    }

    @SuppressWarnings("UnstableApiUsage")
    @ParameterizedTest
    @MethodSource
    void testBind(@NonNull Filter in, @NonNull Map<@NonNull String, Term> v2t,
                  @NonNull Filter expected) {
        Op actual = in.bind(v2t);
        String msg = "\nExpected: " + expected + "\n  Actual: " + actual;
        assertTrue(actual.deepEquals(expected), msg);
        assertTrue(expected.deepEquals(actual), msg);


        for (List<String> varNames : Collections2.permutations(v2t.keySet())) {
            Term[] terms = new Term[varNames.size()];
            for (int i = 0; i < terms.length; i++)
                terms[i] = v2t.get(varNames.get(i));
            Op actual2 = in.bind(varNames, terms);
            String msg2 = "\nExpected: "+expected+"\n  Actual: "+actual2;
            assertTrue(actual2.deepEquals(expected), msg2);
            assertTrue(expected.deepEquals(actual2), msg2);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "?x > ?y:::x y",
            "?x > ?y || true:::x y",
            "?x < ?y:::x y",
            "?x < $y:::x y",
            "?x <?y:::x y",
            "?x <=?y:::x y",
            "?x < str(?y):::x y",
            "?x123ção > ?y_09:::x123ção y_09",
            "?x > str($y):::x y",
            "?x > concat('$', str($y)):::x y",
            "?x > concat('?', str($y)):::x y",
            "?x > concat('?z', str($y)):::x y",
            "?x > concat('$z', str($y)):::x y",
            "?x > concat(\"$\", str($y)):::x y",
            "?x > concat(\"?\", str($y)):::x y",
            "?x > concat(\"?z\", str($y)):::x y",
            "?x > concat(\"$z\", str($y)):::x y",
            "?x > concat('''?z''', str($y)):::x y",
            "?x > concat(\"\"\"?z\"\"\", str($y)):::x y",
            "?x > concat(\"\"\"\"?z\"\"\"\", str($y)):::x y",
            "?x > concat(\"\"\"'\"'?z'\"'\"\"\", str($y)):::x y",
            "?x > concat(\"\\\"?y\", str($y)):::x y",
            "?x > concat('\\'?y', str($y)):::x y",
            "?x = str(<"+EX+"?get.php>) && ?y < $z:::x y z",
    })
    void testFindVarNames(@NonNull String data) {
        String[] pieces = data.split(":::", 2);
        Set<@NonNull String> expected = Set.of(pieces[1].split(" "));
        Set<@NonNull String> set = new HashSet<>();
        Filter.findVarNames(pieces[0], set);
        assertEquals(expected, set);
    }

    static @NonNull Stream<Arguments> testBindFilter() {
        Term i23 = new Term("\"23\"^^<"+XSD.integer+">");
        Term i25 = new Term("\"25\"^^<"+XSD.integer+">");

        return Stream.of(
                arguments("?x > ?y", Map.of("y", i23), "?x > "+i23),
                arguments("?x < ?y", Map.of("y", i23), "?x < "+i23),
                arguments("?x <?y", Map.of("y", i23), "?x <"+i23),
                arguments("?x < ?y", Map.of("y", i23), "?x < "+i23),
                arguments("?x < "+i25, Map.of("x", i23), i23+" < "+i25),
                arguments("?x <"+i25, Map.of("x", i23), i23+" <"+i25),
                arguments("?x > ?y || false", Map.of("y", i23), "?x > "+i23+" || false"),
                arguments("?x = $y", Map.of("x", i23, "y", i25), i23+" = "+i25),
                arguments("str(?x) = str($y)", Map.of("x", i23, "y", i25),
                          "str("+i23+") = str("+i25+")"),
                arguments("str(?x) = concat('''asd?x$y'''@en, str($y))",
                          Map.of("x", i23, "y", i25),
                          "str("+i23+") = concat('''asd?x$y'''@en, str("+i25+"))"),
                arguments("str(?x) = concat(<"+EX+"/get?x=$y>, str($y))",
                        Map.of("x", i23, "y", i25),
                        "str("+i23+") = concat(<"+EX+"/get?x=$y>, str("+i25+"))")
        );
    }

    @ParameterizedTest
    @MethodSource
    void testBindFilter(@NonNull String filter, @NonNull Map<@NonNull String, Term> v2t,
                        @NonNull String expected) {
        assertEquals(expected, Filter.bindFilter(filter, v2t));
    }

}