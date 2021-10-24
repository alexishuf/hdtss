package com.github.lapesd.hdtss.sparql.results;

import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.results.codecs.TSVCodec;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class TSVCodecTest extends CodecTestBase {

    TSVCodecTest() {
        super(TSVCodec.class);
    }

    static Stream<Arguments> data() {
        return Stream.of(
                arguments(ASK_FALSE, "\n"),
                arguments(ASK_TRUE, "\n\n"),
                arguments(ONE_ONE, "?x\n"+
                        Alice+"\n"),
                arguments(ONE_ROW, "?x\t?y\t?z\t?w\n" +
                        AliceEN+"\t"+bob+"\t"+i23+"\t"+blank1+"\n"),
                arguments(TWO_ROWS, "?x\n"+
                        "\n"+
                        Charlie+"\n")
        );
    }

    @ParameterizedTest @MethodSource("data")
    public void testEncodeToStream(@NonNull QuerySolutions solutions, @NonNull String expected) {
        doTestEncodeToStream(solutions, expected);
    }

    @ParameterizedTest @MethodSource("data")
    public void testEncodeToArray(@NonNull QuerySolutions solutions, @NonNull String expected) {
        doTestEncodeToArray(solutions, expected);
    }

    @ParameterizedTest @MethodSource("data")
    public void testEncodeToBuffer(@NonNull QuerySolutions solutions, @NonNull String expected) {
        doTestEncodeToBuffer(solutions, expected);
    }

    @ParameterizedTest @MethodSource("data")
    public void testDecode(@NonNull QuerySolutions expected, @NonNull String encoded) {
        doTestDecode(expected, encoded);
    }
}