package com.github.lapesd.hdtss.sparql.results;

import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.results.codecs.CSVEncoder;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class CSVEncoderTest extends CodecTestBase {

    CSVEncoderTest() {
        super(CSVEncoder.class);
    }

    static Stream<Arguments> data() {
        return Stream.of(
                arguments(ASK_FALSE, "\r\n"),
                arguments(ASK_TRUE, "\r\n\r\n"),
                arguments(ONE_ONE, """
                        x\r
                        http://example.org/Alice\r
                        """),
                arguments(ONE_ROW, """
                        x,y,z,w\r
                        Alice,bob,23,_:blank1\r
                        """),
                arguments(TWO_ROWS, """
                        x\r
                        \r
                        http://example.org/Charlie\r
                        """)
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
}