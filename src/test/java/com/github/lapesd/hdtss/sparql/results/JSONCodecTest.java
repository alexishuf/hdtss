package com.github.lapesd.hdtss.sparql.results;

import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.results.codecs.JSONCodec;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class JSONCodecTest extends CodecTestBase {

    public JSONCodecTest() {
        super(JSONCodec.class);
    }

    static Stream<Arguments> data() {
        return Stream.of(
                arguments(ASK_FALSE,"""
                        {"head":{"vars":[]},"boolean":false}"""),
                arguments(ASK_TRUE, """
                        {"head":{"vars":[]},"boolean":true}"""),
                arguments(ONE_ONE, """
                        {"head":{"vars":["x"]},"results":{"bindings":[{"x":{"type":"uri","value":"http://example.org/Alice"}}]}}"""),
                arguments(ONE_ROW, """
                        {"head":{"vars":["x","y","z","w"]},"results":{"bindings":[{"x":{"type":"literal","value":"Alice","xml:lang":"en"},"y":{"type":"literal","value":"bob","datatype":"http://www.w3.org/2001/XMLSchema#string"},"z":{"type":"literal","value":"23","datatype":"http://www.w3.org/2001/XMLSchema#integer"},"w":{"type":"bnode","value":"blank1"}}]}}"""),
                arguments(TWO_ROWS, """
                        {"head":{"vars":["x"]},"results":{"bindings":[{},{"x":{"type":"uri","value":"http://example.org/Charlie"}}]}}"""),
                arguments(XML_PROBLEMATIC, """
                        {"head":{"vars":["x","y"]},"results":{"bindings":[{"x":{"type":"literal","value":"&","datatype":"http://www.w3.org/2001/XMLSchema#string"},"y":{"type":"literal","value":"<&>","datatype":"http://www.w3.org/2001/XMLSchema#string"}},{"x":{"type":"uri","value":"http://example.org/search?q=1&o=2"},"y":{"type":"literal","value":"x > 2\\n&& y < 3","datatype":"http://www.w3.org/2001/XMLSchema#string"}}]}}"""),
                arguments(CSV_PROBLEMATIC, """
                        {"head":{"vars":["x","y"]},"results":{"bindings":[{"x":{"type":"literal","value":"\\"1\\n2\\"","datatype":"http://www.w3.org/2001/XMLSchema#string"},"y":{"type":"literal","value":"a,b","xml:lang":"en"}}]}}""")
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