package com.github.lapesd.hdtss.utils;

import com.github.lapesd.hdtss.model.LiteralQuote;
import com.github.lapesd.hdtss.vocab.XSD;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestUtils.openResource;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@SuppressWarnings("unused")
@Tag("fast")
class TokenPatternsTest {
    static Stream<Arguments> testValidLiteral() throws IOException {
        List<Arguments> argumentsList = new ArrayList<>(Stream.of(
                "\"\"",
                "\"\"@en",
                "\"\"^^<"+XSD.string+">",
                "\"23\"^^<"+XSD.integer+">",
                "\" \"",
                "\"\\\"\"",
                "\"\\n\"",
                "\"\\r\"",
                "\"\\\\\"",
                "\" \"@en",
                "\"\\\"\"@en",
                "\"\\n\"@en",
                "\"\\r\"@en",
                "\"\\\\\"@en",
                "\" \"@pt-BR",
                "\"\\\"\"@pt-BR",
                "\"\\n\"@pt-BR",
                "\"\\r\"@pt-BR",
                "\"\\\\\"@pt-BR",
                "\" \"^^<"+XSD.string+">",
                "\"\\\"\"^^<"+XSD.string+">",
                "\"\\n\"^^<"+XSD.string+">",
                "\"\\r\"^^<"+XSD.string+">",
                "\"\\\\\"^^<"+XSD.string+">"
        ).map(Arguments::arguments).toList());

        // add examples from literals.ttl
        try (var in = openResource(TokenPatternsTest.class, "../data/literals.ttl")) {
            for (String line : IOUtils.readLines(in, StandardCharsets.UTF_8)) {
                if (!line.startsWith(":"))
                    continue;
                String ttl = line.split(" +", 3)[2];
                if (!ttl.matches("^\"[^\"].*"))
                    continue;
                ttl = ttl.replaceAll(" *\\. *$", "")
                         .replaceAll("\\^\\^(xsd:.*)", "^^<$1>");
                if (LiteralQuote.fromLiteral(ttl) == LiteralQuote.DOUBLE)
                    argumentsList.add(arguments(ttl));
            }
        }
        return argumentsList.stream();
    }

    @ParameterizedTest @MethodSource
    void testValidLiteral(String nt) {
        assertTrue(TokenPatterns.LITERAL_RX.matcher(nt).matches());
    }

    @ParameterizedTest @ValueSource(strings = {
            "\"",
            "'",
            "\"\"\"",
            "'''",
            "\"\"@pt_BR",
            "''@pt_BR",
            "<>",
            "\"\n\"",
            "\"\r\"",
            "\"\\\"",
            "\"bob\"^^xsd:string",
            "\"bob\"^^",
            "\"bob\"@",
    })
    void testInvalidLiteral(String nt) {
        assertFalse(TokenPatterns.LITERAL_RX.matcher(nt).matches());
    }
}