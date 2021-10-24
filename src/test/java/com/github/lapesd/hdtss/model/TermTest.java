package com.github.lapesd.hdtss.model;

import com.github.lapesd.hdtss.vocab.FOAF;
import com.github.lapesd.hdtss.vocab.RDF;
import com.github.lapesd.hdtss.vocab.XSD;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.validation.constraints.NotNull;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@SuppressWarnings({"ResultOfMethodCallIgnored", "unused"})
@Tag("fast")
class TermTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "25:<http://example.org/Alice>",
            "27:<http://u@example.org/Alice>",
            "29:<http://u:p@example.org/Alice>",
            "31:<http://u:p@example.org/Alice^^>",
            "6:\"false\"^^<"+XSD.xboolean+">",
            "3:\"23\"^^<"+XSD.integer+">",
            "3:\"23\"^^<"+XSD.xint+">",
            "5:\"23.0\"^^<"+XSD.xdouble+">",
            "6:\"alice\"",
            "6:\"alice\"@fr",
            "4:\"bob\"@en-US",
            "4:\"bob\"^^<"+XSD.string+">",
            "1:\"\"@en",
            "1:\"\"^^<"+XSD.string+">",
            "1:\"\"",
            "2:?x",
            "8:?varName",
            "7:_:label",
            "1:[]"
    })
    void testContentEnd(String string) {
        int expected = Integer.parseInt(string.replaceAll("^(\\d+):.*$", "$1"));
        String sparql = string.replaceAll("^\\d+:", "");
        Term term = new Term(sparql);
        int actual = term.contentEnd();
        assertEquals(expected, actual);
        switch (term.type()) {
            case VAR     -> assertEquals(sparql.length(), actual);
            case LITERAL -> assertEquals('"', sparql.charAt(actual));
            case URI     -> assertEquals('>', sparql.charAt(actual));
            case BLANK   -> {
                if (sparql.startsWith("[")) assertEquals(1, actual);
                else                        assertEquals(sparql.length(), actual);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "false!\"false\"^^<"+XSD.xboolean+">",
            "alice!\"alice\"^^<"+XSD.string+">",
            "bob!\"bob\"@en",
            "bob!\"bob\"",
            "!\"\"^^<"+XSD.string+">",
            "http://example.org/A!<http://example.org/A>",
            "relative unescaped uri!<relative unescaped uri>",
            "proper%20rel!<proper%20rel>",
            "label!_:label",
            "![]",
            "x!?x",
            "x2!?x2",
    })
    void testContent(String string) {
        String expected = string.replaceAll("^([^!]*)!.*$", "$1");
        String  sparql = string.replaceAll("^[^!]*!(.*)$", "$1");
        assertEquals(expected, new Term(sparql).content());
    }

    static Stream<Arguments> testDatatype() {
        return Stream.of(
                arguments("<http://example.org/A>", null),
                arguments("<rel>", null),
                arguments("?x", null),
                arguments("_:label", null),
                arguments("[]", null),
                arguments("\"23\"^^<"+XSD.xint+">", XSD.xint),
                arguments("\"23\"^^<"+XSD.decimal+">", XSD.decimal),
                arguments("\"\"^^<"+XSD.string+">", XSD.string),
                arguments("\"@\"^^<"+XSD.string+">", XSD.string),
                arguments("\"^^<"+XSD.time+">\"^^<"+XSD.string+">", XSD.string),
                arguments("\"\"", XSD.string),
                arguments("\"\"@en", RDF.langString),
                arguments("\"asd\"@en", RDF.langString),
                arguments("\"asd\"@en-US", RDF.langString),
                arguments("\"\\\"\\\"^^<"+XSD.string+">\"@en-US", RDF.langString)
        );
    }
    @ParameterizedTest @MethodSource
    void testDatatype(String sparql, String expected) {
        assertEquals(expected, new Term(sparql).datatype());
    }

    static @NotNull Stream<Arguments> testType() {
        return Stream.of(
                arguments(null, ""),
                arguments(null, "^"),
                arguments(null, "23"),
                arguments(Term.Type.LITERAL, "\"\"@en"),
                arguments(Term.Type.LITERAL, "\"\""),
                arguments(Term.Type.LITERAL, "\"\"^^<"+XSD.string+">"),
                arguments(Term.Type.LITERAL, "\"23\"^^<"+XSD.xint+">"),
                arguments(Term.Type.URI, "<http://example.org/Alice>"),
                arguments(Term.Type.URI, "<relative>"),
                arguments(Term.Type.VAR, "?x"),
                arguments(Term.Type.BLANK, "_:label"),
                arguments(Term.Type.BLANK, "[]")
        );
    }
    @ParameterizedTest @MethodSource
    void testType(Term.Type type, @NotNull String sparql) {
        char first = sparql.isEmpty() ? '\0' : sparql.charAt(0);
        if (type == null)
            assertThrows(IllegalArgumentException.class, () -> Term.Type.fromNTFirstChar(first));
        else
            assertEquals(type, Term.Type.fromNTFirstChar(first));
        if (type != null) { // only create a Term if the string is valid
            Term t = new Term(sparql);
            assertEquals(type, t.type());
            switch (t.type()) {
                case VAR     -> assertTrue(!t.isGround() && t.isVar());
                case LITERAL -> assertTrue( t.isGround() && t.isLiteral());
                case BLANK   -> assertTrue(!t.isGround() && t.isBlank());
                case URI     -> assertTrue( t.isGround() && t.isURI());
            }
            assertEquals(t.isGround(), t.type().isGround());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "<http://example.org/A>",
            "<relative>",
            "\"\"",
            "\"\"@en",
            "\"\"@en-US",
            "\"\"^^<"+XSD.string+">",
            "\"test' asd\"",
            "\"test' asd\"@en",
            "\"test' asd\"@en-US",
            "\"test' asd\"^^<"+XSD.string+">",
            "\"23\"^^<"+XSD.decimal+">",
            "\"23\"^^<"+XSD.xdouble+">",
            "\"-23.0\"^^<"+XSD.decimal+">",
            "\"-23.0\"^^<"+XSD.xdouble+">",
            "\"false\"^^<"+XSD.xboolean+">",
            "_:name",
            "_:",
            "[]",
            "?x",
            "$x",
            "$X.1",
    })
    void testValid(String sparql) {
        assertTrue(new Term(sparql).isValid());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "http://example.org/A", // no wrapping <>
            "rel", // no wrapping <>
            "<bad><char>", // <> not allowed inside <>
            "23", // TTL style
            "false", // TTL stype
            "$", //var name cannot be empty
            "?", //var name cannot be empty
            "_", //incomplete blank
            "[", //incomplete blank
            "[]*", // extraneous suffix
            "\"asd\"@en^^<"+XSD.string+">", // typed lang string
            "\"asd\"@en_US" //malformed lang tag (should use - instead of _)
    })
    void testInvalid(String sparql) {
        assertFalse(new Term(sparql).isValid());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "true:\"bob\"",
            "true:\"bob\"^^<"+XSD.string+">",
            "true:\"23\"^^<"+XSD.string+">",
            "false:\"23\"^^<"+XSD.integer+">",
            "false:\"23\"^^<"+XSD.xint+">",
            "false:<"+XSD.string+">",
            "false:\"bob\"^^<"+RDF.langString+">", // actually invalid, should never be type like this
            "false:\"bob\"@en", // lang string is not a xsd:string
    })
    void testIsString(@NonNull String string) {
        String[] pieces = string.split(":", 2);
        boolean expected = Boolean.parseBoolean(pieces[0]);
        assertEquals(expected, new Term(pieces[1]).isStringLiteral());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "true:\"bob\"@en",
            "true:\"bob\"@en-US",
            "true:\"23\"@pt",
            "true:\"false\"@en-US",
            "false:\"bob\"",
            "false:\"bob\"^^<"+XSD.string+">",
            "false:\"23\"^^<"+XSD.string+">",
            "false:\"bob\"^^<"+RDF.langString+">",
            "false:<"+FOAF.Person+">",
    })
    void testIsLangString(@NonNull String string) {
        String[] pieces = string.split(":", 2);
        boolean expected = Boolean.parseBoolean(pieces[0]);
        assertEquals(expected, new Term(pieces[1]).isLangStringLiteral());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "\"bob\"!\"bob\"",
            "\"bob\"!\"bob\"@en",
            "\"joão\"!\"joão\"@pt-BR",
            "\"bob\"!\"bob\"^^<"+XSD.string+">",
            "\"23\"!\"23\"^^<"+XSD.string+">",
            "\"23.0\"!\"23.0\"^^<"+XSD.decimal+">",
            "\"23.0\"!\"23.0\"^^<"+XSD.xdouble+">",
            "<http://example.org/Alice>!<http://example.org/Alice>",
            "?x!?x",
            "$x1!$x1",
            "[]![]",
            "_:bn!_:bn",
    })
    void testQuoted(@NonNull String string) {
        String[] pieces = string.split("!", 2);
        assertEquals(pieces[0], new Term(pieces[1]).quoted());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "\"bob\"",
            "\"bob\"^^<"+XSD.string+">",
            "\"alice\"@en",
            "\""+XSD.string+"\"",
            "\""+XSD.string+"\"^^<"+XSD.string+">",
            "\"23\"^^<"+XSD.xint+">",
            "\"false\"^^<"+XSD.xboolean+">",
            "<http://example.org/Alice>",
            "?x",
            "$x",
            "$x1",
            "_:bn1",
            "[]",
    })
    void testWithExplicitString(@NonNull String string) {
        Term term = new Term(string);
        if (string.endsWith("^^<"+XSD.string+">")) {
            assertSame(term, term.withExplicitString());
            assertEquals(term.quoted(), term.withImplicitString().sparql());
        } else if (term.isStringLiteral()) {
            assertSame(term, term.withImplicitString());
            String expected = term.quoted() + "^^<" + XSD.string + ">";
            assertEquals(expected, term.withExplicitString().sparql());
        } else {
            assertSame(term, term.withImplicitString());
            assertSame(term, term.withExplicitString());
        }
    }
}