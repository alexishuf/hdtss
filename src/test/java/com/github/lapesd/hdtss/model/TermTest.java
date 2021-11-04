package com.github.lapesd.hdtss.model;

import com.github.lapesd.hdtss.TestUtils;
import com.github.lapesd.hdtss.vocab.FOAF;
import com.github.lapesd.hdtss.vocab.RDF;
import com.github.lapesd.hdtss.vocab.XSD;
import org.apache.commons.io.IOUtils;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.CollectorStreamRDF;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.EX;
import static com.github.lapesd.hdtss.model.LiteralQuote.*;
import static java.util.Arrays.asList;
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
            "alice!\"alice\"^^xsd:string",
            "23!\"23\"^^xsd:integer",
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

    static Stream<Arguments> escapeData() throws IOException {
        Map<String, String> s2o = new HashMap<>();
        try (var in = TestUtils.openResource(TermTest.class, "../data/literals.ttl")) {
            for (String line : IOUtils.readLines(in, StandardCharsets.UTF_8)) {
                if (line.startsWith(":")) {
                    String[] terms = line.split(" +", 3);
                    String s = EX + terms[0].split(":")[1];
                    String o = terms[2].replaceAll("\\s*.\\s*$", "");
                    if (o.startsWith("\"") && !o.startsWith("\"\"\""))
                        s2o.put(s, o);
                }
            }
        }
        List<Arguments> literals = new ArrayList<>();
        try (var in = TestUtils.openResource(TermTest.class, "../data/literals.ttl")) {
            CollectorStreamRDF sink = new CollectorStreamRDF();
            RDFDataMgr.parse(sink, in, Lang.TTL);
            for (Triple triple : sink.getTriples()) {
                String nt = s2o.getOrDefault(triple.getSubject().toString(), null);
                if (nt == null)
                    continue;
                String unescaped = triple.getObject().getLiteralLexicalForm();
                literals.add(arguments(new Term(nt), unescaped));
            }
        }
        return Stream.concat(
                Stream.of(
                        arguments(new Term("<a>"), "a"),
                        arguments(new Term("<http://example.org/Alice>"), "http://example.org/Alice"),
                        arguments(new Term("_:bnode"), "bnode"),
                        arguments(new Term("?x"), "x"),
                        arguments(new Term("[]"), ""),
                        arguments(new Term("\"a\\\\\""), "a\\"),
                        arguments(new Term("\"a\\n\""), "a\n"),
                        arguments(new Term("\"a\\t\""), "a\t"),
                        arguments(new Term("\"\\na\""), "\na"),
                        arguments(new Term("\"\\n\""), "\n"),
                        arguments(new Term("\"\\\\\""), "\\")
                ),
                literals.stream()
        );
    }

    @ParameterizedTest @MethodSource("escapeData")
    void testUnescapedContent(Term term, String unescaped) {
        assertDoesNotThrow(term::type);
        assertDoesNotThrow(term::content);
        assertDoesNotThrow(term::datatype);
        assertDoesNotThrow(term::lang);
        assertDoesNotThrow(term::langAsString);
        assertEquals(unescaped, term.unescapedContent().toString());
    }

    @ParameterizedTest @ValueSource(strings = {
            "#[]",
            "x#?x",
            "?x#?x",
            "$x#$x",
            "$_123xyz#$_123xyz",
    })
    void testFromVar(String data) {
        String[] pieces = data.split("#", 2);
        assertEquals(new Term(pieces[1]), Term.fromVar(pieces[0]));
    }

    @Test
    void testFromVarNull() {
        assertNull(Term.fromVar(null));
    }

    @ParameterizedTest @ValueSource(strings = {
            "#[]",
            "[]#[]",
            "_:x#_:x",
            "_#_:_",
            "_:#[]",
            "123zxc#_:123zxc",
    })
    void testFromBlank(String data) {
        String[] parts = data.split("#", 2);
        assertEquals(new Term(parts[1]), Term.fromBlank(parts[0]));
    }

    @Test
    void testFromBlankNull() {
        assertNull(Term.fromBlank(null));
    }

    @ParameterizedTest @ValueSource(strings = {
            " | <>",
            "a | <a>",
            "<a> | <a>",
            "<a>> | <a%3E>",
            "<>> | <%3E>",
            "<http://example.org/Alice> | <http://example.org/Alice>",
            "<http://example.org/Bob Smith> | <http://example.org/Bob%20Smith>",
            "<http://example.org/Bob`Smith> | <http://example.org/Bob%60Smith>",
    })
    void testFromURI(String data) {
        String[] pieces =  data.split(" \\| ", 2);
        assertEquals(new Term(pieces[1]), Term.fromURI(pieces[0]).withString());
    }

    @Test
    void testFromURINull() {
        assertNull(Term.fromURI(null));
    }

     static Stream<Arguments> testFromLiteral() {
        String type = "^^<" + XSD.string + ">";
        List<Arguments> argumentsList = new ArrayList<>();

        /* multiple param variations for simple strings without escapes */
        ArrayList<@Nullable LiteralQuote> quoteValues = new ArrayList<>(asList(values()));
        quoteValues.add(null);
        for (Boolean escaped : asList(false, true)) {
            for (LiteralQuote quote : quoteValues) {
                for (String string : asList("bob", "")) {
                    String nt = "\"bob\"";
                    String input = quote == null ? "bob" : quote+"bob"+quote;
                    argumentsList.add(arguments(input, quote, escaped, new Term(nt)));
                    if (quote != null) {
                        argumentsList.add(arguments(input+type, quote, escaped,
                                                    new Term(nt+type)));
                        argumentsList.add(arguments(input+"^^xsd:string", quote, escaped,
                                                    new Term(nt+type)));
                        argumentsList.add(arguments(input+"@en", quote, escaped,
                                                    new Term(nt+"@en")));
                        argumentsList.add(arguments(input+"@pt-BR", quote, escaped,
                                                    new Term(nt+"@pt-BR")));
                    }
                }
            }
        }
        List<Arguments> tempList = asList(
                /* single unescaped " */
                arguments("\"\"\"", DOUBLE, false, new Term("\"\\\"\"")),
                arguments("'\"'", SINGLE, false, new Term("\"\\\"\"")),
                arguments("\"\"\"\"\"\"\"", LONG_DOUBLE, false, new Term("\"\\\"\"")),
                arguments("'''\"'''", LONG_SINGLE, false, new Term("\"\\\"\"")),

                /* single always escaped " */
                arguments("\"\\\"\"", DOUBLE, true, new Term("\"\\\"\"")),
                arguments("'\\\"'", SINGLE, true, new Term("\"\\\"\"")),
                arguments("\"\"\"\\\"\"\"\"", LONG_DOUBLE, true, new Term("\"\\\"\"")),
                arguments("'''\\\"'''", LONG_SINGLE, true, new Term("\"\\\"\"")),

                /* single unescaped ' */
                arguments("\"'\"", DOUBLE, false, new Term("\"'\"")),
                arguments("'''", SINGLE, false, new Term("\"'\"")),
                arguments("\"\"\"'\"\"\"", LONG_DOUBLE, false, new Term("\"'\"")),
                arguments("'''''''", LONG_SINGLE, false, new Term("\"'\"")),

                /* single always escaped ' */
                arguments("\"\\'\"", DOUBLE, true, new Term("\"\\'\"")), // performance: do not convert
                arguments("'\\''", SINGLE, true, new Term("\"'\"")),
                arguments("\"\"\"\\'\"\"\"", LONG_DOUBLE, true, new Term("\"'\"")),
                arguments("'''\\''''", LONG_SINGLE, true, new Term("\"'\"")),

                /* all problem chars unescaped */
                arguments("\"\n\r\t\\'\"\"", DOUBLE, false,
                          new Term("\"\\n\\r\t\\\\'\\\"\"")),
                arguments("\"\"\"\n\r\t\\'\"\"\"\"", LONG_DOUBLE, false,
                          new Term("\"\\n\\r\t\\\\'\\\"\"")),
                arguments("'\n\r\t\\'\"'", SINGLE, false,
                          new Term("\"\\n\\r\t\\\\'\\\"\"")),
                arguments("'''\n\r\t\\'\"'''", LONG_SINGLE, false,
                          new Term("\"\\n\\r\t\\\\'\\\"\"")),

                /* all problem chars escaped */
                arguments("\"\\n\\r\\t\\\\\\'\\\"\"", DOUBLE, true,
                          new Term("\"\\n\\r\\t\\\\\\'\\\"\"")), //do not un-escape ' and \t for preformance
                arguments("\"\"\"\\n\\r\\t\\\\\\'\\\"\"\"\"", LONG_DOUBLE, true,
                          new Term("\"\\n\\r\t\\\\'\\\"\"")),
                arguments("'\\n\\r\\t\\\\\\'\\\"'", SINGLE, true,
                          new Term("\"\\n\\r\t\\\\'\\\"\"")),
                arguments("'''\\n\\r\\t\\\\\\'\\\"'''", LONG_SINGLE, true,
                          new Term("\"\\n\\r\t\\\\'\\\"\""))
        );
        argumentsList.addAll(tempList);
        argumentsList.addAll(tempList.stream().map(a ->
                arguments(a.get()[0].toString()+"@en", a.get()[1], a.get()[2],
                          new Term(a.get()[3].toString()+"@en"))).toList());
        argumentsList.addAll(tempList.stream().map(a ->
                arguments(a.get()[0].toString()+"@pt-BR", a.get()[1], a.get()[2],
                        new Term(a.get()[3].toString()+"@pt-BR"))).toList());
        argumentsList.addAll(tempList.stream().map(a ->
                arguments(a.get()[0].toString()+"^^<"+XSD.NS+"string>", a.get()[1], a.get()[2],
                        new Term(a.get()[3].toString()+"^^<"+XSD.NS+"string>"))).toList());
        argumentsList.addAll(tempList.stream().map(a ->
                arguments(a.get()[0].toString()+"^^xsd:string", a.get()[1], a.get()[2],
                        new Term(a.get()[3].toString()+"^^<"+XSD.NS+"string>"))).toList());
        return argumentsList.stream();
    }

    @ParameterizedTest @MethodSource
    void testFromLiteral(CharSequence literal, LiteralQuote quote, Boolean escaped, Term expected) {
        assertEquals(expected, Term.fromLiteral(literal, quote, escaped).withString());
    }

    static Stream<Arguments> testFromHDTLiteral() {
        // test required chars are escaped
        List<Arguments> argumentsList = new ArrayList<>(asList(
                arguments("\".\".\"", new Term("\".\\\".\"")),
                arguments("\".\n.\"", new Term("\".\\n.\"")),
                arguments("\".\r.\"", new Term("\".\\r.\"")),
                arguments("\".\\.\"", new Term("\".\\\\.\"")),

                arguments("\".\"\"", new Term("\".\\\"\"")),
                arguments("\".\n\"", new Term("\".\\n\"")),
                arguments("\".\r\"", new Term("\".\\r\"")),
                arguments("\".\\\"", new Term("\".\\\\\"")),

                arguments("\"\".\"", new Term("\"\\\".\"")),
                arguments("\"\n.\"", new Term("\"\\n.\"")),
                arguments("\"\r.\"", new Term("\"\\r.\"")),
                arguments("\"\\.\"", new Term("\"\\\\.\"")),

                arguments("\"\"\"", new Term("\"\\\"\"")),
                arguments("\"\n\"", new Term("\"\\n\"")),
                arguments("\"\r\"", new Term("\"\\r\"")),
                arguments("\"\\\"", new Term("\"\\\\\""))
        ));
        // process literals not needing escapes
        Stream.of(
                "\"\"", "\"bob\"", "\"'\"",
                "\"\"@en", "\"bob\"@en", "\"'\"@en",
                "\"\"@pt-BR", "\"bob\"@pt-BR", "\"'\"@pt-BR",
                "\"\"^^<"+XSD.NS+"string>", "\"bob\"^^<"+XSD.NS+"string>", "\"'\"^^<"+XSD.NS+"string>"
        ).map(s -> arguments(s, new Term(s))).forEach(argumentsList::add);
        // pull test cases from testFromHDTLiteral
        testFromLiteral()
                .filter(a -> !(Boolean) a.get()[2]
                        && a.get()[1] == DOUBLE
                        && !a.get()[0].toString().endsWith("xsd:string"))
                .map(a -> arguments(a.get()[0], a.get()[3]))
                .forEach(argumentsList::add);
        return argumentsList.stream();
    }

    @ParameterizedTest @MethodSource
    void testFromHDTLiteral(CharSequence literal, @NonNull Term expected) {
        Term actual = Term.fromNonXSDAbbrevDoubleQuotedUnescapedLiteral(literal);
        assertEquals(expected, actual.withString());
        assertEquals(expected, actual.withString()); //withString does not invalidate actual
        if (expected.sparql().toString().equals(literal.toString()))
            assertSame(actual.sparql(), literal);
    }

    @ParameterizedTest @ValueSource(strings = {
            "\"\"                        | 1",
            "\"asd\"                     | 4",
            "\"\"\"                      | 2",
            "\"'''\"                     | 4",
            "\"\"@en                     | 1",
            "\"asd\"@en                  | 4",
            "\"\"\"@en                   | 2",
            "\"'''\"@en                  | 4",
            "\"\"@pt-BR                  | 1",
            "\"asd\"@pt-BR               | 4",
            "\"\"\"@pt-BR                | 2",
            "\"'''\"@pt-BR               | 4",
            "\"\"^^<"+XSD.NS+"string>    | 1",
            "\"asd\"^^<"+XSD.NS+"string> | 4",
            "\"\"\"^^<"+XSD.NS+"string>  | 2",
            "\"'''\"^^<"+XSD.NS+"string> | 4",
            "\"\"^^xsd:string            | 1",
            "\"asd\"^^xsd:string         | 4",
            "\"\"\"^^xsd:string          | 2",
            "\"'''\"^^xsd:string         | 4",
    })
    void testFindCloseIdx(String data) {
        String[] args = data.split(" +\\| +", 2);
        LiteralQuote quote = fromLiteral(args[0]);
        assertEquals(Integer.parseInt(args[1]), Term.findCloseIdx(args[0], quote));
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
                arguments("\"\\\"\\\"^^<"+XSD.string+">\"@en-US", RDF.langString),
                arguments("\"alice\"^^xsd:string", XSD.string),
                arguments("\"23\"^^xsd:integer", XSD.integer)
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