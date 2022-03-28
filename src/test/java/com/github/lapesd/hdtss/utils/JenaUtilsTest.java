package com.github.lapesd.hdtss.utils;

import com.github.lapesd.hdtss.TestUtils;
import com.github.lapesd.hdtss.model.Row;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.github.lapesd.hdtss.vocab.FOAF;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.CollectorStreamRDF;
import org.apache.jena.riot.writer.NTriplesWriter;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@SuppressWarnings("unused")
@Tag("fast")
class JenaUtilsTest {
    static Stream<Arguments> termAndNodes() throws IOException {
        Stream<Node> literals;
        try (var in = TestUtils.openResource(JenaUtils.class, "../data/literals.ttl")) {
            CollectorStreamRDF sink = new CollectorStreamRDF();
            RDFDataMgr.parse(sink, in , Lang.TTL);
            literals = sink.getTriples().stream().map(Triple::getObject);
        }
        Stream<Arguments> handWritten = Stream.of(
                arguments(null, null),
                arguments(FOAF.knowsTerm, org.apache.jena.sparql.vocabulary.FOAF.knows.asNode()),
                arguments(i23, NodeFactory.createLiteral("23", XSDDatatype.XSDinteger)),
                arguments(bob, NodeFactory.createLiteral("bob")),
                arguments(AliceEN, NodeFactory.createLiteral("Alice", "en")),
                arguments(x, NodeFactory.createVariable("x")),
                arguments(new Term("_:label"), NodeFactory.createBlankNode("label")),
                arguments(new Term("\"\\\"quoted\\\"\""),
                        NodeFactory.createLiteral("\"quoted\"", XSDDatatype.XSDstring))
        );
        List<Arguments> automated = literals.map(n -> {
            ByteArrayOutputStream bOS = new ByteArrayOutputStream();
            Node uri = NodeFactory.createURI("a");
            NTriplesWriter.write(bOS, List.of(new Triple(uri, uri, n)).iterator());
            var nt = bOS.toString(UTF_8).replaceAll("^<a> <a> ", "").replaceAll("\\s*.\\s*$", "");
            return arguments(new Term(nt), n);
        }).toList();
        return Stream.concat(handWritten, automated.stream());
    }

    @ParameterizedTest @MethodSource("termAndNodes")
    public void testToNode(@NonNull Term term, @NonNull Node node) {
        assertEquals(node, JenaUtils.toNode(term));
    }

    @ParameterizedTest @MethodSource("termAndNodes")
    public void testFromNode(@NonNull Term term, @NonNull Node node) {
        assertEquals(term, JenaUtils.fromNode(node));
    }

    @ParameterizedTest @MethodSource("termAndNodes")
    public void testFromNodeAndBackToTerm(@NonNull Term ignored, @NonNull Node node) {
        assertEquals(node, JenaUtils.toNode(JenaUtils.fromNode(node)));
    }

    @ParameterizedTest @MethodSource("termAndNodes")
    public void testFromTermAndBackToTerm(@NonNull Term term, @NonNull Node ignored) {
        assertEquals(term, JenaUtils.fromNode(JenaUtils.toNode(term)));
    }

    @Test
    public void testJenaUnescapes() throws IOException {
        Model model = ModelFactory.createDefaultModel();
        try (var in = TestUtils.openResource(getClass(), "../data/literals.ttl")) {
            RDFDataMgr.read(model, in, Lang.TTL);
        }
        Property name = ResourceFactory.createProperty(FOAF.name);
        Function<String, String> get = ln -> model.createResource(EX+ln)
                .getRequiredProperty(name).getLiteral().getLexicalForm();

        assertEquals("word-dquote", get.apply("a1"));
        assertEquals("word-long-squote-pt-BR", get.apply("a16"));

        assertEquals("dquote\"quoted", get.apply("b1"));
        assertEquals("squote'quoted", get.apply("b5"));
        assertEquals("long-dquote\"quoted", get.apply("b9"));
        assertEquals("long-squote'quoted", get.apply("b13"));

        assertEquals("\"dquote\"", get.apply("c1"));
        assertEquals("'squote-pt-BR'", get.apply("c8"));
        assertEquals("'squote'", get.apply("c5"));
        assertEquals("\"long-dquote\"", get.apply("c9"));
        assertEquals("'long-squote'", get.apply("c13"));

        assertEquals("çãéíú° dquote plain ßẞ¹²³£¢¬«»©“”←µ", get.apply("d1"));
        assertEquals("çãéíú° squote pt-BR ßẞ¹²³£¢¬«»©“”←µ", get.apply("d16"));

        assertEquals("dquote\nplain", get.apply("e1"));
        assertEquals("dquote\ntyped", get.apply("e2"));
        assertEquals("dquote\nen", get.apply("e3"));
        assertEquals("dquote\npt-BR", get.apply("e4"));
        assertEquals("squote\nplain", get.apply("e5"));
        assertEquals("squote\npt-BR", get.apply("e8"));
        assertEquals("long-dquote\nplain", get.apply("e9"));
        assertEquals("long-dquote\nen", get.apply("e11"));
        assertEquals("long-squote\ntyped", get.apply("e14"));
        assertEquals("long-squote\nen", get.apply("e15"));

        for (int i = 1; i <= 16; i++)
            assertEquals(".", get.apply("f"+i));

        for (int i : Stream.concat(range(1, 5).boxed(), range(9, 13).boxed()).toList())
            assertEquals("\"", get.apply("g"+i));
        for (int i : Stream.concat(range(5, 9).boxed(), range(13, 17).boxed()).toList())
            assertEquals("'", get.apply("g"+i));

        for (int i = 1; i <= 12; i++)
            assertEquals("<>?x='%20'&param[]=\"${^}\"#", get.apply("h"+i));
    }

    static Stream<Arguments> triplesAndTriplePatterns() {
        return Stream.of(
                arguments(new Triple(NodeFactory.createVariable("x"),
                                     org.apache.jena.sparql.vocabulary.FOAF.age.asNode(),
                                     NodeFactory.createLiteral("23", XSDDatatype.XSDinteger)),
                          new TriplePattern(x, FOAF.ageTerm, i23)),
                arguments(new Triple(NodeFactory.createBlankNode("label"),
                                     org.apache.jena.sparql.vocabulary.FOAF.name.asNode(),
                                     NodeFactory.createLiteral("Alice", "en")),
                          new TriplePattern(new Term("_:label"), FOAF.nameTerm, AliceEN)),
                arguments(new Triple(NodeFactory.createURI(Alice.content().toString()),
                                     org.apache.jena.sparql.vocabulary.FOAF.name.asNode(),
                                     NodeFactory.createLiteral("bob", XSDDatatype.XSDstring)),
                          new TriplePattern(Alice, FOAF.nameTerm, bob))
        );
    }

    @ParameterizedTest @MethodSource("triplesAndTriplePatterns")
    public void testFromTriple(@NonNull Triple triple, @NonNull TriplePattern tp) {
        assertEquals(tp, JenaUtils.fromTriple(triple));
    }

    @ParameterizedTest @MethodSource("triplesAndTriplePatterns")
    public void testToTriple(@NonNull Triple triple, @NonNull TriplePattern tp) {
        assertEquals(triple, JenaUtils.toTriple(tp));
    }

    @ParameterizedTest @MethodSource("triplesAndTriplePatterns")
    public void testToTripleAndBack(@NonNull Triple ignored, @NonNull TriplePattern tp) {
        assertEquals(tp, JenaUtils.fromTriple(JenaUtils.toTriple(tp)));
    }

    @ParameterizedTest @MethodSource("triplesAndTriplePatterns")
    public void testToTriplePatternAndBack(@NonNull Triple triple, @NonNull TriplePattern ignored) {
        assertEquals(triple, JenaUtils.toTriple(JenaUtils.fromTriple(triple)));
    }

    static Stream<Arguments> testParseFilter() {
        return Stream.of(
                arguments("?x > ?y", "?x > ?y"),
                arguments("?x = ?y", "?x = ?y"),
                arguments("(?x > ?y)", "?x > ?y"),
                arguments("?x > ?y && ?x < ?y", "(?x > ?y) && (?x < ?y)"),
                arguments("regex(?x, \".*s$\")", "regex(?x, \".*s$\")"),
                arguments("!regex(?x, \".*s$\")", "!regex(?x, \".*s$\")"),
                arguments("!(regex(?x, \".*s$\"))", "!regex(?x, \".*s$\")")
        );
    }

    @ParameterizedTest @MethodSource
    public void testParseFilter(@NonNull String filter, @NonNull String expected) {
        assertEquals(expected, JenaUtils.toSPARQL(JenaUtils.parseFilter(filter)));
        assertEquals(expected, JenaUtils.toSPARQL(JenaUtils.parseFilter("FILTER("+filter+")")));
    }

    public static Stream<Arguments> bindingsAndRows() {
        BindingHashMap b = new BindingHashMap();
        b.add(Var.alloc("x"), JenaUtils.toNode(Alice));
        b.add(Var.alloc("y"), JenaUtils.toNode(Bob));
        b.add(Var.alloc("w"), JenaUtils.toNode(Charlie));
        return Stream.of(
                arguments(b, asList("x", "y", "w"), Row.raw(Alice, Bob, Charlie)),
                arguments(b, asList("w", "x", "y"), Row.raw(Charlie, Alice, Bob)),
                arguments(b, List.of("x"), Row.raw(Alice)),
                arguments(b, asList("y", "w"), Row.raw(Bob, Charlie)),
                arguments(b, asList("x", "y", "z"), Row.raw(Alice, Bob, null))
        );
    }

    @ParameterizedTest @MethodSource("bindingsAndRows")
    public void testFromBinding(@NonNull Binding binding, @NonNull List<@NonNull String> varNames,
                                @Nullable Term @NonNull[] row) {
        assertArrayEquals(row, JenaUtils.fromBinding(varNames, binding));
    }

    @ParameterizedTest @MethodSource("bindingsAndRows")
    public void testToBinding(@NonNull Binding binding, @NonNull List<@NonNull String> varNames,
                              @Nullable Term @NonNull[] row) {
        RowJenaBinding converted = JenaUtils.toBinding(row, varNames);

        List<@NonNull Var> acVars = new ArrayList<>();
        for (var it = converted.vars(); it.hasNext(); ) acVars.add(it.next());
        assertEquals(varNames.stream().map(Var::alloc).collect(Collectors.toList()), acVars);

        List<Node> exValues = acVars.stream().map(binding::get).collect(Collectors.toList());
        List<Node> acValues = acVars.stream().map(converted::get).collect(Collectors.toList());
        assertEquals(exValues, acValues);
    }
}