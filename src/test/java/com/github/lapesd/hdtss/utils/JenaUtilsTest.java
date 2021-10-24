package com.github.lapesd.hdtss.utils;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.github.lapesd.hdtss.vocab.FOAF;
import com.github.lapesd.hdtss.vocab.XSD;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class JenaUtilsTest {
    static Stream<Arguments> termAndNodes() {
        return Stream.of(
                arguments(null, null),
                arguments(FOAF.knowsTerm, org.apache.jena.sparql.vocabulary.FOAF.knows.asNode()),
                arguments(i23, NodeFactory.createLiteral("23", XSDDatatype.XSDinteger)),
                arguments(bobString, NodeFactory.createLiteral("bob")),
                arguments(AliceEN, NodeFactory.createLiteral("Alice", "en")),
                arguments(x, NodeFactory.createVariable("x")),
                arguments(new Term("_:label"), NodeFactory.createBlankNode("label")),
                arguments(new Term("\"\\\"quoted\\\"\"^^<"+XSD.string+">"),
                          NodeFactory.createLiteral("\\\"quoted\\\"", XSDDatatype.XSDstring))
        );
    }

    @ParameterizedTest @MethodSource("termAndNodes")
    public void testToNode(@NonNull Term term, @NonNull Node node) {
        assertEquals(node, JenaUtils.toNode(term));
    }

    @ParameterizedTest @MethodSource("termAndNodes")
    public void testFromNode(@NonNull Term term, @NonNull Node node) {
        assertEquals(term, JenaUtils.fromNode(node));
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
                          new TriplePattern(Alice, FOAF.nameTerm, bobString))
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
                arguments(b, asList("x", "y", "w"), SolutionRow.of(Alice, Bob, Charlie)),
                arguments(b, asList("w", "x", "y"), SolutionRow.of(Charlie, Alice, Bob)),
                arguments(b, List.of("x"), SolutionRow.of(Alice)),
                arguments(b, asList("y", "w"), SolutionRow.of(Bob, Charlie)),
                arguments(b, asList("x", "y", "z"), SolutionRow.of(Alice, Bob, null))
        );
    }

    @ParameterizedTest @MethodSource("bindingsAndRows")
    public void testFromBinding(@NonNull Binding binding, @NonNull List<@NonNull String> varNames,
                                @NonNull SolutionRow row) {
        assertEquals(row, JenaUtils.fromBinding(varNames, binding));
        assertArrayEquals(row.terms(), JenaUtils.fromBindingToArray(varNames, binding));
    }

    @ParameterizedTest @MethodSource("bindingsAndRows")
    public void testToBinding(@NonNull Binding binding, @NonNull List<@NonNull String> varNames,
                              @NonNull SolutionRow row) {
        RowJenaBinding converted = JenaUtils.toBinding(row, varNames);

        List<@NonNull Var> acVars = new ArrayList<>();
        for (var it = converted.vars(); it.hasNext(); ) acVars.add(it.next());
        assertEquals(varNames.stream().map(Var::alloc).collect(Collectors.toList()), acVars);

        List<Node> exValues = acVars.stream().map(binding::get).collect(Collectors.toList());
        List<Node> acValues = acVars.stream().map(converted::get).collect(Collectors.toList());
        assertEquals(exValues, acValues);
    }
}