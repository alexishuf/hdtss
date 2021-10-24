package com.github.lapesd.hdtss.data.query.impl;

import com.github.lapesd.hdtss.TestUtils;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.TermPosition;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.github.lapesd.hdtss.vocab.FOAF;
import com.github.lapesd.hdtss.vocab.RDF;
import com.github.lapesd.hdtss.vocab.XSD;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.EX;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class HDTUtilsTest {
    private static HDT foaf;

    @BeforeAll
    static void beforeAll() throws IOException {
        foaf = TestUtils.openHDTResource(HDTUtilsTest.class, "../foaf-graph.hdt");
    }

    @AfterAll
    static void afterAll() throws IOException {
        foaf.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SUBJECT:1:<"+EX+"Alice>",
            "SUBJECT:1:<"+EX+"Bob>",
            "SUBJECT:1:<"+FOAF.knows+">", // both in S and P
            "SUBJECT:1:<"+EX+"Charlie>",
            "SUBJECT:-1:<"+EX+"Dave>", // not in SPO
            "SUBJECT:-1:<"+FOAF.name+">", // only in P
            "SUBJECT:-1:\"bob\"", // only in O
            "PREDICATE:1:<"+FOAF.name+">",
            "PREDICATE:1:<"+ RDF.type+">",
            "PREDICATE:1:<"+FOAF.knows+">", // both in S and P
            "PREDICATE:-1:<"+EX+"Alice>", // only in S
            "PREDICATE:-1:<"+FOAF.mbox+">", // not in SPO
            "OBJECT:1:<"+FOAF.Person+">",
            "OBJECT:1:\"bob\"", // as appears in TTL
            "OBJECT:1:\"bob\"^^<"+XSD.string+">", // with explicit type
            "OBJECT:1:\"Alice\"@en",
            "OBJECT:1:\"Alícia\"@pt-BR", // proper UTF-8 handling
            "OBJECT:1:\"23\"^^<"+XSD.integer+">", // with explicit type
            "OBJECT:1:\"25\"^^<"+XSD.integer+">", // as appears in TTL
            "OBJECT:1:\"charlie\"", // with implicit type
            "OBJECT:1:\"charlie\"^^<"+XSD.string+">", // as appears in TTL
            "OBJECT:1:<"+EX+"Alice>",  // both in S and O
            "OBJECT:-1:<"+FOAF.age+">",  // only in P
            "OBJECT:-1:<"+FOAF.knows+">",  // only in S and P
            "OBJECT:-1:<"+FOAF.mbox+">",  // not in SPO
            "OBJECT:-1:<"+FOAF.Document+">",  // not in SPO
            "SUBJECT:0:?x",
            "PREDICATE:0:?x",
            "OBJECT:0:?x",
            "SUBJECT:0:$whatever",
            "PREDICATE:0:?long.name123",
            "PREDICATE:0:$o2",
            "SUBJECT:0:[]",
            "PREDICATE:0:[]",
            "OBJECT:0:[]",
            "SUBJECT:0:_:x",
            "PREDICATE:0:_:x",
            "OBJECT:0:_:x",
            "SUBJECT:0:_:long-label",
            "PREDICATE:0:_:with_numbers123",
            "OBJECT:0:_:x.y",
    })
    public void testGetIDInRole(@NonNull String string) {
        var pieces = string.split(":", 3);
        var pos = TripleComponentRole.valueOf(pieces[0].toUpperCase());
        var expected = Long.parseLong(pieces[1]);
        var term = new Term(pieces[2]);
        long id = HDTUtils.toHDTId(foaf.getDictionary(), term, pos);
        if (expected != 1)
            assertEquals(expected, id);
        else
            assertTrue(id > 0, "id="+id+" should be > 0");
    }

    static @NonNull Stream<Arguments> testIdBijection() {
        List<Arguments> argumentsList = new ArrayList<>();
        for (var it = foaf.getTriples().searchAll(); it.hasNext(); ) {
            var triple = it.next();
            long s = triple.getSubject(), p = triple.getPredicate(), o = triple.getObject();
            var sStr = foaf.getDictionary().idToString(s, TripleComponentRole.SUBJECT).toString();
            var pStr = foaf.getDictionary().idToString(p, TripleComponentRole.PREDICATE).toString();
            var oStr = foaf.getDictionary().idToString(o, TripleComponentRole.OBJECT).toString();
            argumentsList.add(arguments(HDTUtils.fromHDT(sStr), s, TermPosition.SUB));
            argumentsList.add(arguments(HDTUtils.fromHDT(pStr), p, TermPosition.PRE));
            argumentsList.add(arguments(HDTUtils.fromHDT(oStr), o, TermPosition.OBJ));
        }
        return argumentsList.stream();
    }

    @ParameterizedTest
    @MethodSource
    public void testIdBijection(@NonNull Term term, long id, @NonNull TermPosition position) {
        //Some CharSequence implementations inside HDT have buggy equals
        //see https://github.com/rdfhdt/hdt-java/pull/117
        Term withBadEquals = HDTUtils.fromHDTId(foaf.getDictionary(), id, position);
        Term fromId = new Term(withBadEquals.sparql().toString());
        assertEquals(term, fromId);
        long fromTerm = HDTUtils.toHDTId(foaf.getDictionary(), term, HDTUtils.pos2role(position));
        assertEquals(id, fromTerm);
    }

    private static @NonNull Stream<Arguments> querySolutionCountArguments() {
        return Stream.of(
                "1:<"+EX+"Alice> <"+FOAF.knows+"> <"+EX+"Bob>",
                "1:<"+EX+"Alice> <"+FOAF.knows+"> ?x",
                "1:<"+EX+"Alice> <"+FOAF.knows+"> $y2",
                "2:<"+EX+"Bob> <"+FOAF.knows+"> ?x",
                "2:<"+EX+"Bob> <"+FOAF.knows+"> ?x",
                "2:?x <"+RDF.type+"> <"+FOAF.Person+">",
                "5:?x <"+FOAF.name+"> ?name",
                "4:?x <"+FOAF.knows+"> ?x",
                "1:?x <"+FOAF.name+"> \"bob\"",
                "1:?x <"+FOAF.name+"> \"bob\"^^<"+XSD.string+">",
                "1:?x <"+FOAF.name+"> \"roberto\"@pt",
                "1:?x <"+FOAF.name+"> \"Alícia\"@pt-BR",
                "1:?x ?p \"Alícia\"@pt-BR",
                "0:?x ?p \"Alícia\"@en",
                "0:<"+EX+"Alice> <"+FOAF.knows+"> <"+EX+"Alice>",
                "0:<"+EX+"Alice> <"+FOAF.knows+"> <"+EX+"Charlie>",
                "0:?x <"+FOAF.knows+"> <"+EX+"Charlie>",
                "1:<"+EX+"Charlie> <"+FOAF.knows+"> <"+EX+"Alice>",
                "2:?x <"+FOAF.knows+"> <"+EX+"Alice>"
        ).map(s -> {
            String[] pieces = s.split(":", 2);
            var terms = Arrays.stream(pieces[1].split(" ", 3))
                              .map(Term::new).collect(Collectors.toList());
            return arguments(Integer.parseInt(pieces[0]),
                             new TriplePattern(terms.get(0), terms.get(1), terms.get(2)));
        });
    }

    @ParameterizedTest
    @MethodSource("querySolutionCountArguments")
    public void testQueryIds(int expected, @NonNull TriplePattern query) {
        IteratorTripleID idIterator = HDTUtils.queryIds(foaf, query);
        int count = 0;
        while (idIterator.hasNext()) {
            ++count;
            TripleID tripleID = idIterator.next();
            assertTrue(tripleID.getSubject() > 0, "subject should be > 0");
            assertTrue(tripleID.getPredicate() > 0, "predicate should be > 0");
            assertTrue(tripleID.getObject() > 0, "object should be > 0");
        }
        assertEquals(expected, count);
    }

    @ParameterizedTest
    @MethodSource("querySolutionCountArguments")
    public void testQueryStrings(int expected, @NonNull TriplePattern query) {
        IteratorTripleString stringIterator = HDTUtils.query(foaf, query);
        int count = 0;
        while (stringIterator.hasNext()) {
            ++count;
            TripleString ts = stringIterator.next();
            assertFalse(ts.getSubject().isEmpty(), "subject should be non-empty");
            assertFalse(ts.getPredicate().isEmpty(), "predicate should be non-empty");
            assertFalse(ts.getObject().isEmpty(), "object should be non-empty");
            for (TermPosition pos : TermPosition.values()) {
                Term qTerm = query.get(pos);
                CharSequence tTerm = HDTUtils.get(ts, pos);
                if (qTerm.isLiteral())
                    assertEquals(qTerm.withImplicitString().withImplicitString().sparql(), tTerm);
                else if (qTerm.isURI())
                    assertEquals(qTerm.content(), tTerm);
                else
                    assertFalse(tTerm.isEmpty());
            }
        }
        assertEquals(expected, count);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "true:NONE:<"+EX+"Alice> <"+FOAF.name+"> <"+EX+"Bob>",
            "false:SO:<"+EX+"Alice> <"+FOAF.name+"> <"+EX+"Bob>",
            "true:SO:<"+EX+"Alice> <"+FOAF.name+"> <"+EX+"Alice>",
            "false:SP:<"+EX+"Alice> <"+FOAF.knows+"> <"+EX+"Alice>",
            "true:SP:<"+FOAF.knows+"> <"+FOAF.knows+"> <"+EX+"Alice>",
            "false:PO:<"+EX+"Alice> <"+FOAF.knows+"> <"+EX+"Alice>",
            "true:PO:<"+EX+"Alice> <"+FOAF.knows+"> <"+FOAF.knows+">",
            "false:ALL:<"+FOAF.knows+"> <"+FOAF.name+"> <"+FOAF.knows+">",
            "false:ALL:<"+EX+"Alice> <"+FOAF.knows+"> <"+FOAF.knows+">",
            "true:ALL:<"+FOAF.knows+"> <"+FOAF.knows+"> <"+FOAF.knows+">"
    })
    void testSharedVarStringFilter(@NonNull String testData) {
        String[] args = testData.split(":", 3);
        boolean expected = Boolean.parseBoolean(args[0]);
        var sv = TriplePattern.SharedVars.valueOf(args[1]);
        var tp = TestUtils.parseTripleString(args[2]);
        assertEquals(expected, HDTUtils.sharedVarStringFilter(sv).test(tp));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "true:NONE:1 2 3",
            "false:SO:1 2 3",
            "true:SO:1 2 1",
            "false:SP:1 2 1",
            "true:SP:1 1 3",
            "false:PO:1 2 1",
            "true:PO:1 3 3",
            "false:ALL:1 1 3",
            "true:ALL:1 1 1",
    })
    void testSharedVarIdFilter(@NonNull String testData) {
        String[] args = testData.split(":", 3);
        boolean expected = Boolean.parseBoolean(args[0]);
        var sv = TriplePattern.SharedVars.valueOf(args[1]);
        var tripleID = TestUtils.parseTripleID(args[2]);
        assertEquals(expected, HDTUtils.sharedVarIDFilter(sv).test(tripleID));
    }
}