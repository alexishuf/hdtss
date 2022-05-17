package com.github.lapesd.hdtss.controller.websocket;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class MessageParserTest {
    @SuppressWarnings("unused") static Stream<Arguments> testParseVars() {
        record D(@NonNull String input, @Nullable List<String> vars) {}
        List<D> base = List.of(
                new D("?x\n", List.of("x")),
                new D("?x\t?y\n", List.of("x", "y")),
                new D("?x1\t?y\n", List.of("x1", "y")),
                new D("?x1\t?y1\n", List.of("x1", "y1")),
                new D("?x1\t?y1\t?z\n", List.of("x1", "y1", "z")),
                new D("?x1\t?y1\t?z\n\n", List.of("x1", "y1", "z")),
                new D("?x\t?y", null), // missing EOL
                new D("?x", null), // missing EOL
                new D("x\ty\n", null), // missing ?
                new D("?x\ty\n", null), // missing ?
                new D("x\t?y\n", null) // missing ?
        );
        ArrayList<Arguments> rows = new ArrayList<>();
        for (D d : base)
            rows.add(arguments(d.input, 0, d.vars));
        for (String prefix : List.of(".", "?\t\n", "\n\t?x\t\n")) {
            for (D d : base) {
                int len = prefix.length();
                rows.add(arguments(prefix+d.input, len, d.vars));
                if (d.input.endsWith("\n"))
                    rows.add(arguments(prefix+d.input+prefix, len, d.vars));
            }
        }
        return rows.stream();
    }

    @ParameterizedTest @MethodSource
    void testParseVars(@NonNull String msg, int begin, @Nullable List<String> expected) {
        try {
            List<String> actual = MessageParser.parseVars(msg, begin);
            if (expected == null)
                fail("Expected ProtocolException");
            assertEquals(expected, actual);
        } catch (ProtocolException e) {
            if (expected != null)
                fail("Unexpected ProtocolException", e);
        }
    }

    @SuppressWarnings("unused") static Stream<Arguments> testParseTerms() {
        record D(String input, int columns, @Nullable List<@Nullable Term> expected) {}
        List<D> base = List.of(
                new D("\"\"\n", 1, List.of(new Term("\"\""))),
                new D("<a>\n", 1, List.of(new Term("<a>"))),
                new D("<a>\t<b>\n", 2, List.of(new Term("<a>"), new Term("<b>"))),
                new D("\"a\\t\\n\"\t<b>\n", 2, List.of(new Term("\"a\\t\\n\""), new Term("<b>"))),
                new D("<a>\t\t<c>\n", 3, asList(new Term("<a>"), null, new Term("<c>"))),
                new D("\t<b>\t<c>\n", 3, asList(null, new Term("<b>"), new Term("<c>"))),
                new D(" <a> \t \" b \"\n", 2, List.of(new Term("<a>"), new Term("\" b \""))), //trim terms
                new D(" \t <b>\n", 2, asList(null, new Term("<b>"))), //trim null
                new D("null\t <b>\n", 2, asList(null, new Term("<b>"))), //accept null as null
                new D("<a>\t null \n", 2, asList(new Term("<a>"), null)), //accept null as null
                new D("", 1, null), //missing EOL
                new D("", 2, null), //missing EOL
                new D("\n", 1, singletonList(null)), // ok, first element is null
                new D("\t\n", 1, null), // two nulls offered, rows 1
                new D("<a>\t<b>\n", 1, null), // two terms offered, rows 1
                new D("<a>\n", 2, null), // one term offered, rows 2
                new D("\n", 2, null) // one null offered, rows 2
        );
        List<Arguments> list = new ArrayList<>();
        for (D d : base)
            list.add(arguments(d.input, 0, d.columns, d.expected));
        for (String prefix : List.of("\n", "\t", "\t\n\"\"<>\t?")) {
            for (D d : base) {
                int len = prefix.length();
                list.add(arguments(prefix+d.input, len, d.columns, d.expected));
                if (d.input.endsWith("\n"))
                    list.add(arguments(prefix+d.input+prefix, len, d.columns, d.expected));
            }
        }
        return list.stream();
    }

    @ParameterizedTest @MethodSource
    void testParseTerms(@NonNull String msg, int begin, int columns,
                        @Nullable List<Term> expected) {
        try {
            var terms = MessageParser.parseTerms(msg, begin, columns);
            if (expected == null)
                fail("Expected ProtocolException");
            assertEquals(asList(terms), expected);
        } catch (ProtocolException e) {
            if (expected != null)
                fail("Unexpected ProtocolException", e);
        }
    }

    private static final Term[] END_ROW = new Term[] {new Term("<END>")};
    private static final List<Term> END_ROW_LIST = List.of(new Term("<END>"));

    @SuppressWarnings("unused") static Stream<Arguments> testParse() {
        String sparql = "SELECT * WHERE {?s ?p '''\n!cancel\n''', \"!cancel\"}";
        var queryAction = new Action.Query(sparql);

        return Stream.of(
        /*  1 */arguments(List.of("!queue-cap"),    List.of(Action.QUEUE_CAP), List.of(), List.of(), 0),
        /*  2 */arguments(List.of("!queue-cap "),   List.of(Action.QUEUE_CAP), List.of(), List.of(), 0),
        /*  3 */arguments(List.of("!queue-cap \n"), List.of(Action.QUEUE_CAP), List.of(), List.of(), 0),
        /*  4 */arguments(List.of("!cancel"),       List.of(Action.CANCEL), List.of(), List.of(), 0),
        /*  5 */arguments(List.of("!cancel\t\n"),   List.of(Action.CANCEL), List.of(), List.of(), 0),

        /*  6 */arguments(List.of("!"),                List.of(), List.of(), List.of(), 1),
        /*  7 */arguments(List.of("!\n"),              List.of(), List.of(), List.of(), 1),
        /*  8 */arguments(List.of("!dummy"),           List.of(), List.of(), List.of(), 1),
        /*  9 */arguments(List.of("!query-cap-dummy"), List.of(), List.of(), List.of(), 1),
        /* 10 */arguments(List.of("!query-cape"),      List.of(), List.of(), List.of(), 1),

        /* 11 */arguments(List.of("!query "+sparql), List.of(queryAction), List.of(), List.of(), 0),
        /* 12 */arguments(List.of("!query "+sparql), List.of(queryAction), List.of(), List.of(), 0),
        /* 13 */arguments(List.of("!query\n#\n"+sparql),
                          List.of(new Action.Query("#\n"+sparql)), List.of(), List.of(), 0),

        /* 14 */arguments(List.of("?x\n"), List.of(), List.of(), List.of(), 1),
        /* 15 */arguments(List.of("?x\n!end"), List.of(), List.of(), List.of(), 1),
        /* 16 */arguments(List.of("?x\n<a>\n!end"), List.of(), List.of(), List.of(), 1),

        /* 17 */arguments(List.of("!bind\n"+sparql,
                                  "?x\n",
                                  "<a>\n<b>\n!end"),
                          List.of(new Action.Bind(sparql)),
                          List.of(List.of("x")),
                          List.of(List.of(new Term("<a>")), List.of(new Term("<b>")),
                                  END_ROW_LIST),
                          0),
        /* 18 */arguments(List.of("!bind\n"+sparql,
                                 "?x\n<a>\n<b>\n!end"),
                          List.of(new Action.Bind(sparql)),
                          List.of(List.of("x")),
                          List.of(List.of(new Term("<a>")), List.of(new Term("<b>")),
                                  END_ROW_LIST),
                          0),
        /* 19 */arguments(List.of("!bind\n"+sparql,
                                 "?x\n<a>\n",
                                 "<b>\n"),
                          List.of(new Action.Bind(sparql)),
                          List.of(List.of("x")),
                          List.of(List.of(new Term("<a>")), List.of(new Term("<b>"))),
                          0),
        /* 20 */arguments(List.of("!bind\n"+sparql,
                                  "?x\t?y1\n",
                                  "<a>\t\n",
                                  "<b>\t<c>\n\t<d>\n",
                                  "!end\n"),
                          List.of(new Action.Bind(sparql)),
                          List.of(List.of("x", "y1")),
                          List.of(asList(new Term("<a>"), null),
                                  asList(new Term("<b>"), new Term("<c>")),
                                  asList(null, new Term("<d>")),
                                  END_ROW_LIST),
                          0),
        /* 21 */arguments(List.of("!bind\n"+sparql,
                                 "?x\t?y1\n<a>\t\n",
                                 "<b>\t<c>\n\t<d>\n",
                                 "!end\n"),
                          List.of(new Action.Bind(sparql)),
                          List.of(List.of("x", "y1")),
                          List.of(asList(new Term("<a>"), null),
                                  asList(new Term("<b>"), new Term("<c>")),
                                  asList(null, new Term("<d>")),
                                  END_ROW_LIST),
                          0),
        /* 22 */arguments(List.of("!bind\n"+sparql,
                                 "?x\t?y1\n",
                                 "<a>\t\n<b>\t<c>\n\t<d>\n!end\n",
                                 "!query\n"+sparql,
                                 "!bind "+sparql,
                                 "?x\n<a>\n",
                                 "<b>\n\n!end", // \n is intentionally missing
                                 "!cancel"),
                          List.of(new Action.Bind(sparql),
                                  queryAction,
                                  new Action.Bind(sparql),
                                  Action.CANCEL),
                          List.of(List.of("x", "y1"),
                                  List.of("x")),
                          List.of(asList(new Term("<a>"), null),
                                  List.of(new Term("<b>"), new Term("<c>")),
                                  asList(null, new Term("<d>")),
                                  END_ROW_LIST,
                                  List.of(new Term("<a>")),
                                  List.of(new Term("<b>")),
                                  singletonList(null),
                                  END_ROW_LIST),
                          0)
        );
    }

    @ParameterizedTest @MethodSource
    void testParse(@NonNull List<String> messages, @NonNull List<Action> expectedActions,
                   @NonNull List<List<String>> expectedVarsLists,
                   @NonNull List<List<Term>> expectedRows, int expectedErrors) {
        List<Action> actions = new ArrayList<>();
        List<@Nullable Term @NonNull[]> rows = new ArrayList<>();
        List<@NonNull List<@NonNull String>> varsLists = new ArrayList<>();
        AtomicInteger errors = new AtomicInteger();
        MessageParser parser = new MessageParser() {
            @Override protected void onAction(@NonNull Action action) { actions.add(action); }
            @Override protected void onRow(@Nullable Term @NonNull [] row) { rows.add(row); }
            @Override protected void onVars(@NonNull List<String> list) { varsLists.add(list); }
            @Override protected void onEndRows() { rows.add(END_ROW); }
            @Override protected void onError(String reason) { errors.incrementAndGet(); }
        };
        for (String msg : messages)
            parser.parse(msg);

        assertEquals(expectedActions.size(), actions.size());
        for (int i = 0; i < actions.size(); i++) {
            Action ex = expectedActions.get(i), ac = actions.get(i);
            if (ex instanceof Action.Bind exBind) {
                assertTrue(ac instanceof Action.Bind);
                Action.Bind acBind = (Action.Bind) ac;
                assertEquals(exBind.sparql(), acBind.sparql());
            } else {
                assertEquals(ex, ac);
            }
        }
        assertEquals(expectedErrors, errors.get());
        assertEquals(expectedVarsLists, varsLists);
        assertEquals(expectedRows, rows.stream().map(Arrays::asList).toList());
    }
}