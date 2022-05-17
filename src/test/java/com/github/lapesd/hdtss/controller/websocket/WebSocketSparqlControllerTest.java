package com.github.lapesd.hdtss.controller.websocket;

import com.github.lapesd.hdtss.TempFile;
import com.github.lapesd.hdtss.TestConfigPermutations;
import com.github.lapesd.hdtss.controller.SparqlControllerTest;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.impl.JenaSparqlParser;
import com.github.lapesd.hdtss.sparql.results.codecs.TSVCodec;
import com.github.lapesd.hdtss.vocab.FOAF;
import com.github.lapesd.hdtss.vocab.RDF;
import com.github.lapesd.hdtss.vocab.RDFS;
import com.github.lapesd.hdtss.vocab.XSD;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.MediaType;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.websocket.WebSocketClient;
import io.micronaut.websocket.WebSocketSession;
import io.micronaut.websocket.annotation.ClientWebSocket;
import io.micronaut.websocket.annotation.OnClose;
import io.micronaut.websocket.annotation.OnMessage;
import io.micronaut.websocket.annotation.OnOpen;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.value.qual.MinLen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public class WebSocketSparqlControllerTest {
    private static TestConfigPermutations generator;
    private static final int queueCap = 7;

    @BeforeAll
    static void beforeAll() throws IOException {
        var path = "data/query/foaf-graph.hdt";
        generator = new TestConfigPermutations(TempFile.class, path) {
            @Override public @NonNull Map<String, List<Object>> choices() {
                return Map.of(
                        "sparql.flow", List.of("REACTIVE", "ITERATOR"),
                        "hdt.estimator", List.of("NONE", "PEEK"),
                        "sparql.ws.window.rows", List.of("1", "2"),
                        "sparql.ws.window.us", List.of("5000"),
                        "sparql.ws.bind-request", List.of("2", "64"),
                        "sparql.ws.action-queue", List.of(String.valueOf(queueCap))
                );
            }
        };
    }

    @AfterAll
    static void afterAll() throws IOException {
        if (generator != null)
            generator.close();
    }

    static @NonNull List<String> toLines(@NonNull CharSequence cs) {
        ArrayList<String> list = new ArrayList<>();
        String string = cs.toString();
        for (int i = 0, end, len = string.length(); i < len; i = end+1) {
            end = string.indexOf('\n', i);
            if (end < 0) end = len;
            list.add(string.substring(i, end));
        }
        return list;
    }

    static @NonNull List<@NonNull List<@NonNull String>> toResponse(@NonNull CharSequence tsv) {
        List<@NonNull String> lines = toLines(tsv);

        ArrayList<String> body = new ArrayList<>(lines.subList(1, lines.size()));
        body.add("!end");

        List<@NonNull List<@NonNull String>> blocks = new ArrayList<>();
        blocks.add(List.of(lines.get(0)));
        blocks.add(body);
        return blocks;
    }

    private record QueryData(List<String> clientMessages, List<List<String>> response,
                             boolean burst) { }

    static List<QueryData> queryData() {
        JenaSparqlParser parser = new JenaSparqlParser();
        TSVCodec tsvCodec = new TSVCodec(null);
        List<QueryData> singleQuery = new ArrayList<>();

        SparqlControllerTest.data().forEach(args -> {
            var sparql = args.get()[0].toString();
            List<@NonNull String> vars = parser.parse(sparql).outputVars();
            //noinspection unchecked
            List<Term[]> rows = ((List<List<Term>>) args.get()[1])
                    .stream().map(l -> l.toArray(new Term[0])).toList();
            var solutions = new BatchQuerySolutions(vars, rows);
            String tsv = new String(tsvCodec.encode(solutions), UTF_8);
            List<List<String>> response = toResponse(tsv);
            singleQuery.add(new QueryData(List.of("!query "+sparql), response, false));
        });
        List<QueryData> list = new ArrayList<>(singleQuery);

        // prefix a !cancel action
        for (QueryData base : singleQuery) {
            ArrayList<String> clientMessages = new ArrayList<>(base.clientMessages());
            clientMessages.add(0, "!cancel\n");
            list.add(new QueryData(clientMessages, base.response(), false));
        }

        // prefix a !queue-cap action
        for (QueryData base : singleQuery) {
            ArrayList<String> clientMessages = new ArrayList<>(base.clientMessages());
            clientMessages.add(0, "!queue-cap\n");
            List<List<String>> response = new ArrayList<>();
            response.add(List.of("!action-queue-cap="+queueCap));
            response.addAll(base.response);
            list.add(new QueryData(clientMessages, response, false));
        }

        // postfix a !queue-cap action
        for (QueryData base : singleQuery) {
            ArrayList<String> clientMessages = new ArrayList<>(base.clientMessages());
            clientMessages.add("!queue-cap\n");
            List<List<String>> response = new ArrayList<>(base.response);
            response.add(List.of("!action-queue-cap="+queueCap));
            list.add(new QueryData(clientMessages, response, false));
        }

        // send the same !query twice
        for (QueryData base : singleQuery) {
            ArrayList<String> clientMessages = new ArrayList<>(base.clientMessages());
            clientMessages.addAll(base.clientMessages());
            ArrayList<List<String>> response = new ArrayList<>(base.response);
            response.addAll(base.response);
            list.add(new QueryData(clientMessages, response, false));
        }

        // add burst variations of all predecessors with more than one message
        for (int i = 0, size = list.size(); i < size; i++) {
            QueryData base = list.get(i);
            if (base.clientMessages.size() > 1)
                list.add(new QueryData(base.clientMessages, base.response, true));
        }
        return list;
    }

    @ClientWebSocket("/sparql")
    @Accessors(fluent = true)
    @Slf4j
    public static abstract class QueryClient implements AutoCloseable {
        private final @NonNull StringBuilder response = new StringBuilder();
        private @Nullable WebSocketSession session;
        @Setter private @Nullable ExecutorService executor;
        private QueryData queryData;
        private int dataRow = -1, expectedLines = -1;
        boolean responseComplete = false;
        private @Nullable Throwable error;

        @OnOpen
        public synchronized void onOpen(@NonNull WebSocketSession session) {
            try {
                assert this.session == null;
                this.session = session;
                notifyAll();
            } catch (Throwable t) { addError(t); }
        }

        public synchronized void send(@NonNull String msg, @Nullable Runnable onComplete) {
            try {
                boolean interrupted = false;
                while (session == null) {
                    try {
                        wait();
                    } catch (InterruptedException e) { interrupted = true; }
                }
                log.trace("[client] {} <<< {}", session.getId(), msg.replace("\n", "\\n"));
                session.sendAsync(msg).whenComplete((same, error) -> {
                    if      (error      != null) addError(error);
                    else if (onComplete != null) {
                        try {
                            onComplete.run();
                        } catch (Throwable t) { addError(t); }
                    }
                });
                if (interrupted)
                    Thread.currentThread().interrupt();
            } catch (Throwable t) { addError(t); }
        }

        public synchronized void sendAll() {
            if (queryData.burst) {
                for (String msg : queryData.clientMessages) send(msg, null);
            } else {
                Consumer<Integer> sender = new Consumer<>() {
                    @Override public void accept(Integer i) {
                        send(queryData.clientMessages.get(i), () -> {
                            if (i+1 < queryData.clientMessages.size())
                                requireNonNull(executor).execute(() -> accept(i+1));
                        });
                    }
                };
                sender.accept(0);
            }
        }

        @OnMessage
        public synchronized void onMessage(@NonNull String msg) {
            try {
                response.append(msg);
                checkResponseComplete();
            } catch (Throwable t) { addError(t); }
        }

        public synchronized void queryData(@NonNull QueryData data, int dataRow) {
            try {
                if (this.queryData != null)
                    throw new IllegalStateException("queryData already set");
                this.queryData = data;
                this.dataRow = dataRow;
                expectedLines = data.response.stream().map(List::size)
                                             .reduce(0, Integer::sum);
                checkResponseComplete();
            } catch (Throwable t) { addError(t); }
        }

        public synchronized void awaitTestResult() {
            boolean interrupted = false;
            while (!responseComplete) {
                try {
                    wait();
                } catch (InterruptedException e) { interrupted = true; }
            }
            if (error != null)
                fail(error);
            if (queryData == null)
                fail("No expectedResponse set");
            List<String> lines = toLines(response);
            int next = 0;
            List<List<String>> batches = queryData.response;
            for (int i = 0; i < batches.size(); i++) {
                List<String> subset = batches.get(i);
                HashSet<String> actual = new HashSet<>(lines.subList(next, next + subset.size()));
                assertEquals(new HashSet<>(subset), actual, "at subset "+i+", dataRow="+dataRow);
                next = next + subset.size();
            }
            if (interrupted)
                Thread.currentThread().interrupt();
        }

        private synchronized void addError(@Nullable Throwable error) {
            if (error != null) {
                if (this.error == null) {
                    this.error = error;
                    responseComplete = true;
                    notifyAll();
                } else {
                    this.error.addSuppressed(error);
                }
            }
        }

        private void checkResponseComplete() {
            if (queryData == null)
                throw new IllegalStateException("null queryData");
            if (response.isEmpty())
                return;
            if (toLines(response).size() >= expectedLines) {
                Thread completer = new Thread(() -> {
                    try { Thread.sleep(100); } catch (InterruptedException ignored) { }
                    synchronized (QueryClient.this) {
                        responseComplete = true;
                        notifyAll();
                    }
                });
                String safeId = session == null ? "null" : session.getId();
                completer.setName("Completer[session="+safeId+"]");
                completer.start();
            }
        }
    }

    private static class TestContext implements AutoCloseable {
        private final @NonNull ApplicationContext appCtx;
        private final @NonNull EmbeddedServer server;
        private final @NonNull String endpointURL;
        private final @NonNull ExecutorService executor;
        private final @NonNull List<Future<?>> asyncFutures = new ArrayList<>();

        public TestContext(@NonNull ApplicationContext appCtx) {
            this.appCtx = appCtx;
            this.server = appCtx.getBean(EmbeddedServer.class);
            this.server.start();
            String url = this.server.getURL().toString();
            this.endpointURL = url + (url.endsWith("/") ? "" : "/") + "sparql";
            this.executor = Executors.newCachedThreadPool();
        }

        public void async(@NonNull Callable<?> callable) {
            asyncFutures.add(executor.submit(callable));
        }

        public @NonNull QueryClient queryClient(@NonNull QueryData queryData, int row) {
            var wsc = appCtx.getBean(WebSocketClient.class);
            var flux = Flux.from(wsc.connect(QueryClient.class, endpointURL));
            var client = requireNonNull(flux.blockFirst());
            client.queryData(queryData, row);
            client.executor(executor);
            return client;
        }

        public @NonNull BindClient bindClient() {
            var wsc = appCtx.getBean(WebSocketClient.class);
            var flux = Flux.from(wsc.connect(BindClient.class, endpointURL));
            return requireNonNull(flux.blockFirst());
        }

        @Override public void close() {
            List<Throwable> failures = new ArrayList<>();
            for (Future<?> future : asyncFutures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    failures.add(e);
                }
            }
            List<Runnable> pending = executor.shutdownNow();
            server.stop();
            appCtx.close();
            long start = System.nanoTime();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS))
                    failures.add(new AssertionFailedError("executor.awaitTermination()==false"));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            long shutdownMs = NANOSECONDS.toMillis(System.nanoTime() - start);
            if (shutdownMs > 1000)
                failures.add(new AssertionFailedError("Executor shutdown too slow: "+shutdownMs+"ms"));
            try {
                assertEquals(List.of(), pending);
            } catch (Throwable t) {
                failures.add(t);
            }
            if (!failures.isEmpty())
                fail(failures.get(0));
        }
    }

    @Test
    void testQuery() {
        for (ApplicationContext appCtx : generator) {
            try (TestContext ctx = new TestContext(appCtx)) {
                List<QueryData> queryData = queryData();
                for (int i = 0; i < queryData.size(); i++) {
                    int idx = i;
                    QueryData d = queryData.get(i);
                    ctx.async(() -> {
                        try (QueryClient client = ctx.queryClient(d, idx)) {
                            client.sendAll();
                            client.awaitTestResult();
                        }
                        return null;
                    });

                }
            }
        }
    }

    record BoundResults(@NonNull List<@NonNull @MinLen(1) String> vars,
                        @NonNull List<@NonNull BoundResultsEntry> entries) {}

    record BoundResultsEntry(@NonNull List<Term> binding,
                             @NonNull List<@NonNull List<@Nullable Term>> rows) {
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BoundResultsEntry that)) return false;
            return Objects.equals(binding, that.binding)
                    && rows.size() == that.rows.size()
                    && Objects.equals(new HashSet<>(rows), new HashSet<>(that.rows));
        }

        @Override public int hashCode() {
            return Objects.hash(binding, new HashSet<>(rows));
        }
    }

    record BindData(String sparql, List<BoundResultsEntry> expected, QuerySolutions bindings, boolean burst) {}

    record ProtoBindData(String sparql, List<BoundResultsEntry> expected, QuerySolutions bindings) {
        public @NonNull BindData create(boolean burst) {
            return new BindData(sparql, expected, bindings, burst);
        }
    }

    static List<BindData> bindData() {
        List<ProtoBindData> protoList = new ArrayList<>();

        SparqlControllerTest.data().forEach(args -> {
            var sparql = args.get()[0].toString();
            @SuppressWarnings("unchecked") var expected = (List<List<Term>>) args.get()[1];

            var nullBinding = new BatchQuerySolutions(List.of(),
                                                      List.of(new Term[][]{new Term[0]}));
            var boundExpected = List.of(new BoundResultsEntry(List.of(), expected));
            protoList.add(new ProtoBindData(sparql, boundExpected, nullBinding));

            var singleBogus = new BatchQuerySolutions(
                    List.of("bogus"), singletonList(new Term[]{new Term("<bogus>")}));
            boundExpected = List.of(new BoundResultsEntry(List.of(new Term("<bogus>")), expected));
            protoList.add(new ProtoBindData(sparql, boundExpected, singleBogus));

            var twoBogus = new BatchQuerySolutions(
                    List.of("bogus"), List.of(new Term[][]{
                            new Term[]{new Term("<bogus1>")},
                            new Term[]{new Term("<bogus2>")}
            }));
            boundExpected = List.of(new BoundResultsEntry(List.of(new Term("<bogus1>")), expected),
                                    new BoundResultsEntry(List.of(new Term("<bogus2>")), expected));
            protoList.add(new ProtoBindData(sparql, boundExpected, twoBogus));
        });

        String prefix = String.format("""
                PREFIX : <http://example.org/>
                PREFIX xsd: <%s>
                PREFIX rdf: <%s>
                PREFIX rdfs: <%s>
                PREFIX foaf: <%s>
                """, XSD.NS, RDF.NS, RDFS.NS, FOAF.NS);

        // useful bindings
        protoList.add(new ProtoBindData(
                prefix+"SELECT ?x WHERE { ?x foaf:name ?y }",
                List.of(new BoundResultsEntry(List.of(new Term("\"Alice\"@en")),
                                          List.of(List.of(Alice))),
                        new BoundResultsEntry(List.of(new Term("\"bob\"")),
                                          List.of(List.of(Bob)))),
                new BatchQuerySolutions(List.of("y"),
                                        List.of(new Term[][]{
                                                new Term[]{new Term("\"Alice\"@en")},
                                                new Term[]{new Term("\"bob\"")},
                                        }))
        ));

        // project out bound vars
        protoList.add(new ProtoBindData(
                prefix+"SELECT ?y ?x WHERE { ?x foaf:name ?y }",
                List.of(new BoundResultsEntry(List.of(new Term("\"Alice\"@en")),
                                          List.of(List.of(Alice))),
                        new BoundResultsEntry(List.of(new Term("\"bob\"")),
                                          List.of(List.of(Bob)))),
                new BatchQuerySolutions(List.of("y"),
                        List.of(new Term[][]{
                                new Term[]{new Term("\"Alice\"@en")},
                                new Term[]{new Term("\"bob\"")},
                        }))
        ));

        // preserve dummy projection var
        protoList.add(new ProtoBindData(
                prefix+"SELECT ?y ?x ?z WHERE { ?x foaf:name ?y }",
                List.of(new BoundResultsEntry(asList(new Term("\"Alice\"@en"), null),
                                          List.of(List.of(Alice))),
                        new BoundResultsEntry(asList(new Term("\"bob\""), null),
                                List.of(List.of(Bob)))
                ),
                new BatchQuerySolutions(List.of("y", "z"),
                        List.of(new Term[][]{
                                new Term[]{new Term("\"Alice\"@en"), null},
                                new Term[]{new Term("\"bob\""),      null},
                        }))
        ));

        //do not re-introduce var bound to null
        protoList.add(new ProtoBindData(
                prefix+ """
                        SELECT ?x ?name WHERE {
                            ?x foaf:knows ?y;
                               foaf:name ?name .
                        }
                        """,
                List.of(new BoundResultsEntry(List.of(Alicia), List.of()),
                        new BoundResultsEntry(List.of(Alice),
                                          List.of(List.of(AliceEN), List.of(Alicia))),
                        new BoundResultsEntry(singletonList(null),
                                          List.of(List.of(AliceEN),
                                                  List.of(Alicia),
                                                  List.of(bob),     //:Bob foaf:knows :Alice
                                                  List.of(roberto), //:Bob foaf:knows :Alice
                                                  List.of(bob),     //:Bob foaf:knows :Bob
                                                  List.of(roberto), //:Bob foaf:knows :Bob
                                                  List.of(charlie)))
                ),
                new BatchQuerySolutions(List.of("x"), List.of(new Term[][]{
                        new Term[]{Alicia},
                        new Term[]{Alice},
                        new Term[]{null}
                }))
        ));

        //add variants bursting bindings
        return protoList.stream().flatMap(p -> Stream.of(false, true).map(p::create)).toList();
    }

    @ClientWebSocket("/sparql")
    public abstract static class BindClient implements AutoCloseable {
        private @Nullable WebSocketSession session;
        private long requested;
        private @Nullable List<String> vars = null;
        private int boundVars = -1;
        private @Nullable List<@Nullable Term> activeBinding = null;
        private @NonNull List<List<Term>> rows = new ArrayList<>();
        private final @NonNull List<BoundResultsEntry> resultsEntries = new ArrayList<>();
        private @Nullable BoundResults results;
        private RuntimeException error;

        @OnOpen
        public synchronized void onOpen(@NonNull WebSocketSession session) {
            if (this.session != null)
                throw new IllegalArgumentException("session previously set");
            this.session = session;
            notifyAll();
        }

        @OnClose
        public synchronized void onClose(@NonNull WebSocketSession session) {
            assert this.session == session;
            this.session = null;
            this.requested = Long.MAX_VALUE;
            this.results = new BoundResults(List.of("closed session"), List.of());
            notifyAll();
        }

        @OnMessage
        public synchronized void onMessage(@NonNull String msg) {
            try {
                for (int i = 0, eol, len = msg.length(); i < len; i = eol + 1) {
                    eol = msg.indexOf('\n', i);
                    if (eol < 0)
                        eol = len;
                    String line = msg.substring(i, eol);
                    if (line.startsWith("!active-binding ")) {
                        closeResultEntries();
                        activeBinding = parseRow(line.substring(16), boundVars);
                    } else if (line.startsWith("!end")) {
                        assert results == null && vars != null;
                        closeResultEntries();
                        results = new BoundResults(vars, resultsEntries);
                        notifyAll();
                    } else if (line.startsWith("!error")) {
                        error = new RuntimeException(line);
                        results = new BoundResults(vars == null ? List.of() : vars, resultsEntries);
                        requested = 0;
                        notifyAll();
                    } else if (line.startsWith("!bind-request +")) {
                        requested += Long.parseLong(line.replaceAll("!bind-request \\+(\\d+)", "$1"));
                        notifyAll();
                    } else if (line.startsWith("!bind-request ")) {
                        requested = Long.parseLong(line.replaceAll("!bind-request (\\d+)", "$1"));
                        notifyAll();
                    } else if (line.startsWith("?")) {
                        assert vars == null;
                        vars = Arrays.stream(line.split("\t"))
                                .map(s -> s.replaceAll("^\\?", ""))
                                .filter(s -> !s.isEmpty()).toList();
                    } else if (line.isEmpty()) {
                        if (vars == null) vars = List.of();
                        else rows.add(List.of());
                    } else {
                        assert vars != null;
                        rows.add(parseRow(line, vars.size()));
                    }
                }
            } catch (Throwable t) {
                error = t instanceof RuntimeException re ? re : new RuntimeException(t);
                notifyAll();
            }
        }

        private void closeResultEntries() {
            if (activeBinding != null) {
                resultsEntries.add(new BoundResultsEntry(activeBinding, rows));
                rows = new ArrayList<>();
                activeBinding = null;
            }
        }

        private static @NonNull List<@Nullable Term> parseRow(String line, int expectedColumns) {
            List<Term> terms = new ArrayList<>();
            for (int i = 0, end, len = line.length(); i < len; i = end+1) {
                end = line.indexOf('\t', i);
                if (end < 0) end = len;
                String sparql = line.substring(i, end);
                Term term = sparql.isEmpty() ? null : new Term(sparql);
                assert term == null || term.isValid();
                terms.add(term);
            }
            if (line.endsWith("\t"))
                terms.add(null);
            if (expectedColumns == 0 && terms.equals(singletonList(null)))
                terms.clear();
            else if (expectedColumns == 1 && terms.isEmpty())
                terms.add(null);
            assertEquals(expectedColumns, terms.size());
            return terms;
        }

        public synchronized @NonNull BoundResults request(@NonNull BindData d) {
            resetForRequest(d);

            boolean interrupted = send("!bind "+d.sparql);
            interrupted |= sendBindings(d);
            while (results == null && error == null) {
                try { wait(); } catch (InterruptedException e) { interrupted = true; }
            }
            if (error != null)
                throw error;
            if (interrupted)
                Thread.currentThread().interrupt();
            return this.results;
        }

        private void resetForRequest(BindData d) {
            this.vars = null;
            this.rows.clear();
            this.requested = 0;
            this.resultsEntries.clear();
            this.boundVars = d.bindings.varNames().size();
            this.results = null;
        }

        private synchronized void storeError(@Nullable Throwable t) {
            if (t != null) {
                if      (error != null)                 error.addSuppressed(t);
                else if (t instanceof RuntimeException) error = (RuntimeException) t;
                else                                    error = new RuntimeException(t);
            }
        }

        private boolean send(@NonNull CharSequence cs) {
            boolean interrupted = waitSession();
            assert session != null;
            if (error != null) throw error;
            session.sendAsync(cs, MediaType.TEXT_PLAIN_TYPE).whenComplete((i, t) -> storeError(t));
            return interrupted;
        }

        private boolean sendBindings(@NonNull BindData d) {
            boolean interrupted = false;
            List<@Nullable Term @NonNull []> list = d.bindings.list();
            if (d.burst) {
                for (int i = 0, end, size = list.size(); i < size; i = end) {
                    interrupted |= waitRequested();
                    end = (int) Math.min(size, i + requested);
                    requested -= end-i;
                    StringBuilder msg = new StringBuilder();
                    if (i == 0) {
                        for (String name : d.bindings.varNames())
                            msg.append('?').append(name).append('\t');
                        msg.setLength(Math.max(0, msg.length()-1));
                        msg.append('\n');
                    }
                    for (int j = i; j < end; j++)
                        serializeRow(msg, list.get(j));
                    if (end == size)
                        msg.append("!end\n");
                    send(msg);
                }
            } else {
                waitRequested();
                String tsv = d.bindings.varNames().stream().map(n -> "?" + n).collect(joining("\t"));
                interrupted = send(tsv + "\n");
                for (Term[] terms : list) {
                    interrupted |= waitRequested();
                    --requested;
                    send(serializeRow(new StringBuilder(), terms));
                }
                send("!end\n");
            }
            return interrupted;
        }

        private @NonNull StringBuilder serializeRow(@NonNull StringBuilder out,
                                                    @Nullable Term @NonNull[] row) {
            for (Term term : row)
                out.append(term == null ? "" : TSVCodec.sanitize(term)).append('\t');
            if (row.length > 0)
                out.setLength(out.length()-1);
            return out.append('\n');
        }

        @EnsuresNonNull("this.session") private boolean waitSession() {
            boolean interrupted = false;
            while (session == null && error == null) {
                try { wait(); } catch (InterruptedException e) { interrupted = true; }
            }
            if (error != null)
                throw error;
            return interrupted;
        }

        private boolean waitRequested() {
            boolean interrupted = false;
            while (requested == 0 && error == null) {
                try { wait(); } catch (InterruptedException e) { interrupted = true; }
            }
            if (error != null)
                throw error;
            assert requested > 0;
            return interrupted;
        }
    }

    @Test
    void testBind() throws Exception {
        JenaSparqlParser parser = new JenaSparqlParser();
        for (ApplicationContext appCtx : generator) {
            try (TestContext ctx = new TestContext(appCtx);
                 BindClient bindClient = ctx.bindClient()) {
                int i = 0;
                for (BindData d : bindData()) {
                    log.debug("--- --- --- Starting bindData[{}] --- --- ---", i);
                    String msg = "at bindData()[" + i + "]";
                    BoundResults actual = bindClient.request(d);
                    var exVars = new ArrayList<>(parser.parse(d.sparql).outputVars());
                    exVars.removeAll(d.bindings.varNames());
                    assertEquals(exVars, actual.vars);
                    assertEquals(d.expected, actual.entries, msg);
                    ++i;
                }
            }
        }
    }
}
