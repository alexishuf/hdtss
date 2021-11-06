package com.github.lapesd.hdtss.controller;

import com.github.lapesd.hdtss.TempFile;
import com.github.lapesd.hdtss.TestUtils;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.SparqlQueryForm;
import com.github.lapesd.hdtss.vocab.FOAF;
import com.github.lapesd.hdtss.vocab.RDF;
import com.github.lapesd.hdtss.vocab.RDFS;
import com.github.lapesd.hdtss.vocab.XSD;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.BlockingHttpClient;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.server.EmbeddedServer;
import org.apache.jena.query.QueryFactory;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes.*;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SparqlControllerTest {
    private static final @NonNull List<Object> FLOWS = asList("REACTIVE", "ITERATOR");
    private static final @NonNull String FMT_PROP = "sparql.test.results.format";
    private static TempFile hdtFile;
    private static Map<String, List<Object>> propertyChoices;

    @BeforeAll
    static void beforeAll() throws IOException {
        String path = "data/query/foaf-graph.hdt";
        hdtFile = new TempFile(".hdt").initFromResource(TempFile.class, path);
        propertyChoices = new HashMap<>();
        propertyChoices.put("hdt.location", List.of(hdtFile.getAbsolutePath()));
        propertyChoices.put("sparql.endpoint.flow", List.of("BATCH", "CHUNKED"));
        for (String op : asList("hdt", "join", "filter"))
            propertyChoices.put("sparql."+op+".flow", FLOWS);

        propertyChoices.put("sparql.join.reorder", List.of("NONE"));
        propertyChoices.put("sparql.reactive.scheduler", asList("IO", "ELASTIC"));
        propertyChoices.put("sparql.reactive.max-threads", asList("-1", "5"));
        propertyChoices.put(FMT_PROP, List.of(RESULTS_TSV, RESULTS_JSON));
    }

    @AfterAll
    static void afterAll() throws IOException {
        if (hdtFile != null)
            hdtFile.close();
    }

    static Stream<Arguments> data() {
        String prolog = String.format("""
                PREFIX : <http://example.org/>
                PREFIX xsd: <%s>
                PREFIX rdf: <%s>
                PREFIX rdfs: <%s>
                PREFIX foaf: <%s>
                """, XSD.NS, RDF.NS, RDFS.NS, FOAF.NS);
        List<List<Object>> positive = List.of(List.of());
        List<List<Object>> negative = List.of();
        return Stream.of(
    /*  1 */    arguments(prolog+"SELECT ?x WHERE { ?x foaf:age 23. }",
                          List.of(List.of(Alice))),
    /*  2 */    arguments(prolog+"SELECT DISTINCT * WHERE { ?x foaf:age ?y. }",
                          asList(asList(Alice, i23), asList(Bob, i25))),
    /*  3 */    arguments(prolog+ """
                        SELECT * WHERE {
                          { ?x foaf:name "Alice"@en }
                          UNION
                          { ?x foaf:name "charlie". }
                        }""",
                        asList(List.of(Alice), List.of(Charlie))),
                // Filter by number
    /*  4 */    arguments(prolog+ """
                        SELECT * WHERE {
                        ?who foaf:age ?age FILTER(?age < 25)
                        }
                        """, List.of(asList(Alice, i23))),
                // simple S-S join
    /*  5 */    arguments(prolog+ """
                        SELECT ?name WHERE {
                          ?who foaf:name ?name;
                               foaf:knows :Alice.
                        }
                        """, asList(List.of(bob), List.of(roberto), List.of(charlie))),
                // bogus result var
    /*  6 */    arguments(prolog+ """
                        SELECT ?name ?age WHERE {
                          ?who foaf:name ?name;
                               foaf:knows :Alice.
                        }
                        """, asList(asList(bob, null), asList(roberto, null), asList(charlie, null))),
                // left join
    /*  7 */    arguments(prolog+ """
                        SELECT ?name ?age WHERE {
                          ?who foaf:name ?name;
                               foaf:knows :Alice.
                          OPTIONAL { ?who foaf:age ?age }
                        }
                        """, asList(asList(bob, i25), asList(roberto, i25), asList(charlie, null))),
                // regex evaluation
    /*  8 */    arguments(prolog+ """
                        SELECT ?name ?age WHERE {
                          ?who foaf:name ?name;
                               foaf:knows :Alice.
                          FILTER(regex(str(?name), "^[abc].*"))
                          OPTIONAL { ?who foaf:age ?age }
                        }
                        """, asList(asList(bob, i25), asList(charlie, null))),
    /*  9 */    arguments(prolog + "ASK { ?s ?p ?o }", positive),
    /* 10 */    arguments(prolog + "ASK { ?s foaf:name ?o }", positive),
    /* 11 */    arguments(prolog + "ASK { ?s foaf:mbox ?o }", negative),
    /* 12 */    arguments(prolog + "ASK { {?s foaf:name ?o} UNION {?s foaf:mbox ?o} }", positive),
                // ASK on non-trivial query
    /* 13 */    arguments(prolog + """
                        ASK {
                          ?who foaf:name ?name;
                               foaf:knows :Alice.
                          FILTER(regex(str(?name), "^[abc].*"))
                          OPTIONAL { ?who foaf:age ?age }
                        }
                        """, positive),
                // regex has no solutions, failing the ASK
    /* 14 */    arguments(prolog + """
                        ASK {
                          ?who foaf:name ?name;
                               foaf:knows :Alice.
                          FILTER(regex(str(?name), "^[d].*"))
                          OPTIONAL { ?who foaf:age ?age }
                        }
                        """, negative),
                // ASK of union of non-trivial queries, only last has a solution
    /* 15 */    arguments(prolog + """
                        ASK {
                          { # no solution
                            ?who foaf:name ?name;
                            foaf:knows :Alice.
                            FILTER(regex(str(?name), "^[d].*"))
                            OPTIONAL { ?who foaf:age ?age }
                          } UNION { # no solution
                            ?x foaf:knows ?x;
                               foaf:knows ?y.
                             ?y foaf:age ?age FILTER(?age > 23)
                          } UNION { #This has one solution
                            ?x foaf:knows ?x;
                               foaf:knows ?y.
                             ?y foaf:age ?age FILTER(?age < 25)
                          }
                        }
                        """, positive),
                // bogus projected variable is kept
    /* 16 */    arguments(prolog+"SELECT ?x ?y ?z WHERE {?x foaf:age ?y}",
                          asList(asList(Alice, i23, null),
                                 asList(Bob, i25, null))),
                // bogus projected variable is kept at the first position
    /* 17 */    arguments(prolog+"SELECT ?z ?x ?y WHERE {?x foaf:age ?y}",
                          asList(asList(null, Alice, i23),
                                 asList(null, Bob, i25)))
        );
    }

    private static class TestContext implements AutoCloseable {
        final @NonNull ApplicationContext applicationContext;
        final @NonNull EmbeddedServer server;
        final @NonNull HttpClient rClient;
        final @NonNull BlockingHttpClient bClient;
        final @NonNull String baseURL;

        public TestContext(@NonNull ApplicationContext applicationContext) {
            this.applicationContext = applicationContext;
            server = applicationContext.createBean(EmbeddedServer.class);
            // ERROR messages about port in use are OK: NettyHttpServer will retry at least
            // 2 more times on a new random port
            server.start();
            baseURL = server.getURL().toString();
            rClient = applicationContext.createBean(HttpClient.class);
            bClient = rClient.toBlocking();
        }

        @NonNull MediaType resultsMediaType() {
            String resultsMT = applicationContext.get(FMT_PROP, String.class).orElseThrow();
            return new MediaType(resultsMT);
        }

        @NonNull String sparqlURL() {
            return baseURL + "/sparql";
        }

        @NonNull HttpRequest<Object> get(@NonNull String queryString) {
            var request = HttpRequest.GET(sparqlURL());
            request.getParameters().add("query", queryString);
            return request.accept(resultsMediaType());
        }

        @NonNull HttpRequest<SparqlQueryForm> postForm(@NonNull String queryString) {
            return HttpRequest.POST(sparqlURL(), SparqlQueryForm.class)
                    .accept(resultsMediaType())
                    .contentType(MediaType.FORM)
                    .body(new SparqlQueryForm(queryString));
        }

        @NonNull HttpRequest<String> post(@NonNull String queryString) {
            return HttpRequest.POST(sparqlURL(), String.class)
                    .accept(resultsMediaType())
                    .contentType(QUERY_TYPE)
                    .body(queryString);
        }

        @Override public void close() {
            server.stop();
            applicationContext.close();
        }

        public @NonNull ApplicationContext getApplicationContext() {
            return this.applicationContext;
        }

        public @NonNull EmbeddedServer getServer() {
            return this.server;
        }

        public @NonNull HttpClient getRClient() {
            return this.rClient;
        }

        public @NonNull BlockingHttpClient getBClient() {
            return this.bClient;
        }

        public @NonNull String getBaseURL() {
            return this.baseURL;
        }
    }

    private void doTest(@NonNull List<List<Term>> expected,
                        @NonNull Function<TestContext, HttpRequest<?>> requestFactory) {
        for (ApplicationContext appCtx : TestUtils.listApplicationContext(propertyChoices)) {
            try (TestContext ctx = new TestContext(appCtx)) {
                HttpRequest<?> request = requestFactory.apply(ctx);
                QuerySolutions solutions = ctx.bClient.retrieve(request, QuerySolutions.class);
                List<List<Term>> actual = solutions.list().stream().map(r -> asList(r.terms()))
                        .collect(Collectors.toList());
                assertEquals(new HashSet<>(expected), new HashSet<>(actual));
                for (List<Term> row : expected) {
                    long exCount = expected.stream().filter(row::equals).count();
                    long acCount = actual.stream().filter(row::equals).count();
                    assertEquals(exCount, acCount, "row="+row);
                }
            }
        }
    }

    @Test
    void testSyntaxError() {
        for (ApplicationContext appCtx : TestUtils.listApplicationContext(propertyChoices)) {
            try (TestContext ctx = new TestContext(appCtx)) {
                try {
                    HttpResponse<Object> r = ctx.bClient.exchange(ctx.get("I'm not a query"));
                    fail("Expected HttpClientResponseException to be thrown, got "+r.status());
                } catch (HttpClientResponseException e) {
                    HttpResponse<?> response = e.getResponse();
                    assertEquals(400, response.status().getCode());
                    assertEquals(MediaType.TEXT_PLAIN_TYPE, response.getContentType().orElse(null));
                    Optional<String> body = response.getBody(String.class);
                    assertTrue(body.isPresent());
                    assertTrue(body.orElse("").contains("Syntax error in query"));
                }
            }
        }
    }

    @Test
    void testFeatureNotSupported() {
        String query = String.format("""
                PREFIX : <%s>
                PREFIX foaf: <%s>
                SELECT * WHERE {
                  SERVICE :endpoint { ?s ?p ?o }
                }
                """, EX, FOAF.NS);
        assertDoesNotThrow(() -> QueryFactory.create(query));
        for (ApplicationContext appCtx : TestUtils.listApplicationContext(propertyChoices)) {
            try (TestContext ctx = new TestContext(appCtx)) {
                var r = ctx.bClient.exchange(ctx.get(query));
                fail("Expected HttpClientResponseException, got a "+r.status()+" response");
            } catch (HttpClientResponseException e) {
                HttpResponse<?> r = e.getResponse();
                assertEquals(400, r.status().getCode());
                assertEquals(MediaType.TEXT_PLAIN_TYPE, r.getContentType().orElse(null));
                Optional<String> body = r.getBody(String.class);
                assertTrue(body.isPresent());
                assertTrue(body.orElse("").contains("SERVICE is not supported"));
            }
        }
    }

    @ParameterizedTest @MethodSource("data")
    void testGet(@NonNull String query, @NonNull List<List<Term>> expected) {
        doTest(expected, c -> c.get(query));
    }

    @ParameterizedTest @MethodSource("data")
    void testPostForm(@NonNull String query, @NonNull List<List<Term>> expected) {
        doTest(expected, c -> c.postForm(query));
    }

    @ParameterizedTest @MethodSource("data")
    void testPost(@NonNull String query, @NonNull List<List<Term>> expected) {
        doTest(expected, c -> c.post(query));
    }
}