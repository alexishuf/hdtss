package com.github.lapesd.hdtss.controller;

import com.github.lapesd.hdtss.TestConfigPermutations;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.SparqlQueryForm;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.BlockingHttpClient;
import io.micronaut.http.client.HttpClient;
import io.micronaut.runtime.server.EmbeddedServer;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashSet;
import java.util.List;
import java.util.function.Function;

import static com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes.QUERY_TYPE;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class ControllerTestBase {

    protected abstract @NonNull TestConfigPermutations getPermutations();

    protected static class TestContext implements AutoCloseable {
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

        @NonNull String sparqlURL() {
            return baseURL + "/sparql";
        }

        @NonNull HttpRequest<Object> get(@NonNull String queryString) {
            var request = HttpRequest.GET(sparqlURL());
            request.getParameters().add("query", queryString);
            return request.accept(TestConfigPermutations.resultsMediaType(applicationContext));
        }

        @NonNull HttpRequest<SparqlQueryForm> postForm(@NonNull String queryString) {
            return HttpRequest.POST(sparqlURL(), SparqlQueryForm.class)
                    .accept(TestConfigPermutations.resultsMediaType(applicationContext))
                    .contentType(MediaType.FORM)
                    .body(new SparqlQueryForm(queryString));
        }

        @NonNull HttpRequest<String> post(@NonNull String queryString) {
            return HttpRequest.POST(sparqlURL(), String.class)
                    .accept(TestConfigPermutations.resultsMediaType(applicationContext))
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

    protected void doTest(@NonNull List<List<Term>> expected,
                          @NonNull Function<TestContext, HttpRequest<?>> requestFactory) {
        for (ApplicationContext appCtx : getPermutations()) {
            try (TestContext ctx = new TestContext(appCtx)) {
                HttpRequest<?> request = requestFactory.apply(ctx);
                QuerySolutions solutions = ctx.bClient.retrieve(request, QuerySolutions.class);
                var actual = solutions.list().stream().map(r -> asList(r.terms())).toList();
                assertEquals(new HashSet<>(expected), new HashSet<>(actual));
                for (List<Term> row : expected) {
                    long exCount = expected.stream().filter(row::equals).count();
                    long acCount = actual.stream().filter(row::equals).count();
                    assertEquals(exCount, acCount, "row="+row);
                }
            }
        }
    }
}
