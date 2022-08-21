package com.github.lapesd.hdtss.controller;

import com.github.lapesd.hdtss.TempFile;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.BlockingHttpClient;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.websocket.WebSocketClient;
import io.micronaut.websocket.WebSocketSession;
import io.micronaut.websocket.annotation.*;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

public class MultipleRoutesRegressionTest {
    private static final String DUMMY_QRY = "SELECT * WHERE { ?s a <http://example.org/Dummy>}";
    private static final String DUMMY_QRY_ESC = "SELECT%20*%20WHERE%20%7B%20%3Fs%20a%20%3Chttp%3A%2F%2Fexample.org%2FDummy%3E%7D";

    private static TempFile hdt;

    @BeforeAll static void beforeAll() throws IOException {
        String rPath = "data/query/foaf-graph.hdt";
        hdt = new TempFile(".hdt").initFromResource(TempFile.class, rPath);
    }

    @AfterAll static void afterAll() throws IOException {
        hdt.close();
    }

    @ParameterizedTest @EnumSource(EndpointFlowType.class)
    void test(EndpointFlowType flowType) {
        String[] args= {"-sparql.endpoint.flow="+flowType, "-hdt.location="+hdt.getAbsolutePath()};
        try (var appCtx = ApplicationContext.builder().args(args).start();
             var server = appCtx.getBean(EmbeddedServer.class);
             var rClient = appCtx.createBean(HttpClient.class);
             var client = rClient.toBlocking()) {
            server.start();
            Thread.sleep(10);
            String base = "http://localhost:" + server.getPort();
            doGet(client, base);
            doPost(client, base);
            doPostForm(client, base);
            doWs(appCtx, base);
        } catch (HttpClientResponseException e) {
            fail(e.getResponse().getBody(String.class).orElse(e.getResponse().reason()));
        } catch (Exception t) {
            fail(t);
        }
    }

    private void doGet(BlockingHttpClient client, String baseUri) {
        var req = HttpRequest.GET(baseUri + "/sparql?query="+DUMMY_QRY_ESC)
                .accept(SparqlMediaTypes.RESULTS_TSV_TYPE);
        assertEquals("?s\n", client.retrieve(req, String.class));
    }

    private void doPost(BlockingHttpClient client, String base) {
            var req = HttpRequest.POST(base+"/sparql", DUMMY_QRY)
                    .contentType(SparqlMediaTypes.QUERY_TYPE)
                    .accept(SparqlMediaTypes.RESULTS_CSV_TYPE);
            assertEquals("s\r\n", client.retrieve(req));
    }

    private void doPostForm(BlockingHttpClient client, String base) {
        var req = HttpRequest.POST(base+"/sparql", "query=" + DUMMY_QRY_ESC)
                             .contentType(MediaType.APPLICATION_FORM_URLENCODED_TYPE)
                             .accept(SparqlMediaTypes.RESULTS_TSV_TYPE);
        assertEquals("?s\n", client.retrieve(req));
    }

    @SuppressWarnings("unused")
    @ClientWebSocket("/sparql/ws")
    public static class WsClient implements AutoCloseable {
        private final StringBuilder received = new StringBuilder();
        private final CompletableFuture<String> response = new CompletableFuture<>();
        private @MonotonicNonNull WebSocketSession session;

        @OnOpen public void onOpen(WebSocketSession s) {
            if (session != null) {
                response.completeExceptionally(new IllegalStateException("2 onOpen() calls"));
            } else {
                session = s;
                s.sendAsync("!query SELECT * WHERE { ?x <http://xmlns.com/foaf/0.1/age> 23 }\n")
                        .whenComplete((msg, err) -> {
                            if (err != null) response.completeExceptionally(err);
                        });
            }
        }

        @OnMessage public void onMessage(WebSocketSession session, String msg) {
            received.append(msg);
            if (msg.endsWith("!end\n")) {
                new Thread(() -> {
                    try { Thread.sleep(25); } catch (InterruptedException ignored) {}
                    response.complete(received.toString());
                }).start();
            }
        }

        @OnError public void onError(WebSocketSession session, Throwable error) {
            response.completeExceptionally(error);
        }

        @OnClose public void onClose(WebSocketSession session) {
            if (!received.toString().endsWith("!end\n"))
                response.completeExceptionally(new IllegalStateException("Early onClose"));
        }

        @Override public void close() {
            if (!response.isDone() && !response.isCancelled())
                response.completeExceptionally(new IllegalStateException("close()d"));
            if (session != null && session.isOpen())
                session.close();
        }
    }

    private void doWs(ApplicationContext appCtx, String base) throws Exception {
        String uri = base + "/sparql/ws";
        try (var genClient = appCtx.createBean(WebSocketClient.class);
             WsClient client = Flux.from(genClient.connect(WsClient.class, uri)).blockFirst()) {
            assertNotNull(client);
            assertEquals("?x\n<http://example.org/Alice>\n!end\n", client.response.get());
        }
    }


}
