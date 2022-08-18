package com.github.lapesd.hdtss.controller.websocket.task;

import com.github.lapesd.hdtss.TempFile;
import com.github.lapesd.hdtss.controller.execution.SparqlExecutor;
import com.github.lapesd.hdtss.controller.websocket.SparqlSession;
import com.github.lapesd.hdtss.controller.websocket.SparqlSessionContext;
import io.micronaut.context.ApplicationContext;
import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.convert.value.MutableConvertibleValues;
import io.micronaut.core.convert.value.MutableConvertibleValuesMap;
import io.micronaut.http.MediaType;
import io.micronaut.websocket.CloseReason;
import io.micronaut.websocket.WebSocketSession;
import io.micronaut.websocket.WebSocketVersion;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.*;
import org.opentest4j.AssertionFailedError;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import static java.lang.System.nanoTime;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;

class MessageSerializerTest {
    private static TempFile hdtFile;
    private static ApplicationContext appCtx;
    private static SparqlExecutor sparqlExecutor;

    @RequiredArgsConstructor @Accessors(fluent = true, chain = true)
    private static final class SessionMock implements WebSocketSession {
        @Setter @MonotonicNonNull List<String> sent;
        boolean closed = false;
        private final MutableConvertibleValues<Object> attributes = new MutableConvertibleValuesMap<>();
        @Setter private @Nullable RuntimeException fail;

        public void unFail() { fail = null; }

        @Override public String toString() {
            return String.format("SessionMock-%d", System.identityHashCode(this));
        }
        @Override public String getId() { return toString(); }
        @Override public MutableConvertibleValues<Object> getAttributes() { return attributes; }
        @Override public boolean isOpen() { return true; }
        @Override public boolean isWritable() { return true; }
        @Override public boolean isSecure() { return false; }
        @Override public String getProtocolVersion() { return WebSocketVersion.V13.toString(); }
        @Override public void close() { closed = true; }
        @Override public void close(CloseReason closeReason) { closed = true; }
        @Override public MutableConvertibleValues<Object> clear() {return attributes.clear(); }
        @Override public Set<String> names() { return attributes.names(); }
        @Override public Collection<Object> values() { return attributes.values(); }
        @Override public Set<? extends WebSocketSession> getOpenSessions() { return Collections.singleton(this); }
        @SneakyThrows @Override public URI getRequestURI() { return new URI("http://example.org/"); }

        @Override public <T> Publisher<T> send(T message, MediaType mediaType) {
            assert message instanceof String;
            if (fail == null) {
                sent.add(message.toString());
                return Mono.just(message);
            } else {
                return Mono.error(fail);
            }
        }

        @Override public <T> CompletableFuture<T> sendAsync(T message, MediaType mediaType) {
            assert message instanceof String;
            CompletableFuture<T> f = new CompletableFuture<>();
            if (fail == null) {
                sent.add(message.toString());
                f.complete(message);
            } else {
                f.completeExceptionally(fail);
            }
            return f;
        }

        @Override public MutableConvertibleValues<Object> put(CharSequence key, Object value) { return attributes.put(key, value); }
        @Override public MutableConvertibleValues<Object> remove(CharSequence key) { return attributes.remove(key); }
        @Override public <T> Optional<T> get(CharSequence name, ArgumentConversionContext<T> conversionContext) { return attributes.get(name, conversionContext); }
    }

    @Accessors(fluent = true, chain = true)
    private static class Mock extends MessageSerializer implements AutoCloseable {
        private final @Nullable Class<? extends Throwable> expectedError;
        private @MonotonicNonNull Throwable failure;
        private final Semaphore terminated = new Semaphore(0);
        private final List<String> messages = Collections.synchronizedList(new ArrayList<>());
        private final long startNanos = nanoTime();
        private final long msTestTimeout;

        private static class SparqlSessionHelper extends SparqlSession {
            final SessionMock sessionMock;
            public SparqlSessionHelper(long windowTimeoutMs, int maxMessagesInWindow) {
                this(new SparqlSessionContext(sparqlExecutor, 8, 64,
                                              maxMessagesInWindow,
                                             windowTimeoutMs*1_000L, 120));
            }
            public SparqlSessionHelper(@NonNull SparqlSessionContext ctx) {
                this(ctx, new SessionMock());
            }
            public SparqlSessionHelper(@NonNull SparqlSessionContext ctx,
                                       @NonNull SessionMock sessionMock) {
                super(ctx, sessionMock);
                this.sessionMock = sessionMock;
            }
        }

        @Builder
        public Mock(long windowTimeoutMs, int maxMessagesInWindow, long msTestTimeout,
                    @Nullable Class<? extends Throwable> expectedError) {
            super(new SparqlSessionHelper(windowTimeoutMs, maxMessagesInWindow));
            ((SparqlSessionHelper)this.session).sessionMock.sent(messages);
            this.expectedError = expectedError;
            assert msTestTimeout >= 0;
            this.msTestTimeout = msTestTimeout == 0 ? Long.MAX_VALUE : msTestTimeout;
            Thread thread = new Thread(this);
            thread.setName(toString());
            thread.start();
        }

        public SessionMock session() { return ((SparqlSessionHelper)session).sessionMock; }

        @Override public void onCompletion(@Nullable Throwable error) {
            if (terminated.tryAcquire()) {
                terminated.release();
                if (failure == null)
                    failure = new AssertionFailedError("double onCompletion");
                assert false : "double onCompletion";
            } else {
                terminated.release();
                if (error != null) {
                    if (expectedError != null) {
                        for (Throwable cause = error; cause != null ; cause = cause.getCause()) {
                            if (expectedError.isInstance(cause)) {
                                error = cause;
                                break;
                            }
                        }
                    }
                    if (expectedError != null) {
                        if (!expectedError.isInstance(error)) {
                            String msg = "Expected " + expectedError + ", got " + error.getClass();
                            failure = new AssertionFailedError(msg);
                        } // else: do not save error into failure
                    } else if (failure == null) {
                        failure = error;
                    } else {
                        failure.addSuppressed(error);
                    }
                } else if (expectedError != null) {
                    failure = new AssertionFailedError("Expected "+expectedError.getName());
                }
            }
        }

        public void sync() {
            while (!queueEmpty() && failure == null) Thread.yield();
            long deadline = nanoTime()+ 1_000_000L;
            while (nanoTime() < deadline && failure == null) Thread.yield();
        }

        public void awaitSent(int ms) {
            //noinspection resource
            SessionMock sessionMock = session();
            int target = sessionMock.sent.size() + 1;
            long nanos = ms * 1_000_000L;
            nanos += Math.min(nanos/2, 5_000_000L);
            long deadline = nanoTime()+nanos;
            while (nanoTime() < deadline && sessionMock.sent.size() < target && failure == null)
                Thread.yield();
            if (sessionMock.sent.size() < target)
                Assertions.fail("no message sent in "+ms+"ms");
        }

        public void await(int ms, List<String> expectedMessages) {
            long deadline = nanoTime()+ms*1_000_000L;
            while (nanoTime() < deadline && failure == null) {
                assertEquals(expectedMessages, messages);
                Thread.yield();
            }
        }


        @Override public void close() throws Exception {
            if (isOpen())
                end(null, null);
            terminated.acquireUninterruptibly();
            terminated.release();
            if      (failure instanceof Error) throw (Error) failure;
            else if (failure != null)          fail(failure);
            double elapsedMs = (nanoTime() - startNanos) / 1_000_000.0;
            assertTrue(elapsedMs < msTestTimeout, "elapsedMs="+elapsedMs+", expected < "+msTestTimeout);
        }

    }


    @BeforeAll
    static void beforeAll() throws IOException {
        var path = "data/query/foaf-graph.hdt";
        hdtFile = new TempFile(".hdt").initFromResource(TempFile.class, path);
        var properties = Map.of("hdt.location", (Object)hdtFile.getAbsolutePath());
        appCtx = ApplicationContext.builder().properties(properties).start();
        sparqlExecutor = appCtx.getBean(SparqlExecutor.class);
        assertNotNull(sparqlExecutor);
        try (SessionMock mock = new SessionMock().sent(new ArrayList<>())) {
            mock.sendSync("1"); // first execution may block for a few seconds
            assertEquals(singletonList("1"), mock.sent);
        }
    }

    @AfterAll
    static void afterAll() throws IOException {
        appCtx.close();
        hdtFile.close();
    }

    @Test
    void testDoNotMerge() throws Exception {
        try (Mock mock = Mock.builder().windowTimeoutMs(100).maxMessagesInWindow(1)
                                       .msTestTimeout(50).build()) {
            mock.send("1");
            mock.send("2\nnewline");
            mock.sync();
            assertEquals(asList("1", "2\nnewline"), mock.messages);
        }
    }

    @RepeatedTest(5)
    void testMerge() throws Exception {
        int window = 5;
        try (Mock mock = Mock.builder().windowTimeoutMs(window).maxMessagesInWindow(2)
                .msTestTimeout(50).build()) {
            mock.send("1");
            mock.await(1, emptyList());
            mock.send("2");
            mock.sync();
            assertEquals(singletonList("12"), mock.messages);
            mock.send("3");
            mock.await(1, singletonList("12"));
            mock.awaitSent(window);
            assertEquals(asList("12", "3"), mock.messages);
            mock.send("4");
            mock.await(1, asList("12", "3"));
            mock.awaitSent(window);
            assertEquals(asList("12", "3", "4"), mock.messages);
        }
    }

    @RepeatedTest(10)
    void testCancelFillingWindow() throws Exception {
        try (Mock mock = Mock.builder().windowTimeoutMs(100).maxMessagesInWindow(2)
                                       .msTestTimeout(50).build()) {
            mock.send("1");
            mock.await(1, emptyList());
            mock.end("!cancel", null);
            mock.sync();
            assertEquals(singletonList("1!cancel"), mock.messages);
        }
    }

    @Test
    void testCancelMidWindow() throws Exception {
        try (Mock mock = Mock.builder().windowTimeoutMs(100).maxMessagesInWindow(3)
                .msTestTimeout(50).build()) {
            mock.send("1\n");
            mock.await(1, emptyList());
            mock.end("!cancel\n", null);
            mock.sync();
            assertEquals(singletonList("1\n!cancel\n"), mock.messages);
        }
    }

    @RepeatedTest(10)
    void testEndMidWindow() throws Exception {
        try (Mock mock = Mock.builder().windowTimeoutMs(100).maxMessagesInWindow(3)
                .msTestTimeout(50).build()) {
            long start = nanoTime();
            mock.await(1, emptyList());
            mock.end("bye", null);
            mock.sync();
            assertEquals(singletonList("bye"), mock.messages);
            double elapsedMs = (nanoTime()-start)/1_000_000.0;
            assertTrue(elapsedMs < 50, "too slow, expected < 50ms, took "+elapsedMs);
        }
    }

    public static final class DummyException extends RuntimeException {}

    @RepeatedTest(10)
    void testFailedFirstSend() throws Exception {
        try (Mock mock = Mock.builder().windowTimeoutMs(0).maxMessagesInWindow(1)
                             .expectedError(DummyException.class).msTestTimeout(50).build()) {
            mock.send("1");
            mock.sync();
            assertEquals(singletonList("1"), mock.messages);
            mock.session().fail(new DummyException());
            mock.send("2");
            mock.await(2, singletonList("1"));
            mock.session().unFail();
            mock.send("3"); // worker exited
            mock.await(2, singletonList("1"));
            assertEquals(singletonList("1"), mock.messages);
        }
    }

    @Test
    void testEndWithException() throws Exception {
        try (Mock mock = Mock.builder().expectedError(DummyException.class).build()) {
            mock.send("1");
            mock.sync();
            assertEquals(singletonList("1"), mock.messages);
            mock.end("bye", new DummyException());
            mock.sync();
            assertEquals(asList("1", "bye"), mock.messages);
        }
    }
}