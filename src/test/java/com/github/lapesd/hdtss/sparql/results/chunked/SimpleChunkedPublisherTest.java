package com.github.lapesd.hdtss.sparql.results.chunked;

import com.github.lapesd.hdtss.TestVocab;
import com.github.lapesd.hdtss.model.Row;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.FluxQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.IteratorQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.google.common.base.Stopwatch;
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static reactor.core.publisher.Flux.fromIterable;
import static reactor.core.scheduler.Schedulers.boundedElastic;

class SimpleChunkedPublisherTest {
    private static final int N_THREADS = 1 + 2*Runtime.getRuntime().availableProcessors();
    private static final List<@Nullable Term @NonNull[]> ROWS = List.of(
            Row.raw(TestVocab.Alice),
            Row.raw(TestVocab.Bob),
            Row.raw(TestVocab.Charlie),
            Row.raw(TestVocab.Dave)
    );

    private static class TestPublisher extends SimpleChunkedPublisher {
        public TestPublisher(@NonNull QuerySolutions solutions) { super(solutions); }
        @Override protected void release() { }
        @Override protected byte[] askPrologue() { return "\n".getBytes(UTF_8); }
        @Override protected byte[] askBodyAndPrologue(boolean result) { return (result ? "\n" : "").getBytes(UTF_8); }
        @Override protected byte[] rowsPrologue() { return "?x\n".getBytes(UTF_8); }
        @Override protected byte[] rowsEpilogue() { return "EPILOGUE\n".getBytes(UTF_8); }
        @Override protected byte[] rowBytes(@Nullable Term @NonNull[] r) { return Arrays.toString(r).getBytes(UTF_8); }
    }

    private record Termination(Throwable error, boolean cancel, long rows, long items, long nanos) {}

    private static List<Termination> collectTermination(TestPublisher p) {
        ArrayList<Termination> list = new ArrayList<>();
        p.onTermination((error, cancelled, rows, items, nanosSinceSubscribe)
                -> list.add(new Termination(error, cancelled, rows, items, nanosSinceSubscribe)));
        return list;
    }

    private static void waitForTermination(List<Termination> terminationList) {
        Stopwatch sw = Stopwatch.createStarted();
        while (sw.elapsed(MILLISECONDS) < 2000 && terminationList.isEmpty())
            Thread.yield();
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void assertSingleNormalTermination(List<Termination> terminationList,
                                                      Type type, long expectedRows) {
        assertEquals(1, terminationList.size(), "more than one termination");
        Termination t = terminationList.get(0);
        assertNull(t.error(), "terminated by "+t.error());
        assertFalse(t.cancel(), "terminated by cancellation");
        long effExpectedRows = type.isAsk() ? Math.min(1, expectedRows) : expectedRows;
        assertEquals(effExpectedRows, t.rows(), "number of rows mismatch");
        long expectedItems = effExpectedRows + (type.isAsk() ? 1 : 2);
        assertEquals(expectedItems, t.items(), "number of rows mismatch");
    }

    private static void assertSingleErrorTermination(List<Termination> terminationList,
                                                     Exception error) {
        assertEquals(1, terminationList.size());
        assertEquals(error, terminationList.get(0).error());
    }

    private static ExecutorService executor;

    @BeforeAll
    static void beforeAll() {
        executor = Executors.newCachedThreadPool();
    }

    @AfterAll
    static void afterAll() throws InterruptedException {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    private void waitAll(List<Future<?>> futures) throws ExecutionException {
        List<ExecutionException> errors = new ArrayList<>();
        boolean interrupted = false;
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (ExecutionException t) {
                errors.add(t);
            } catch (InterruptedException exception) {
                interrupted = true;
            }
        }
        if (!errors.isEmpty())
            throw errors.get(0);
        if (interrupted)
            Thread.currentThread().interrupt();
    }

    enum Type {
        IT,
        FLUX,
        SCHED_FLUX,
        ASK_IT,
        ASK_FLUX,
        ASK_SCHED_FLUX;

        public boolean isAsk() {
            return this == ASK_IT || this ==  ASK_FLUX || this ==  ASK_SCHED_FLUX;
        }

        public boolean isAsync() {
            return this == ASK_SCHED_FLUX || this == SCHED_FLUX;
        }

        public QuerySolutions create(@NonNull List<@Nullable Term @NonNull[]> l) {
            List<@NonNull String> vars = isAsk() ? List.of() : List.of("x");
            return switch (this) {
                case IT, ASK_IT -> new IteratorQuerySolutions(vars, l.iterator());
                case FLUX, ASK_FLUX -> new FluxQuerySolutions(vars, fromIterable(l));
                case SCHED_FLUX, ASK_SCHED_FLUX ->
                        new FluxQuerySolutions(vars, fromIterable(l).publishOn(boundedElastic()));
            };
        }

        public QuerySolutions create(@NonNull RuntimeException exception) {
            List<@NonNull String> vars = isAsk() ? List.of() : List.of("x");
            return switch (this) {
                case IT, ASK_IT -> {
                    Iterator<@Nullable Term @NonNull[]> it = new Iterator<>() {
                        @Override public boolean  hasNext() { throw exception; }
                        @Override public @Nullable Term @NonNull[] next() {
                            throw new NoSuchElementException();
                        }
                    };
                    yield new IteratorQuerySolutions(vars, it);
                }
                case FLUX, ASK_FLUX -> new FluxQuerySolutions(vars, Flux.error(exception));
                case SCHED_FLUX, ASK_SCHED_FLUX -> {
                    Flux<@Nullable Term @NonNull[]> errorFlux = Flux.error(exception);
                    yield new FluxQuerySolutions(vars, errorFlux.publishOn(boundedElastic()));
                }
            };
        }

    }

    @SneakyThrows private static @NonNull String
    expected(@NonNull Type type, @NonNull List<@Nullable Term @NonNull[]> rows) {
        var p = new TestPublisher(new BatchQuerySolutions(List.of(), List.of()));
        ByteArrayOutputStream bOS = new ByteArrayOutputStream();
        if (type.isAsk()) {
            bOS.write(p.askPrologue());
            bOS.write(p.askBodyAndPrologue(!rows.isEmpty()));
        } else {
            bOS.write(p.rowsPrologue());
            for (@Nullable Term @NonNull[] row : rows)
                bOS.write(p.rowBytes(row));
            bOS.write(p.rowsEpilogue());
        }
        return bOS.toString(UTF_8);
    }

    static Stream<Arguments> testHappyPath() {
        List<List<@Nullable Term @NonNull[]>> lists =
                IntStream.range(0, ROWS.size()).mapToObj(i -> ROWS.subList(0, i)).toList();
        return Arrays.stream(Type.values()).flatMap(t -> lists.stream()
                .map(rows -> arguments(t, rows, expected(t, rows))));
    }

    @ParameterizedTest @MethodSource
    void testHappyPath(Type type, List<@Nullable Term @NonNull[]> rows, String expected) throws ExecutionException {
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < N_THREADS; i++)
                futures.add(executor.submit(() -> doTestHappyPath(type, rows, expected)));
        } finally {
            waitAll(futures);
        }
    }

    private Object doTestHappyPath(Type type, List<@Nullable Term @NonNull[]> rows,
                                   String expected) {
        TestPublisher p = new TestPublisher(type.create(rows));
        List<Termination> terminations = collectTermination(p);
        String actual = Flux.from(p).map(b -> new String(b, UTF_8))
                                    .reduce("", String::concat).block();
        assertEquals(expected, actual);
        if (type.isAsync())
            waitForTermination(terminations);
        assertSingleNormalTermination(terminations, type, rows.size());
        return null;
    }

    @ParameterizedTest @MethodSource("testHappyPath")
    void testHappyPathUnitRequests(Type type, List<@Nullable Term @NonNull[]> rows,
                                   String expected) throws Exception {
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < N_THREADS; i++) {
                futures.add(executor.submit(()
                        -> doTestHappyPathUnitRequests(type, rows, expected)));
            }
        } finally {
            waitAll(futures);
        }
    }

    private Object doTestHappyPathUnitRequests(Type type, List<@Nullable Term @NonNull[]> rows,
                                               String expected) throws Exception {
        TestPublisher p = new TestPublisher(type.create(rows));
        List<Termination> terminations = collectTermination(p);
        CompletableFuture<String> concat = new CompletableFuture<>();
        p.subscribe(new Subscriber<>() {
            private final StringBuilder acc = new StringBuilder();
            private Subscription subscription;
            @Override public void onSubscribe(Subscription s) { (subscription = s).request(1); }
            @Override public void onError(Throwable t) { concat.completeExceptionally(t); }
            @Override public void onComplete() { concat.complete(acc.toString()); }
            @Override public void onNext(byte[] bytes) {
                acc.append(new String(bytes, UTF_8));
                subscription.request(1);
            }
        });
        assertEquals(expected, concat.get());
        if (type.isAsync())
            waitForTermination(terminations);
        assertSingleNormalTermination(terminations, type, rows.size());
        return null;
    }

    @SuppressWarnings("unused") static Stream<Arguments> testThrowing() {
        return Arrays.stream(Type.values()).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource
    void testThrowing(Type type) throws ExecutionException {
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < N_THREADS; i++)
                futures.add(executor.submit(() -> { doTestThrowing(type); return null; }));
        } finally {
            waitAll(futures);
        }
    }

    private void doTestThrowing(Type type) throws InterruptedException {
        var exception = new RuntimeException("test");
        var p = new TestPublisher(type.create(exception));
        List<Termination> terminations = collectTermination(p);
        AtomicInteger onSubscribe = new AtomicInteger();
        List<String> items = new ArrayList<>();
        List<Throwable> errors = new ArrayList<>();
        AtomicInteger onComplete = new AtomicInteger();
        p.subscribe(new Subscriber<>() {
            @Override public void onSubscribe(Subscription s) { onSubscribe.incrementAndGet(); s.request(Long.MAX_VALUE); }
            @Override public void onNext(byte[] bytes)        { items.add(new String(bytes, UTF_8)); }
            @Override public void onError(Throwable t)        { errors.add(t); }
            @Override public void onComplete()                { onComplete.incrementAndGet(); }
        });
        if (type.isAsync())
            waitForTermination(terminations);
        while (errors.isEmpty() && onComplete.get() == 0)
            Thread.yield();
        Thread.sleep(5);
        assertEquals(1, onSubscribe.get());
        assertEquals(List.of(new String(type.isAsk() ? p.askPrologue() : p.rowsPrologue(), UTF_8)),
                     items);
        assertEquals(0, onComplete.get());
        assertEquals(List.of(exception), errors);
        assertSingleErrorTermination(terminations, exception);
    }
}