package com.github.lapesd.hdtss.controller.execution;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.GetPredicatesExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.SparqlParser;
import com.github.lapesd.hdtss.sparql.optimizer.OptimizerRunner;
import com.github.lapesd.hdtss.sparql.results.chunked.ChunkedEncoder;
import io.micronaut.http.MediaType;
import jakarta.inject.Singleton;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.reactivestreams.Publisher;
import reactor.core.scheduler.Scheduler;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.System.nanoTime;

@Singleton
@RequiredArgsConstructor @Accessors(fluent = true) @Getter
public class SparqlExecutor {
    private final @NonNull SparqlParser parser;
    private final @NonNull OptimizerRunner optimizer;
    private final @NonNull Scheduler scheduler;
    private final @NonNull List<@NonNull QueryInfoConsumer> infoConsumers;
    private final @NonNull GetPredicatesExecutor predicatesExecutor;
    private final @NonNull OpExecutorDispatcher dispatcher;
    private final @NonNull ScheduledExecutorService scheduledExecutor
            = Executors.newScheduledThreadPool(1, new ThreadFactory() {
                private final AtomicInteger nextId = new AtomicInteger(1);
                @Override public Thread newThread(@NonNull Runnable r) {
                    int id = nextId.getAndIncrement();
                    Thread t = new Thread(r, "SparqlExecutor-Scheduled-"+id);
                    if (!t.isDaemon())
                        t.setDaemon(true);
                    if (t.getPriority() != Thread.NORM_PRIORITY)
                        t.setPriority(Thread.NORM_PRIORITY);
                    return t;
                }
            });

    public record DispatchResult(@NonNull QuerySolutions solutions,
                                 QueryInfo.@NonNull Builder info) {}

    /**
     * Parse, optimize and dispatch the query, while collecting metrics.
     *
     * @return A {@link DispatchResult} with {@link QuerySolutions} and {@link QueryInfo}
     *         with parse, optimize and dispatch metrics filled.
     * @throws RuntimeException Anything thrown by the {@link SparqlParser},
     *                          {@link GetPredicatesExecutor}, {@link OptimizerRunner} or
     *                          {@link OpExecutorDispatcher}. A {@link QueryInfo} will be
     *                          built with the exception and distributed via
     *                          {@link SparqlExecutor#notify(QueryInfo)}.
     */
    public @NonNull DispatchResult dispatch(CharSequence sparql) {
        QueryInfo.Builder info = QueryInfo.builder(sparql);
        try {
            long reference = nanoTime();
            Op parsed = parser.parse(sparql);
            info.addParseNs(nanoTime() - reference);

            reference = nanoTime();
            QuerySolutions solutions = predicatesExecutor.tryExecute(parsed);
            info.addDispatchNs(nanoTime() - reference);

            if (solutions == null) {
                reference = nanoTime();
                Op optimized = optimizer.optimize(parsed);
                info.addOptimizeNs(nanoTime() - reference);

                reference = nanoTime();
                solutions = dispatcher.execute(optimized);
                info.addDispatchNs(nanoTime() - reference);
            }

            return new DispatchResult(solutions, info);
        } catch (Throwable error) {
            notify(info.error(error).build());
            throw error;
        }
    }

    /**
     * Get a {@link Publisher} for the serialized result of the query.
     *
     * <p>The publisher will be instrumented to fill and distribute a {@link QueryInfo} upon
     * termination (whatever the cause: cancel, error or completion).</p>
     */
    public @NonNull Publisher<byte[]> execute(@NonNull CharSequence sparql,
                                              @NonNull ChunkedEncoder encoder,
                                              @NonNull MediaType mt) {
        DispatchResult dr = dispatch(sparql);
        return encoder.encode(mt, dr.solutions)
                .onTermination((err, cancelled, rows, items, nanos)
                        -> notify(dr.info.error(err).cancelled(cancelled).rows(rows).build()));
    }

    /**
     * Distribute a {@link QueryInfo} to all known {@link QueryInfoConsumer}s.
     *
     * <p>This should be called by callers of {@link SparqlExecutor#dispatch(CharSequence)} after
     * a non-exception return.</p>
     */
    public void notify(@NonNull QueryInfo info) {
        for (QueryInfoConsumer consumer : infoConsumers)
            consumer.accept(info);
    }



}
