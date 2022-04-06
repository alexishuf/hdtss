package com.github.lapesd.hdtss.sparql.impl.minus;

import com.github.lapesd.hdtss.model.Row;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Minus;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.ExecutorUtils;
import com.github.lapesd.hdtss.sparql.impl.MinusBinder;
import com.github.lapesd.hdtss.utils.QueryExecutionScheduler;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.scheduler.Scheduler;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.function.Supplier;

@Requires(property = "sparql.minus.strategy", value = "SET")
@Singleton
public class SetMinusStrategy implements MinusStrategy {
    private final @NonNull Supplier<Set<Row>> setSupplier;
    private final @NonNull OpExecutorDispatcher dispatcher;
    private final @NonNull Scheduler scheduler;

    @Inject
    public SetMinusStrategy(@Named("minusSet") @NonNull Supplier<Set<Row>> setSupplier,
                            @NonNull OpExecutorDispatcher dispatcher,
                            @Named(QueryExecutionScheduler.NAME) @NonNull Scheduler scheduler) {
        this.setSupplier = setSupplier;
        this.dispatcher = dispatcher;
        this.scheduler = scheduler;
    }

    @Override
    public @NonNull Predicate<@Nullable Term @NonNull[]> createFilter(@NonNull Minus minus) {
        Op inner = minus.filter();
        var binder = new MinusBinder(minus);
        int[] outerIndices = binder.leftIndices();
        int[] innerIndices = ExecutorUtils.findIndices(binder.sharedVars(), inner.outputVars());
        CompletableFuture<Set<Row>> future = new CompletableFuture<>();
        Set<Row> set = setSupplier.get();
        dispatcher.execute(inner).flux()
                .subscribeOn(scheduler).subscribe(new Subscriber<>() {
                    @Override public void onNext(@Nullable Term @NonNull[] row) {
                        set.add(new Row(ExecutorUtils.project(innerIndices, row)));
                    }
                    @Override public void onSubscribe(Subscription s) { s.request(Long.MAX_VALUE); }
                    @Override public void onError(Throwable t)        { future.completeExceptionally(t); }
                    @Override public void onComplete()                { future.complete(set); }
                });
        return r -> {
            try {
                return !future.get().contains(new Row(ExecutorUtils.project(outerIndices, r)));
            } catch (InterruptedException|ExecutionException e) {
                throw new RuntimeException("Cannot evaluated Minus", e);
            }
        };
    }
}
