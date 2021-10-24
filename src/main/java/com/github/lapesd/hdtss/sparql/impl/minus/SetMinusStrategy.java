package com.github.lapesd.hdtss.sparql.impl.minus;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Minus;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.utils.QueryExecutionScheduler;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
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
    private final @NonNull Supplier<Set<SolutionRow>> setSupplier;
    private final @NonNull OpExecutorDispatcher dispatcher;
    private final @NonNull Scheduler scheduler;

    @Inject
    public SetMinusStrategy(@Named("minusSet") @NonNull Supplier<Set<SolutionRow>> setSupplier,
                            @NonNull OpExecutorDispatcher dispatcher,
                            @Named(QueryExecutionScheduler.NAME) @NonNull Scheduler scheduler) {
        this.setSupplier = setSupplier;
        this.dispatcher = dispatcher;
        this.scheduler = scheduler;
    }

    private record State(int[] outerIndices, CompletableFuture<Set<SolutionRow>> future)
            implements Predicate<SolutionRow> {
        @Override public boolean test(SolutionRow row) {
            Term[] terms = row.terms(), projected = new Term[outerIndices.length];
            for (int i = 0; i < outerIndices.length; i++)
                projected[i] = terms[outerIndices[i]];
            try {
                return future.get().contains(new SolutionRow(projected));
            } catch (InterruptedException|ExecutionException e) {
                throw new RuntimeException("Cannot evaluated Minus", e);
            }
        }
    }


    @Override
    public @NonNull Predicate<SolutionRow> createFilter(@NonNull Minus minus) {
        var outerVars = minus.outer().varNames();
        var innerVars = minus.inner().varNames();
        int nInnerVars = innerVars.size();
        int[] innerIndices = new int[nInnerVars];
        int nextIdx = 0;
        for (String outerVar : outerVars) {
            int i = innerVars.indexOf(outerVar);
            if (i >= 0)
                innerIndices[nextIdx++] = i;
        }
        int nShared = nextIdx;
        int[] outerIndices = new int[nShared];
        for (int i = 0; i < nShared; i++)
            outerIndices[i] = outerVars.indexOf(innerVars.get(innerIndices[i]));
        CompletableFuture<Set<SolutionRow>> future = new CompletableFuture<>();
        Set<SolutionRow> set = setSupplier.get();
        dispatcher.execute(minus.inner()).flux()
                .subscribeOn(scheduler).subscribe(new Subscriber<>() {
            @Override public void onNext(SolutionRow row) {
                Term[] terms = row.terms(), projectedTerms = new Term[nShared];
                for (int i = 0; i < nShared; i++)
                    projectedTerms[i] = terms[innerIndices[i]];
                set.add(new SolutionRow(projectedTerms));
            }
            @Override public void onSubscribe(Subscription s) { }
            @Override public void onError(Throwable t)        { future.completeExceptionally(t); }
            @Override public void onComplete()                { future.complete(set); }
        });
        return new State(outerIndices, future);
    }
}
