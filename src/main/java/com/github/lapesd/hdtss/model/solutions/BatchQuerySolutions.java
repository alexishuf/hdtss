package com.github.lapesd.hdtss.model.solutions;

import org.checkerframework.checker.nullness.qual.NonNull;
import reactor.core.publisher.Flux;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;


public record BatchQuerySolutions(
        @lombok.NonNull @NonNull List<@NonNull String> varNames,
        @lombok.NonNull @NonNull List<@NonNull SolutionRow> list
) implements QuerySolutions {
    public static final @NonNull BatchQuerySolutions ASK_TRUE = new BatchQuerySolutions(List.of(), List.of(SolutionRow.EMPTY));
    public static final @NonNull BatchQuerySolutions ASK_FALSE = new BatchQuerySolutions(List.of(), List.of());

    public BatchQuerySolutions(@NonNull QuerySolutions other) {
        this(other.varNames(), other.list());
    }

    @Override public @NonNull Flux<@NonNull SolutionRow> flux() {
        return Flux.fromIterable(list);
    }

    @Override public @NonNull Iterator<@NonNull SolutionRow> iterator() {
        return list.iterator();
    }

    @Override public @NonNull Stream<@NonNull SolutionRow> stream() {
        return list.stream();
    }

    @Override public boolean isHot() {
        return false;
    }

    @Override public boolean askResult() {
        return !list.isEmpty();
    }
}
