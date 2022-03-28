package com.github.lapesd.hdtss.model.solutions;

import com.github.lapesd.hdtss.model.Row;
import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;


public record BatchQuerySolutions(
        @lombok.NonNull @NonNull List<@NonNull String> varNames,
        @lombok.NonNull @NonNull List<@Nullable Term @NonNull[]> list
) implements QuerySolutions {
    public static final @NonNull BatchQuerySolutions ASK_TRUE = new BatchQuerySolutions(List.of(), List.of(new Term[][]{Row.EMPTY}));
    public static final @NonNull BatchQuerySolutions ASK_FALSE = new BatchQuerySolutions(List.of(), List.of());

    public BatchQuerySolutions(@NonNull QuerySolutions other) {
        this(other.varNames(), other.list());
    }

    @Override public @NonNull Flux<@Nullable Term @NonNull[]> flux() {
        return Flux.fromIterable(list);
    }

    @Override public @NonNull Iterator<@Nullable Term @NonNull[]> iterator() {
        return list.iterator();
    }

    @Override public @NonNull Stream<@Nullable Term @NonNull[]> stream() {
        return list.stream();
    }

    @Override public boolean isHot() {
        return false;
    }

    @Override public boolean askResult() {
        return !list.isEmpty();
    }

    public boolean deepEquals(QuerySolutions other) {
        if (!varNames.equals(other.varNames())) return false;
        int size = list.size();
        List<@Nullable Term @NonNull []> otherList = other.list();
        if (otherList.size() != size) return false;
        for (int i = 0; i < size; i++) {
            if (!Arrays.equals(list.get(i), otherList.get(i))) return false;
        }
        return true;
    }
}
