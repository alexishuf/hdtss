package com.github.lapesd.hdtss.model.solutions;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import reactor.core.publisher.Flux;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

@RequiredArgsConstructor @EqualsAndHashCode @ToString @Accessors(fluent = true)
public class FluxQuerySolutions implements QuerySolutions {
    @Getter private final @NonNull List<@NonNull String> varNames;
    private final @NonNull Flux<@NonNull SolutionRow> flux;

    private @ToString.Exclude @EqualsAndHashCode.Exclude @Nullable Boolean askResult;
    private @ToString.Exclude @EqualsAndHashCode.Exclude @Nullable List<@NonNull SolutionRow> list;

    public @NonNull Flux<SolutionRow> flux() {
        return list != null ? Flux.fromIterable(list) : flux;
    }

    @Override public @NonNull Iterator<@NonNull SolutionRow> iterator() {
        return list != null ? list.iterator() : flux.toIterable().iterator();
    }

    @Override public @NonNull Stream<@NonNull SolutionRow> stream() {
        return list != null ? list.stream() : flux.toStream();
    }

    @Override public @NonNull List<@NonNull SolutionRow> list() {
        return list != null ? list : (list = Objects.requireNonNull(flux.collectList().block()));
    }

    @Override public boolean askResult() {
        return askResult != null
                ? askResult
                : (askResult = Boolean.TRUE.equals(flux().hasElements().block()));
    }
}
