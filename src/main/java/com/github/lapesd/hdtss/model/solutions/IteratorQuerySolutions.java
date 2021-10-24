package com.github.lapesd.hdtss.model.solutions;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.NonNull;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


@RequiredArgsConstructor @EqualsAndHashCode @ToString  @Accessors(fluent = true)
public class IteratorQuerySolutions implements QuerySolutions, Iterable<@NonNull SolutionRow>  {
    @Getter private final @lombok.NonNull @NonNull List<@NonNull String> varNames;
    private final @lombok.NonNull @NonNull Iterator<@NonNull SolutionRow> iterator;

    private @EqualsAndHashCode.Exclude @ToString.Exclude List<@NonNull SolutionRow> list;
    private @EqualsAndHashCode.Exclude @ToString.Exclude Boolean askResult;

    @Override public @NonNull Flux<@NonNull SolutionRow> flux() {
        return list != null ? Flux.fromIterable(list) : Flux.fromIterable(this);
    }

    @Override public @NonNull Stream<@NonNull SolutionRow> stream() {
        if (list != null)
            return list.stream();
        var split = Spliterators.spliteratorUnknownSize(iterator,
                Spliterator.NONNULL | Spliterator.ORDERED);
        return StreamSupport.stream(split, false);
    }

    @Override public @NonNull Iterator<@NonNull SolutionRow> iterator() {
        return list != null ? list.iterator() : iterator;
    }

    @Override public @NonNull List<@NonNull SolutionRow> list() {
        if (list != null)
            return list;
        List<@NonNull SolutionRow> list = new ArrayList<>();
        iterator.forEachRemaining(list::add);
        return this.list = list;
    }

    @Override public boolean askResult() {
        return askResult != null ? askResult : (askResult = iterator.hasNext());
    }
}
