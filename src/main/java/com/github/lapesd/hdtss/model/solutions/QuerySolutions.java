package com.github.lapesd.hdtss.model.solutions;

import com.github.lapesd.hdtss.model.Row;
import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import reactor.core.publisher.Flux;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface QuerySolutions extends Iterable<@Nullable Term @NonNull[]> {
    /**
     * List of variable names expected in solutions.
     *
     * Rows contained in this {@link QuerySolutions} will present their {@link Term}s in the
     * exact same order (the i-th {@link Term} is a binding for the i-th var). If a row misses
     * a binding for the i-th var, it will include a null in the i-th position.
     *
     * @return A non-null, possibly empty, immutable list of non-null variable names, without the
     *         leading '?' or '$'.
     */
    @NonNull List<@NonNull String> varNames();

    /**
     * Get a {@link Flux} over the solution rows.
     *
     * This operation may be destructive: this and other accessors for the rows may return no
     * row or only row not consumed yet via the returned {@link Flux}
     *
     * @return a non-null, possibly empty {@link Flux} of non-null rows.
     */
    @NonNull Flux<@Nullable Term @NonNull[]> flux();

    /**
     * Get an {@link Iterator} over the solution rows.
     *
     * This operation may be destructive: this and other accessors for the rows may return no row
     * or only rows not consumed yet via the returned {@link Iterator}
     *
     * @return a non-null, possibly empty {@link Iterator} of non-null rows.
     */
    @Override
    @NonNull Iterator<@Nullable Term @NonNull[]> iterator();

    /**
     * Get a {@link Stream} over the solution rows.
     *
     * This operation may be destructive: this and other accessors for the rows may return no
     * row or only row not consumed yet via the returned {@link Stream}
     *
     * @return a non-null, possibly empty {@link Stream} of non-null rows.
     */
    @NonNull Stream<@Nullable Term @NonNull[]> stream();

    /**
     * Get a {@link List} over the solution rows.
     *
     * This operation may be destructive: this and other accessors for the rows may return no
     * row or only row not consumed yet via the returned {@link List}.
     *
     * For lazy implementations (backed by a {@link Flux}, {@link Iterator} or {@link Stream}),
     * this will block indefinitely to construct a {@link List} instance.
     *
     * @return a non-null, possibly empty {@link List} of non-null rows.
     */
    @NonNull List<@Nullable Term @NonNull[]> list();

    /**
     * Same as {@link QuerySolutions#list()}, but rows are wrapped into {@link Row} instances.
     *
     * @return a non-null possibly empty list of non-null {@link Row} objects.
     */
    default @NonNull List<@NonNull Row> wrappedList() {
        return list().stream().map(Row::new).toList();
    }

    /**
     * Aggregate all rows into a set of {@link Row} objects.
     *
     * {@link Row} is used instead of {@code Term[]} because array equals is implemented by
     * reference and not by value.
     *
     * @return a new set of {@link Row} objects representing the distinct rows in this
     * {@link QuerySolutions} object.
     */
    default Set<@NonNull Row> toSet() { return stream().map(Row::new).collect(Collectors.toSet()); }

    /**
     * A {@link QuerySolutions} is <strong>cold</strong> iff a second call to
     * {@link QuerySolutions#flux()} or {@link QuerySolutions#iterator()} will start again from
     * the very first solution. A <strong>hot</strong> {@link QuerySolutions} provides no
     * guarantee and later calls to those methods will loose all previously-consumed solutions.
     *
     * @return true iff the {@link QuerySolutions} is hot.
     */
    default boolean isHot() { return true; }

    /**
     * A query is an ASK query if it has an empty projection,
     * i.e., ({@link QuerySolutions#varNames()} is empty
     *
     * @return true iff this instance pertains to an ASK query
     */
    default boolean isAsk() { return varNames().isEmpty(); }

    /**
     * Get the result of an ASK query or test if a SELECT query has at least one solution.
     *
     * The convention for ASK query results is to create a no-vars {@link QuerySolutions}
     * and place a single {@link Row#EMPTY} iff the ASK result is true.
     *
     * Note that this method may block even for a reactive implementation.
     *
     * @return true iff this is true ASK query or if this is a non-empty SELECT query
     */
    boolean askResult();
}
