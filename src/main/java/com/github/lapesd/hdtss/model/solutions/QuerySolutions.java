package com.github.lapesd.hdtss.model.solutions;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;
import reactor.core.publisher.Flux;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public interface QuerySolutions extends Iterable<@NonNull SolutionRow> {
    /**
     * List of variable names expected in solutions.
     *
     * {@link SolutionRow}s contained in this {@link QuerySolutions} will present their
     * {@link Term}s in the exact same order (the i-th {@link Term} is a binding for the i-th var).
     * If a {@link SolutionRow} misses a binding for the i-th var, it will include a null in the
     * i-th position.
     *
     * @return A non-null, possibly empty, immutable list of non-null variable names, without the
     *         leading '?' or '$'.
     */
    @NonNull List<@NonNull String> varNames();

    /**
     * Get a {@link Flux} over the solution rows.
     *
     * This operation may be destructive: this and other accessors for the {@link SolutionRow}s
     * may return no {@link SolutionRow} or only {@link SolutionRow} not consumed yet via
     * the returned {@link Flux}
     *
     * @return a non-null, possibly empty {@link Flux} of non-null {@link SolutionRow}s.
     */
    @NonNull Flux<@NonNull SolutionRow> flux();

    /**
     * Get an {@link Iterator} over the solution rows.
     *
     * This operation may be destructive: this and other accessors for the {@link SolutionRow}s
     * may return no {@link SolutionRow} or only {@link SolutionRow} not consumed yet via
     * the returned {@link Iterator}
     *
     * @return a non-null, possibly empty {@link Iterator} of non-null {@link SolutionRow}s.
     */
    @Override
    @NonNull Iterator<@NonNull SolutionRow> iterator();

    /**
     * Get a {@link Stream} over the solution rows.
     *
     * This operation may be destructive: this and other accessors for the {@link SolutionRow}s
     * may return no {@link SolutionRow} or only {@link SolutionRow} not consumed yet via
     * the returned {@link Stream}
     *
     * @return a non-null, possibly empty {@link Stream} of non-null {@link SolutionRow}s.
     */
    @NonNull Stream<@NonNull SolutionRow> stream();

    /**
     * Get a {@link List} over the solution rows.
     *
     * This operation may be destructive: this and other accessors for the {@link SolutionRow}s
     * may return no {@link SolutionRow} or only {@link SolutionRow} not consumed yet via
     * the returned {@link List}.
     *
     * For lazy implementations (backed by a {@link Flux}, {@link Iterator} or {@link Stream}),
     * this will block indefinitely to construct a {@link List} instance.
     *
     * @return a non-null, possibly empty {@link List} of non-null {@link SolutionRow}s.
     */
    @NonNull List<@NonNull SolutionRow> list();

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
     * and place a single {@link SolutionRow} iff the ASK result is true.
     *
     * Note that for a reactive implementation, this method may block.
     *
     * @return true iff this is true ASK query or if this is a non-empty SELECT query
     */
    boolean askResult();
}
