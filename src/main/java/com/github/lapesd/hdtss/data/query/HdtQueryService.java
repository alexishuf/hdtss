package com.github.lapesd.hdtss.data.query;

import com.github.lapesd.hdtss.model.FlowType;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.validation.constraints.NotNull;

public interface HdtQueryService {
    /**
     * Call {@link HdtQueryService#query(TriplePattern, FlowType)} with this implementation's
     * default {@link FlowType}.
     *
     * @param query the {@link TriplePattern} to query for
     * @return a non-null {@link QuerySolutions} with the solutions.
     */
    @NonNull QuerySolutions query(@NotNull TriplePattern query);

    /**
     * Dispatch {@code query} to the appropriate {@code query*()} method given the {@link FlowType}.
     *
     * @param query the triple pattern to query for
     * @param flowType the Flow control pattern to use for this query.
     * @return a non-null {@link QuerySolutions} with the solutions.
     */
    default @NonNull QuerySolutions query(@NonNull TriplePattern query, @NonNull FlowType flowType) {
        return switch (flowType) {
            case REACTIVE -> queryReactive(query);
            case ITERATOR -> queryIterator(query);
            case BATCH -> queryBatch(query);
        };
    }

    /**
     * Answer a single triple pattern query.
     *
     * The answers honor the semantics of a single variable appearing in more than one position.
     *
     * @param query a query
     * @return A non-null {@link QuerySolutions} backed by an Iterator
     */
    @NonNull QuerySolutions queryIterator(@NotNull TriplePattern query);

    /**
     * Answer a single triple pattern query.
     *
     * The answers honor the semantics of a single variable appearing in more than one position.
     *
     * @param query a query
     * @return A non-null {@link QuerySolutions} backed by a {@link reactor.core.publisher.Flux}
     */
    @NonNull QuerySolutions queryReactive(@NotNull TriplePattern query);

    /**
     * Answer a single triple pattern query.
     *
     * The answers honor the semantics of a single variable appearing in more than one position.
     *
     * <strong>Warning</strong>: This will aggregate all solutions into a List before return,
     * which may be bad idea, performance-wise.
     *
     * @param query a query
     * @return A non-null, cold, {@link QuerySolutions} backed by a {@link java.util.List}.
     */
    @NonNull QuerySolutions queryBatch(@NotNull TriplePattern query);
}
