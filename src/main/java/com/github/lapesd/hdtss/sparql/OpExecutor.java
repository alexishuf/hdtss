package com.github.lapesd.hdtss.sparql;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import io.micronaut.core.order.Ordered;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Set;

public interface OpExecutor extends Ordered {
    /**
     * The set of {@link Op} types that this executor can handle on
     * {@link OpExecutor#execute(Op)}.
     *
     * @return a non-null non-empty set of {@link Op.Type}s.
     */
    @NonNull Set<Op.Type> supportedTypes();

    /**
     * Execute the given node, returning a {@link QuerySolutions} object over its solutions.
     *
     * @param node the SPARQL algebra to execute
     * @return a {@link QuerySolutions}, which can be lazy or asynchronous.
     */
    @NonNull QuerySolutions execute(@NonNull Op node);
}
