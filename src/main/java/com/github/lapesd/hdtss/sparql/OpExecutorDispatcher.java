package com.github.lapesd.hdtss.sparql;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import org.checkerframework.checker.nullness.qual.NonNull;

public interface OpExecutorDispatcher {
    /**
     * Performs initialization that cannot be safely done from within constructor nor
     * {@link jakarta.annotation.PostConstruct}-annotated methods.
     */
    void init();

    /**
     * Evaluate the SPARQL algebra rooted at the given node.
     *
     * Unlike {@link OpExecutor}, an {@link OpExecutorDispatcher} shall accept any
     * {@link Op.Type}.
     *
     * @param node a SPARQL algebra expression to evalute
     * @return A non-null, possilby empty and possibly lazy {@link QuerySolutions} object over
     *         solutions for the given SPARQL query.
     */
    @NonNull QuerySolutions execute(@NonNull Op node);
}
