package com.github.lapesd.hdtss.sparql;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.utils.Binding;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

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
     * @param node a SPARQL algebra expression to evaluate
     * @return A non-null, possilby empty and possibly lazy {@link QuerySolutions} object over
     *         solutions for the given SPARQL query.
     */
    @NonNull QuerySolutions execute(@NonNull Op node);

    /**
     * Equivalent to {@code execute(node.bind(binding))}, but faster by minimizing creation of new
     * of {@link Op} objects.
     *
     * @param node  a SPARQL algebra expression to evaluate
     * @param binding A mapping from vars to values that should replace them in {@code node}.
     * @return A non-null, possibly empty and possibly lazye {@link QuerySolutions} object
     *         over the solutions for {@code node.bind(bindings)}.
     */
    @NonNull QuerySolutions execute(@NonNull Op node, @Nullable Binding binding);
}
