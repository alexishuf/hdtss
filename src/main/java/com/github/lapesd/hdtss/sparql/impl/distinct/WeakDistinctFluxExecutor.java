package com.github.lapesd.hdtss.sparql.impl.distinct;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import io.micronaut.context.annotation.Property;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

@Singleton
@Named("weakDistinct")
@RequiresOperatorFlow(values = "REACTIVE")
public class WeakDistinctFluxExecutor extends WeakDistinctExecutor {
    private final @NonNull DistinctFluxExecutor delegate;

    public WeakDistinctFluxExecutor(@NonNull OpExecutorDispatcher dispatcher,
                                    @Property(name = "sparql.weakDistinct.window",
                                              defaultValue = "512")
                                    int window) {
        super();
        delegate = new DistinctFluxExecutor(dispatcher, new WindowDistinctStrategy(window));
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        return delegate.execute(node);
    }
}
