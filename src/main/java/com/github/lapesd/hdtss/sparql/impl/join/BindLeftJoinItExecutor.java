package com.github.lapesd.hdtss.sparql.impl.join;

import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

@Singleton
@Named("leftJoin")
@RequiresOperatorFlow(operator = "join", values = {"ITERATOR", "HDT_REACTIVE"})
@Requires(property = "sparql.join.strategy", value = "BIND", defaultValue = "BIND")
public class BindLeftJoinItExecutor extends BindJoinItExecutor {
    @Inject
    public BindLeftJoinItExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }
}
