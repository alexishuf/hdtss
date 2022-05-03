package com.github.lapesd.hdtss.sparql.impl.exists;

import com.github.lapesd.hdtss.model.nodes.Exists;
import com.github.lapesd.hdtss.model.nodes.IdentityNode;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.FluxQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import com.github.lapesd.hdtss.utils.Binding;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Singleton
@Named("exists")
@RequiresOperatorFlow(values = {"REACTIVE", "HEAVY_REACTIVE"})
public class ExistsFluxExecutor extends ExistsExecutor {
    @Inject
    public ExistsFluxExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        Exists exists = (Exists) node;
        Op main = exists.main(), filter = exists.filter();
        if (IdentityNode.is(filter))
            return dispatcher.execute(main);
        boolean negate = exists.negate();
        var outerVars = main.outputVars().toArray(String[]::new);
        var flux = dispatcher.execute(main).flux()
                .filter(r -> {
                    Op bound = filter.bind(new Binding(outerVars, r));
                    return negate ^ dispatcher.execute(bound).askResult();
                });
        return new FluxQuerySolutions(node.outputVars(), flux);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node, @Nullable Binding binding) {
        if (binding == null || binding.isEmpty())
            return execute(node);
        Exists exists = (Exists) node;
        Op main = exists.main(), filter = exists.filter();
        if (IdentityNode.is(filter))
            return dispatcher.execute(main, binding);
        boolean negate = exists.negate();
        Binding template = createTemplate(exists.main(), binding);
        var flux = dispatcher.execute(main, binding).flux().filter(
                r -> negate ^ dispatcher.execute(filter, fillTemplate(template, r)).askResult());
        return new FluxQuerySolutions(binding.unbound(node.outputVars()), flux);
    }


}
