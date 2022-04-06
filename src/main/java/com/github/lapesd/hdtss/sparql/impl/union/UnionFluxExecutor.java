package com.github.lapesd.hdtss.sparql.impl.union;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.Op.Type;
import com.github.lapesd.hdtss.model.solutions.FluxQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import reactor.core.publisher.Flux;

@Singleton
@Named("union")
@RequiresOperatorFlow(values = {"REACTIVE"})
public class UnionFluxExecutor extends UnionExecutor {
    @Inject
    public UnionFluxExecutor(@NonNull OpExecutorDispatcher dispatcher) {super(dispatcher);}

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        if (node.type() != Type.UNION)
            throw new IllegalArgumentException("node is not Union");
        var exposedVars = node.outputVars();
        var merge = Flux.merge(Flux.fromIterable(node.children())
                .map(n -> {
                    var sols = dispatcher.execute(n);
                    return sols.flux().map(new TermsOrder(exposedVars).reset(sols.varNames()));
                }));
        return new FluxQuerySolutions(exposedVars, merge);
    }
}
