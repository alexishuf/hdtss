package com.github.lapesd.hdtss.sparql.impl.slice;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.Slice;
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
@Named("slice")
@RequiresOperatorFlow(values = "REACTIVE")
public class SliceFluxExecutor extends SliceExecutor{
    @Inject public SliceFluxExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node, @Nullable Binding binding) {
        Slice slice = (Slice) node;
        var flux = dispatcher.execute(slice.inner(), binding).flux();
        var outVars = binding == null ? slice.outputVars() : binding.unbound(slice.outputVars());
        return new FluxQuerySolutions(outVars,
                                      flux.skip(slice.offset()).take(slice.limit()));
    }
}
