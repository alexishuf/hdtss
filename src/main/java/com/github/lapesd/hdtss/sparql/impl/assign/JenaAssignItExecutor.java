package com.github.lapesd.hdtss.sparql.impl.assign;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Assign;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.IteratorQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import com.github.lapesd.hdtss.utils.Binding;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;

@Singleton
@Named("assign")
@RequiresOperatorFlow(values = {"ITERATOR", "HDT_REACTIVE"})
public class JenaAssignItExecutor extends JenaAssignExecutor {

    @Inject
    public JenaAssignItExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        var it = dispatcher.execute(node.children().get(0)).iterator();
        Evaluator evaluator = new Evaluator((Assign) node);
        return new IteratorQuerySolutions(node.outputVars(), new Iterator<>() {
            @Override public boolean hasNext() { return it.hasNext(); }
            @Override public @Nullable Term @NonNull[] next() { return evaluator.apply(it.next()); }
        });
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node, @Nullable Binding binding) {
        return execute(binding == null ? node : node.bind(binding));
    }
}
