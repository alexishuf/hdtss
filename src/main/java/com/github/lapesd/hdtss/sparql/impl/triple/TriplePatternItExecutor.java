package com.github.lapesd.hdtss.sparql.impl.triple;

import com.github.lapesd.hdtss.data.query.HdtQueryService;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import com.github.lapesd.hdtss.utils.Binding;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Singleton
@Named("triple")
@RequiresOperatorFlow(operator = "hdt", values = {"ITERATOR"})
public class TriplePatternItExecutor extends TriplePatternExecutor {
    @Inject
    public TriplePatternItExecutor(@NonNull HdtQueryService hdtQueryService) {
        super(hdtQueryService);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        return hdtQueryService.queryIterator((TriplePattern) node);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node, @Nullable Binding binding) {
        if (binding != null)
            node = node.bind(binding);
        return hdtQueryService.queryIterator((TriplePattern) node);
    }
}
