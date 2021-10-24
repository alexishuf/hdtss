package com.github.lapesd.hdtss.sparql.impl.triple;

import com.github.lapesd.hdtss.data.query.HdtQueryService;
import com.github.lapesd.hdtss.model.FlowType;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

@Singleton
@Named("triple")
@RequiresOperatorFlow(operator = "hdt", values = {"REACTIVE", "HEAVY_REACTIVE", "HDT_REACTIVE"})
public class TriplePatternFluxExecutor extends TriplePatternExecutor {

    @Inject
    public TriplePatternFluxExecutor(@NonNull HdtQueryService hdtQueryService) {
        super(hdtQueryService);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        return hdtQueryService.query((TriplePattern) node, FlowType.REACTIVE);
    }
}
