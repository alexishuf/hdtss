package com.github.lapesd.hdtss.sparql;

import com.github.lapesd.hdtss.data.query.HdtQueryService;
import com.github.lapesd.hdtss.data.query.impl.HDTUtils;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.github.lapesd.hdtss.model.solutions.IteratorQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.rdfhdt.hdt.dictionary.Dictionary;

import java.util.Iterator;
import java.util.List;

import static com.github.lapesd.hdtss.model.TermPosition.PRE;

@Singleton
public class GetPredicatesExecutor {
    private final HdtQueryService hdtQueryService;

    public GetPredicatesExecutor(HdtQueryService hdtQueryService) {
        this.hdtQueryService = hdtQueryService;
    }

    /**
     * If {@code query} corresponds to a "SELECT DISTINCT ?p WHERE {?s ?p ? o}", return a
     * {@link QuerySolutions} that answers such query efficiently.
     *
     * @param query the query to execute if it is a "select distinct predicates"
     * @return A {@link QuerySolutions} for "select distinct predicates" or null
     */
    public @Nullable QuerySolutions tryExecute(Op query) {
        if (query.type() != Op.Type.DISTINCT)
            return null;
        Op project = query.children().get(0);
        if (project.type() != Op.Type.PROJECT || project.varNames().size() != 1)
            return null;
        List<@NonNull Op> children = project.children();
        if (children.size() != 1 || children.get(0).type() != Op.Type.TRIPLE)
            return null;
        TriplePattern tp = (TriplePattern) children.get(0);
        if (tp.collectVarsInfo().positions().length != 3)
            return null;
        if (!project.varNames().get(0).contentEquals(tp.predicate().content()))
            return null;

        Dictionary dict = hdtQueryService.hdt().getDictionary();
        return new IteratorQuerySolutions(project.varNames(), new Iterator<>() {
            long next = 1;
            final long last = dict.getNpredicates();
            @Override public boolean hasNext() { return next <= last; }
            @Override public @Nullable Term @NonNull[] next() {
                return new Term[]{HDTUtils.fromHDTId(dict, next++, PRE)};
            }
        });
    }
}
