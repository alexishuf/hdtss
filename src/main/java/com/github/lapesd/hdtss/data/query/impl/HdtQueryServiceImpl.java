package com.github.lapesd.hdtss.data.query.impl;

import com.github.lapesd.hdtss.data.load.HDTLoaderUtil;
import com.github.lapesd.hdtss.data.query.HdtQueryService;
import com.github.lapesd.hdtss.model.FlowType;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.TermPosition;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.github.lapesd.hdtss.model.solutions.*;
import com.github.lapesd.hdtss.utils.QueryExecutionScheduler;
import io.micronaut.context.annotation.Property;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

@Singleton
public class HdtQueryServiceImpl implements HdtQueryService, Closeable {
    private HDT hdt;
    private @NonNull final FlowType flowType;
    private @NonNull final Scheduler scheduler;

    @Inject
    public HdtQueryServiceImpl(@NonNull HDTLoaderUtil loader,
                               @Property(name = "sparql.hdt.flow", defaultValue = "REACTIVE")
                               @NonNull FlowType flowType,
                               @Named(QueryExecutionScheduler.NAME) @NonNull Scheduler scheduler)
            throws IOException {
        this.hdt = loader.load();
        this.flowType = flowType;
        this.scheduler = scheduler;
    }

    @RequiredArgsConstructor
    public static class SolutionIterator implements Iterator<SolutionRow> {
        private final @NonNull Dictionary dictionary;
        private final @NonNull IteratorTripleID source;
        private final @NonNull TermPosition[] order;
        private final @NonNull Predicate<TripleID> filter;
        private @Nullable SolutionRow next;


        private @NonNull SolutionRow lift(@NonNull TripleID tid) {
            int len = order.length;
            Term[] terms = new Term[len];
            for (int i = 0; i < len; i++)
                terms[i] = HDTUtils.fromHDTId(dictionary, HDTUtils.get(tid, order[i]), order[i]);
            return new SolutionRow(terms);
        }

        @Override public boolean hasNext() {
            while (next == null && source.hasNext()) {
                TripleID candidate = source.next();
                if (filter.test(candidate))
                    next = lift(candidate);
            }
            return next != null;
        }

        @Override public @NonNull SolutionRow next() {
            if (!hasNext())
                throw new NoSuchElementException();
            SolutionRow row = this.next;
            assert row != null;
            this.next = null;
            return row;
        }
    }

    private record NamesAndIt(@NonNull List<@NonNull String> names,
                              @NonNull SolutionIterator it) implements Iterable<SolutionRow> {
        @Override public @NonNull Iterator<SolutionRow> iterator() {
            return it;
        }
    }

    protected @NonNull NamesAndIt getSolutionIt(@NonNull TriplePattern query) {
        if (hdt == null)
            throw new IllegalStateException("HdtQueryServiceImpl is close()ed");
        var hdtIt = HDTUtils.queryIds(hdt, query);
        var vi = query.collectVarsInfo();
        var it = new SolutionIterator(hdt.getDictionary(), hdtIt, vi.positions(),
                                    HDTUtils.sharedVarIDFilter(vi.sharedVars()));
        return new NamesAndIt(query.varNames(), it);
    }

    @Override public @NonNull HDT hdt() {
        return hdt;
    }

    @Override public @NonNull QuerySolutions query(@NotNull TriplePattern query) {
        return query(query, flowType);
    }

    @Override public @NonNull QuerySolutions queryReactive(@NotNull TriplePattern query) {
        var ni = getSolutionIt(query);
        return new FluxQuerySolutions(ni.names, Flux.fromIterable(ni).publishOn(scheduler));
    }

    @Override public @NonNull QuerySolutions queryBatch(@NotNull TriplePattern query) {
        var ni = getSolutionIt(query);
        List<SolutionRow> list = new ArrayList<>();
        ni.it.forEachRemaining(list::add);
        return new BatchQuerySolutions(ni.names, list);
    }

    @Override public @NonNull QuerySolutions queryIterator(@NotNull TriplePattern query) {
        NamesAndIt ni = getSolutionIt(query);
        return new IteratorQuerySolutions(ni.names, ni.it);
    }

    @Override public void close() throws IOException {
        if (hdt != null) {
            hdt.close();
            hdt = null;
        }
    }
}
