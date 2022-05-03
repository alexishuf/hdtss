package com.github.lapesd.hdtss.sparql.impl;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.utils.Binding;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Singleton @Slf4j
public class DefaultOpExecutorDispatcher implements OpExecutorDispatcher {
    private final @NonNull Provider<OpExecutor> tripleExecutor;
    private final @NonNull Provider<OpExecutor> filterExecutor;
    private final @NonNull Provider<OpExecutor> projectExecutor;
    private final @NonNull Provider<OpExecutor> distinctExecutor;
    private final @NonNull Provider<OpExecutor> weakDistinctExecutor;
    private final @NonNull Provider<OpExecutor> unionExecutor;
    private final @NonNull Provider<OpExecutor> joinExecutor;
    private final @NonNull Provider<OpExecutor> leftJoinExecutor;
    private final @NonNull Provider<OpExecutor> identityExecutor;
    private final @NonNull Provider<OpExecutor> sliceExecutor;
    private final @NonNull Provider<OpExecutor> valuesExecutor;
    private final @NonNull Provider<OpExecutor> assignExecutor;
    private final @NonNull Provider<OpExecutor> existsExecutor;
    private final @NonNull Provider<OpExecutor> minusExecutor;
    private final @NonNull Provider<OpExecutor> askExecutor;
    private @Nullable Map<Op.@NonNull Type, @NonNull OpExecutor> executorMap;

    @Inject
    public DefaultOpExecutorDispatcher(@Named("triple")        @NonNull Provider<OpExecutor> tripleExecutor,
                                       @Named("filter")        @NonNull Provider<OpExecutor> filterExecutor,
                                       @Named("project")       @NonNull Provider<OpExecutor> projectExecutor,
                                       @Named("distinct")      @NonNull Provider<OpExecutor> distinctExecutor,
                                       @Named("weakDistinct")  @NonNull Provider<OpExecutor> weakDistinctExecutor,
                                       @Named("union")         @NonNull Provider<OpExecutor> unionExecutor,
                                       @Named("join")          @NonNull Provider<OpExecutor> joinExecutor,
                                       @Named("leftJoin")      @NonNull Provider<OpExecutor> leftJoinExecutor,
                                       @Named("identity")      @NonNull Provider<OpExecutor> identityExecutor,
                                       @Named("slice")         @NonNull Provider<OpExecutor> sliceExecutor,
                                       @Named("values")        @NonNull Provider<OpExecutor> valuesExecutor,
                                       @Named("assign")        @NonNull Provider<OpExecutor> assignExecutor,
                                       @Named("exists")        @NonNull Provider<OpExecutor> existsExecutor,
                                       @Named("minus")         @NonNull Provider<OpExecutor> minusExecutor,
                                       @Named("ask")           @NonNull Provider<OpExecutor> askExecutor) {
        this.tripleExecutor = tripleExecutor;
        this.filterExecutor = filterExecutor;
        this.projectExecutor = projectExecutor;
        this.distinctExecutor = distinctExecutor;
        this.weakDistinctExecutor = weakDistinctExecutor;
        this.unionExecutor = unionExecutor;
        this.joinExecutor = joinExecutor;
        this.leftJoinExecutor = leftJoinExecutor;
        this.identityExecutor = identityExecutor;
        this.sliceExecutor = sliceExecutor;
        this.valuesExecutor = valuesExecutor;
        this.assignExecutor = assignExecutor;
        this.existsExecutor = existsExecutor;
        this.minusExecutor = minusExecutor;
        this.askExecutor = askExecutor;
    }

    @Override  public void init() {
        long startNs = System.nanoTime();
        Map<Op. @NonNull Type, @NonNull OpExecutor> map = new HashMap<>();
        map.put(Op.Type.TRIPLE,        this.tripleExecutor.get());
        map.put(Op.Type.FILTER,        this.filterExecutor.get());
        map.put(Op.Type.PROJECT,       this.projectExecutor.get());
        map.put(Op.Type.DISTINCT,      this.distinctExecutor.get());
        map.put(Op.Type.WEAK_DISTINCT, this.weakDistinctExecutor.get());
        map.put(Op.Type.UNION,         this.unionExecutor.get());
        map.put(Op.Type.JOIN,          this.joinExecutor.get());
        map.put(Op.Type.LEFT_JOIN,     this.leftJoinExecutor.get());
        map.put(Op.Type.IDENTITY,      this.identityExecutor.get());
        map.put(Op.Type.SLICE,         this.sliceExecutor.get());
        map.put(Op.Type.VALUES,        this.valuesExecutor.get());
        map.put(Op.Type.ASSIGN,        this.assignExecutor.get());
        map.put(Op.Type.EXISTS,        this.existsExecutor.get());
        map.put(Op.Type.MINUS,         this.minusExecutor.get());
        map.put(Op.Type.ASK,           this.askExecutor.get());
        assert Arrays.stream(Op.Type.values()).allMatch(map::containsKey);
        this.executorMap = map;
        double ms = (System.nanoTime() - startNs)/1000000.0;
        log.debug("OpExecutorDispatcher.init() took {}", String.format("%.3fms", ms));
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        if (executorMap == null)
            init();
        QuerySolutions solutions = executorMap.get(node.type()).execute(node);
        // multiple OpExecutor implementations rely on this:
        assert solutions.varNames().equals(node.outputVars());
        return solutions;
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node, @Nullable Binding binding) {
        if (executorMap == null)
            init();
        QuerySolutions solutions = executorMap.get(node.type()).execute(node, binding);
        assert validOutputVars(solutions, node, binding);
        return solutions;
    }

    private boolean validOutputVars(@NonNull QuerySolutions solutions, @NonNull Op node,
                                    @Nullable Binding binding) {
        var expected = binding == null ? node.outputVars() : binding.unbound(node.outputVars());
        return solutions.varNames().equals(expected);
    }
}
