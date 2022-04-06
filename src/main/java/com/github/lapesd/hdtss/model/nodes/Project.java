package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.List;

public class Project extends AbstractOp {
    public Project(@NonNull List<String> varNames, @NonNull Op inner) {
        super(Collections.singletonList(inner));
        this.varNames = varNames;
    }

    public @NonNull Op inner() {
        return children.get(0);
    }

    @Override public @NonNull Type type() {
        return Type.PROJECT;
    }

    @Override
    public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        return new Project(outputVars(), OpUtils.single(replacements));
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        if (!(other instanceof Project project)) return false;
        return project.outputVars().equals(outputVars()) && inner().deepEquals(project.inner());
    }

    @Override public @NonNull String toString() {
        return "Project"+varNames+"("+inner()+")";
    }
}
