package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.utils.Binding;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;

public final class Project extends AbstractOp {
    public Project(@NonNull List<String> varNames, @NonNull Op inner) {
        super(List.of(inner));
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

    @Override public @NonNull Op bind(@NonNull Binding binding) {
        assert varNames != null;
        Op inner = children.get(0), bound = inner.bind(binding);
        ArrayList<String> remainingVars = new ArrayList<>(varNames.size());
        for (String name : varNames) {
            if (!binding.hasValue(name))
                remainingVars.add(name);
        }
        boolean change = bound != inner || remainingVars.size() < varNames.size();
        return change ? new Project(remainingVars, bound) : this;
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        if (!(other instanceof Project project)) return false;
        return project.outputVars().equals(outputVars()) && inner().deepEquals(project.inner());
    }

    @Override public @NonNull String toString() {
        return "Project"+varNames+"("+inner()+")";
    }
}
