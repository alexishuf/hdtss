package com.github.lapesd.hdtss.model.nodes;

import lombok.Getter;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.List;

@Accessors(fluent = true)
public class Limit extends AbstractOp {
    @Getter long limit;

    public Limit(long limit, @NonNull Op child) {
        super(Collections.singletonList(child));
        if (limit < 0)
            throw new IllegalArgumentException("limit must be >= 0");
        this.limit = limit;
    }

    public @NonNull Op inner() {
        return children.get(0);
    }

    @Override public @NonNull Type type() {
        return Type.LIMIT;
    }

    @Override public @NonNull List<@NonNull String> outputVars() {
        return children.get(0).outputVars();
    }

    @Override public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        return new Limit(limit, OpUtils.single(replacements));
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        if (!(other instanceof Limit l)) return false;
        return l.limit == limit && l.inner().deepEquals(inner());
    }

    @Override public @NonNull String toString() {
        return "Limit("+limit+", "+children.get(0)+")";
    }
}
