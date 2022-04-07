package com.github.lapesd.hdtss.model.nodes;

import lombok.Getter;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Set;

@Accessors(fluent = true)
public final class Slice extends AbstractOp {
    @Getter private final long limit, offset;

    public Slice(@NonNull Op op, long limit, long offset) {
        super(List.of(op));
        if (limit <= 0)
            throw new IllegalArgumentException("limit ("+limit+") <= 0");
        if (offset < 0)
            throw new IllegalArgumentException("offset ("+offset+") <= 0");
        this.limit = limit;
        this.offset = offset;
    }

    public @NonNull Op inner() { return children.get(0); }
    @Override public @NonNull Type type() { return Type.SLICE; }
    @Override public @NonNull List<@NonNull String> outputVars() { return children.get(0).outputVars(); }
    @Override public @NonNull Set<@NonNull String>   inputVars() { return children.get(0).inputVars(); }

    @Override public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        return new Slice(OpUtils.single(replacements), limit, offset);
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        if (!(other instanceof Slice s)) return false;
        return s.limit == limit && s.offset == offset && s.inner().deepEquals(inner());
    }

    @Override public @NonNull String toString() {
        return "Slice[lim="+limit+", off="+offset+"]("+inner()+")";
    }
}
