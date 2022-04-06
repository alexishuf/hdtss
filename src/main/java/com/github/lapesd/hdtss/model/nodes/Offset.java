package com.github.lapesd.hdtss.model.nodes;

import lombok.Getter;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

@Accessors(fluent = true)
public class Offset extends AbstractOp {
    private final @Getter long offset;

    public Offset(long offset, @NonNull Op inner) {
        super(List.of(inner));
        this.offset = offset;
    }

    public @NonNull Op inner() {
        return children.get(0);
    }

    @Override public @NonNull List<@NonNull String> outputVars() {
        return children.get(0).outputVars();
    }

    @Override public @NonNull Type type() {
        return Type.OFFSET;
    }

    @Override public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        return new Offset(offset, OpUtils.single(replacements));
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        if (!(other instanceof Offset o)) return false;
        return o.offset == offset && o.inner().deepEquals(inner());
    }
}
