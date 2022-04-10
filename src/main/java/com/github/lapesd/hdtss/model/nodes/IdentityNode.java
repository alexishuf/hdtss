package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.utils.Binding;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Set;

/**
 * This node acts as an algebraic identity on operators.
 *
 * <ul>
 *     <li>If queried, part of a {@link Filter} or part of an {@link Union},
 *         it shall produce no solutions (not even an empty one)</li>
 *     <li>If part of a {@link Join}, shall produce one single empty solution with no variables,
 *         thus causing a cartesian product which has no multiplicative effect on the join.</li>
 * </ul>
 *
 * There should be a single instance of this node, {@link IdentityNode#INSTANCE}, which can be
 * also obtained via {@link IdentityNode#get()}. If more than one instance ends up exiting, all
 * are considered equal.
 */
public final class IdentityNode implements Op {
    public static @NonNull IdentityNode INSTANCE = new IdentityNode();

    private IdentityNode() {}

    /**
     * Get the singleton {@link IdentityNode}.
     * @return the non-null singleton {@link IdentityNode}.
     */
    public static @NonNull IdentityNode get() { return INSTANCE; }

    /**
     * Shorthand for {@code instanceof IdentityNode}.
     *
     * @param node the {@link Op} to test
     * @return true iff node is the {@link IdentityNode}.
     */
    public static boolean is(@NonNull Op node) {
        return node instanceof IdentityNode;
    }

    @Override public @NonNull Type type() {
        return Type.IDENTITY;
    }

    @Override public @NonNull List<@NonNull String> outputVars() {
        return List.of();
    }

    @Override public @NonNull Set<@NonNull String> inputVars() {
        return Set.of();
    }

    @Override public @NonNull List<@NonNull Op> children() {
        return List.of();
    }

    @Override public @NonNull Op bind(@NonNull Binding binding) {
        return this;
    }

    @Override public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        if (!replacements.isEmpty())
            throw new IllegalArgumentException("IdentityNode has no children by definition");
        return this;
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        return other instanceof IdentityNode;
    }

    /* --- --- --- Object methods --- --- --- */

    @Override public boolean equals(Object obj) {return obj instanceof IdentityNode;}
    @Override public int hashCode() {return 1;}
    @Override public @NonNull String toString() {return "IDENTITY";}
}
