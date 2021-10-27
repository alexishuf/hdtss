package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class OpUtils {

    public static @NonNull Op single(@NonNull Collection<? extends Op> coll) {
        return maybeSingle(coll)
                .orElseThrow(() -> new IllegalArgumentException("Expected a single node"));
    }

    public static @NonNull Optional<Op> maybeSingle(@NonNull Collection<? extends Op> coll) {
        Op selected = IdentityNode.INSTANCE;
        for (Op op : coll) {
            if (op != null && !IdentityNode.is(op)) {
                if (!IdentityNode.is(selected))
                    return Optional.empty();
                selected = op;
            }
        }
        return Optional.of(selected);
    }

    public static @NonNull List<@NonNull Op> flattenOrCopy(@NonNull Class<? extends Op> parentCls,
                                                           @NonNull List<@NonNull Op> list) {
        List<@NonNull Op> inc = incorporate(parentCls, list);
        return inc == null ? new ArrayList<>(list) : inc;
    }

    public static  @NonNull List<@NonNull Op>
    flatten(@NonNull Class<? extends Op> parentClass, @NonNull List<@NonNull Op> list) {
        List<@NonNull Op> inc = incorporate(parentClass, list);
        return inc == null ? list : inc;
    }

    public static @NonNull Op flatten(@NonNull Op node) {
        List<Op> children = node.children();
        List<Op> replacements = null;
        for (int i = 0, size = children.size(); i < size; i++) {
            Op child = children.get(i);
            Op replaced = flatten(child);
            //noinspection ObjectEquality
            if (replaced != child) {
                if (replacements == null)
                    replacements = new ArrayList<>(children);
                replacements.set(i, replaced);
            }
        }
        return flatten(node, replacements);
    }

    private static @Nullable List<@NonNull Op>
    incorporate(@NonNull Class<? extends Op> cls,
                @NonNull List<@NonNull Op> children) {
        int size = children.size();
        List<@NonNull Op> flattened = null;
        for (int i = 0; i < size; i++) {
            Op child = children.get(i);
            if (cls.isAssignableFrom(child.getClass()) || child instanceof IdentityNode) {
                var grandchildren = child.children();
                if (flattened == null) {
                    flattened = new ArrayList<>(size+ grandchildren.size());
                    for (int j = 0; j < i; j++)
                        flattened.add(children.get(j));
                }
                flattened.addAll(grandchildren);
            } else if (flattened != null) {
                flattened.add(child);
            }
        }
        if (flattened != null && flattened.isEmpty())
            return List.of(IdentityNode.INSTANCE);
        return flattened;
    }

    private static @NonNull Op flatten(@NonNull Op parent,
                                       @Nullable List<Op> replacements) {
        var effChildren = replacements == null ? parent.children() : replacements;
        if (effChildren.isEmpty())
            return parent;
        if (parent instanceof Union || parent instanceof Join) {
            var flattened = incorporate(parent.getClass(), effChildren);
            if (flattened != null)
                replacements = flattened;
        } else if (parent instanceof Filter filter) {
            Op operand = effChildren.get(0);
            if (operand instanceof IdentityNode)
                return IdentityNode.INSTANCE;
            else if (operand instanceof Filter inner)
                return filter.flatten(inner);
        }
        if (replacements != null) {
            if (replacements.isEmpty())
                return IdentityNode.INSTANCE;
            return parent.withChildren(replacements);
        }
        return parent;
    }
}
