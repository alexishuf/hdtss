package com.github.lapesd.hdtss.sparql.impl.join;

import com.github.lapesd.hdtss.model.nodes.Join;
import com.github.lapesd.hdtss.model.nodes.Op;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * A join reorder consists of:
 * <ul>
 *     <li>{@link JoinReorder#operands()}: a reorder of the original {@link Join#children()} </li>
 *     <li>{@link JoinReorder#projection}: Let {@code exVars} be the {@link Join#outputVars()} of
 *         the original {@link Join} operation and let
 *         {@code effVars = Join.of(operands()).varNames()}, then
 *         {@code exVars.get(i).equals(effVars.get(projection[i]))}.
 *         Instead of creating {@code projection[i] == i} for all i, projection should be null</li>
 * </ul>
 */
public record JoinReorder(@NonNull List<@NonNull Op> operands,
                          int @Nullable [] projection) {
}
