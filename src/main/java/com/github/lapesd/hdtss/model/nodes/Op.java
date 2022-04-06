package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A node in a SPARQL algebra expression.
 */
public interface Op {
    enum Type {
        ASK,
        ASSIGN,
        DISTINCT,
        EXISTS,
        FILTER,
        IDENTITY,
        JOIN,
        LEFT_JOIN,
        LIMIT,
        MINUS,
        OFFSET,
        PROJECT,
        TRIPLE,
        UNION,
        VALUES;

        private static final @NonNull Set<Type> VALUE_SET = Set.of(values());
        public static @NonNull Set<Type> valueSet() { return VALUE_SET; }
    }

    /**
     * Get the node type
     *
     * @return a non-null {@link Type}.
     */
    @NonNull Type type();

    /**
     * Create a new SPARQL algebra expression that replaces any variables in {@code varNames}
     * with the values in {@code row}.
     *
     * The returned expression may be this same instance if no change is made as result of
     * the bind.
     *
     * @param varNames a list of variable names not including the leading '?' or '$'.
     * @param row Term assignments for the variables in {@code varNames}. The i-th {@link Term}
     *            corresponds to the i-th variable name.
     * @return A SPARQL expression with mentioned variables replaced with the given {@link Term}s.
     */
    @NonNull Op bind(@NonNull List<String> varNames, Term @NonNull[] row);

    /**
     * Same effect as {@link Op#bind(List, Term[])}, but variable-{@link Term} bindings are
     * given in a {@link Map}.
     *
     * @param var2term a map from variable names (not including leading '?' or '$') to the
     *                 {@link Term}s that shall be used instead.
     * @return a SPARQL expression with the given vars replaced with the given {@link Term}s.
     */
    @NonNull Op bind(@NonNull Map<String, Term> var2term);

    /**
     * A list of distinct var names exposed in solutions for this SPARQL query.
     *
     * <strong>This list is orderred in the same order as columns of result rows</strong>.
     *
     * @return a non-null, possibly empty list of non-null and non-empty
     * variable names (not including leading '?' nor '$').
     */
    @NonNull List<@NonNull String> outputVars();

    /**
     * The variables that would never be bound if this node were the root of the SPARQL algebra.
     *
     * Examples:
     * <ul>
     *     <li>{@code [x]} in {@code ?x <p> <o> FILTER(?s = ?x)}</li>
     *     <li>{@code []} in {@code SELECT ?x {?x <p> ?y} MINUS {<a> <b> ?z FILTER(?z > ?y)}}</li>
     *     <li>{@code [z]} in {@code SELECT ?x {?x <p> ?y} MINUS {<a> <b> ?x FILTER(?z > ?y)}}</li>
     * </ul>
     *
     * Note that:
     * <ol>
     *     <li>A variable in this set at a node may not be present in this set for a parent node</li>
     *     <li>A variable being projected-out is not sufficient condition for inclusion in this
     *         list: if the variable is assigned during evaluation of intermediate results it does
     *         not belong here.</li>
     * </ol>
     *
     *
     * If this node is a {@link Filter}, variables of its own filter expressions may appear in
     * this set.
     *
     * @return A non-null immutable set of non-null and non-empty variable names
     *         (no leading '?' or '$').
     */
    @NonNull Set<@NonNull String> inputVars();

    /**
     * Get all <strong>direct</strong> child operands of this node.
     *
     * @return a non-null, possibly empty list of non-null {@link Op}s
     */
    @NonNull List<@NonNull Op> children();

    /**
     * Create a new {@link Op} of exact same class with the given children.
     *
     * Subclass-specific constraints may apply to the members and to the size of the
     * given {@code replacements} list.
     *
     * @param replacements the new list of direct children of this node.
     * @return a new {@link Op}
     */
    @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements);

    /**
     * Compare the SPARQL expression root at this node with the one rooted at {@code other}.
     *
     * The comparison is for euqality and not equivalence. Queries with same semantics but
     * different representation (e.g., re-ordered children in some node) are not equal.
     *
     * This comparison is deep and thus costly. In contrast, the {@link Op#equals(Object)}
     * and {@link Op#hashCode()} methods compare by reference for all {@link Op}s
     * except for {@link TriplePattern}.
     *
     * @param other other SPARQL algebra tree to compare against.
     * @return true iff both algebra expressions are exactly equal.
     */
    boolean deepEquals(@NonNull Op other);
}
