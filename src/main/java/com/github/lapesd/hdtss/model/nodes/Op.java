package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.*;

/**
 * A node in a SPARQL algebra expression.
 */
public interface Op {
    enum Type {
        ASSIGN,
        DISTINCT,
        EXISTS,
        FILTER,
        IDENTITY,
        JOIN,
        LEFT_JOIN,
        LIMIT,
        MINUS,
        NOT_EXISTS,
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

    enum BindType {
        /**
         * Only bind triple patters (recursively). Do not recurse into
         * FILTER()s, FILTER (NOT) EXISTS, MINUS and sub-queries
         */
        ONLY_TRIPLES,
        /**
         * Recurse and bind into every node.
         */
        ALL
    }

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
     * @param bindType controls into which node types the bind will recurse into.
     * @return A SPARQL expression with mentioned variables replaced with the given {@link Term}s.
     */
    default @NonNull Op bind(@NonNull List<String> varNames, Term @NonNull[] row,
                             @NonNull BindType bindType) {
        if (row.length != varNames.size())
            throw new IllegalArgumentException("varNames.size() != row.length");
        else if (row.length == 0)
            return this;
        Map<String, Term> map = new HashMap<>();
        for (int i = 0; i < row.length; i++)
            map.put(varNames.get(i), row[i]);
        return bind(map, bindType);
    }

    /**
     * Calls {@link Op#bind(List, Term[], BindType)} with {@link BindType#ALL}.
     *
     * @param varNames forwarded to {@link Op#bind(List, Term[], BindType)}
     * @param row forwarded to {@link Op#bind(List, Term[], BindType)}
     * @return same as {@link Op#bind(List, Term[], BindType)}
     */
    default @NonNull Op bind(@NonNull List<String> varNames, Term @NonNull[] row) {
        return bind(varNames, row, BindType.ALL);
    }

    /**
     * Same effect as {@link Op#bind(List, Term[])}, but variable-{@link Term} bindings are
     * given in a {@link Map}.
     *
     * @param var2term a map from variable names (not including leading '?' or '$') to the
     *                 {@link Term}s that shall be used instead.
     * @param bindType controls into which node types the bind will recurse into.
     * @return a SPARQL expression with the given vars replaced with the given {@link Term}s.
     */
    default @NonNull Op bind(@NonNull Map<String, Term> var2term, @NonNull BindType bindType) {
        if (var2term.isEmpty())
            return this;
        ArrayList<String> names = new ArrayList<>(var2term.keySet());
        Term[] terms = new Term[names.size()];
        for (int i = 0; i < terms.length; i++)
            terms[i] = var2term.get(names.get(i));
        return bind(names, terms, bindType);
    }

    /**
     * Call {@link Op#bind(Map, BindType)} with {@link BindType#ALL}.
     *
     * @param var2term forwarded to {@link Op#bind(Map, BindType)}.
     * @return see {@link Op#bind(Map, BindType)}.
     */
    default @NonNull Op bind(@NonNull Map<String, Term> var2term) {
        return bind(var2term, BindType.ALL);
    }

    /**
     * A list of distinct var names exposed in solutions for this SPARQL query.
     *
     * @return a non-null, possibly empty list of non-null and non-empty
     * variable names (not including leading '?' nor '$').
     */
    @NonNull List<@NonNull String> varNames();

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
    default boolean deepEquals(@NonNull Op other) {
        return OpUtils.deepEquals(this, other);
    }
}
