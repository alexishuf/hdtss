package com.github.lapesd.hdtss.utils;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

import static com.github.lapesd.hdtss.utils.BitsetOps.nextSet;

public final class Binding {
    private final String @NonNull[] varNames;
    private Term @NonNull [] terms;

    private static boolean validInputs(@Nullable String @Nullable[] varNames, Term @Nullable[] terms) {
        if (varNames == null || terms == null) return false;
        if (varNames.length != terms.length) return false;
        for (String name : varNames) {
            if (name == null || name.isEmpty()) return false;
        }
        return true;
    }

    /**
     * Create a {@link Binding} mapping the i-th var name to the i-th {@link Term}.
     *
     * @param varNames Array of non-null and non-empty variable names. The resulting
     *                 {@link Binding} will keep a reference to the given {@code varNames} array.
     * @param terms Array of {@link Term}s corresponding to the given var names. The resulting
     *              {@link Binding} will keep a reference to the {@code terms} array.
     */
    public Binding(String @NonNull[] varNames, Term @NonNull [] terms) {
        assert validInputs(varNames, terms) : "Invalid varNames/terms";
        this.varNames = varNames;
        this.terms = terms;
    }

    /** Alias for {@code Binding(varNames.toArray(String[]::new), terms)} */
    public Binding(@NonNull List<@NonNull String> varNames, Term @NonNull [] terms) {
        this(varNames.toArray(String[]::new), terms);
    }

    /** Alias for {@code Binding(varNames, new Term[varNames.length])} */
    public Binding(@NonNull String @NonNull[] varNames) {
        this(varNames, new Term[varNames.length]);
    }

    /** Alias for {@code Binding(varNames.toArray(String[]::new))} */
    public Binding(@NonNull List<@NonNull String> varNames) {
        this(varNames.toArray(String[]::new));
    }

    /**
     * Create a {@link Binding} from a {@link Map} from var names to terms.
     *
     * @param var2terms map from var names to Term
     */
    public Binding(@NonNull Map<String, Term> var2terms) {
        int size = var2terms.size(), i = 0;
        varNames = new String[size];
        terms = new Term[size];
        for (String name : var2terms.keySet()) {
            varNames[i] = name;
            terms[i++] = var2terms.get(name);
        }
    }

    public @NonNull Binding filter(@NonNull Predicate<@NonNull String> varNamePredicate) {
        long[] selected = BitsetOps.createBitset(varNames.length);
        for (int i = 0; i < varNames.length; i++) {
            String name = varNames[i];
            if (varNamePredicate.test(name))
                BitsetOps.set(selected, i);
        }
        int selectedSize = BitsetOps.cardinality(selected);
        if (selectedSize == varNames.length)
            return this;
        String[] selectedVars = new String[selectedSize];
        Term[] selectedTerms = new Term[selectedSize];
        for (int o = 0, i = nextSet(selected, 0); i >= 0; i = nextSet(selected, i+1)) {
            selectedVars[o] = varNames[i];
            selectedTerms[o++] = terms[i];
        }
        return new Binding(selectedVars, selectedTerms);
    }

    public int     size()                        { return terms.length; }
    public boolean isEmpty()                     { return terms.length == 0; }
    public boolean contains(@Nullable String varName) { return indexOf(varName) >= 0; }
    public Term    get(int index)                { return terms[index]; }
    public String  var(int index)                { return varNames[index]; }

    public static class NoSuchVarException extends NoSuchElementException {
        public NoSuchVarException(String varName) { super(varName+" not found"); }
    }

    public @NonNull Binding setTerms(Term @NonNull[] terms) {
        if (terms.length != varNames.length) {
            var msg = "Bad length for terms: " + terms.length + ", expected " + varNames.length;
            throw new IllegalArgumentException(msg);
        }
        this.terms = terms;
        return this;
    }

    /**
     * Get a reference to the list of var names.
     *
     * <strong>Changes to the array will reflect in the {@link Binding}s object</strong>
     *
     * @return The array of non-null and non-empty var names backing this {@link Binding}.
     */
    public @NonNull String @NonNull[] vars() {
        return varNames;
    }

    /**
     * Get a reference to the list of terms.
     *
     * <strong>Changes to the array will reflect in the {@link Binding}s object</strong>
     *
     * @return The array of {@link Term}s backing this {@link Binding}.
     */
    public @Nullable Term @NonNull[] terms() {
        return terms;
    }

    /**
     * Get the {@link Term} mapped to {@code varName}.
     *
     * @param varName The case-sensitive var name to look for. If null, will not be found.
     * @return The {@link Term} mapped to that variable, which may be null.
     * @throws NoSuchVarException if {@code varName} is not present in this {@link Binding}.
     */
    public Term get(@NonNull String varName) throws NoSuchVarException {
        int i = indexOf(varName);
        if (i < 0) throw new NoSuchVarException(varName);
        return terms[i];
    }

    /** Get the {@link Term} mapped to {@code varName} or {@code fallback} if not found */
    public @Nullable Term get(@Nullable String varName, @Nullable Term fallback) {
        int i = indexOf(varName);
        return i < 0 ? fallback : terms[i];
    }

    /**
     * Find the index at which {@code varName} appears in this {@link Binding}.
     *
     * @param varName the var name to look for. Case-sensitive. If null, will not be found.
     * @return -1 if not found, else an index to use on {@link Binding#get(int)}.
     */
    public int indexOf(@Nullable String varName) {
        if (varName == null) return -1;
        for (int i = 0, size = varNames.length; i < size; i++) {
            if (varName.equals(varNames[i])) return i;
        }
        return -1;
    }

    /** Whether this {@link Binding} and {@code names} share at least one var name. */
    public boolean intersects(@NonNull Collection<String> names) {
        for (String name : varNames) {
            if (names.contains(name)) return true;
        }
        return false;
    }

    @Override public String toString() {
        StringBuilder b = new StringBuilder(terms.length * 64).append('{');
        for (int i = 0; i < terms.length; i++)
            b.append(varNames[i]).append('=').append(terms[i]).append(",  ");
        if (terms.length > 0)
            b.setLength(b.length()-3);
        return b.append('}').toString();
    }
}
