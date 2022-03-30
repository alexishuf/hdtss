package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.TermPosition;
import io.micronaut.core.annotation.Introspected;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.validation.constraints.NotNull;
import java.util.*;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

@Introspected
@Accessors(fluent = true)
@RequiredArgsConstructor @EqualsAndHashCode
public final class TriplePattern implements Op {
    private final @Getter @NotNull Term subject;
    private final @Getter @NotNull Term predicate;
    private final @Getter @NotNull Term object;
    private @Nullable @EqualsAndHashCode.Exclude VarsInfo varsInfo = null;
    private @Nullable @EqualsAndHashCode.Exclude List<String> varNames = null;

    public TriplePattern(@lombok.NonNull @NonNull CharSequence subject,
                         @lombok.NonNull @NonNull CharSequence predicate,
                         @lombok.NonNull @NonNull CharSequence object) {
        this(new Term(subject), new Term(predicate), new Term(object));
    }

    @Override public @NonNull List<@NonNull String> varNames() {
        if (varNames == null)
            varNames = Arrays.asList(collectVarsInfo().names);
        return varNames;
    }

    @Override public @NonNull Set<@NonNull String> inputVars() {
        return Set.of();
    }

    @Override public @NonNull Type type() {
        return Type.TRIPLE;
    }

    @Override public @NonNull List<@NonNull Op> children() {
        return Collections.emptyList();
    }

    @Override public @NonNull Op bind(@NonNull List<String> varNames, Term @NonNull [] row) {
        Term[] terms = null;
        VarsInfo info = collectVarsInfo();
        TermPosition f = info.sharedVars.first();
        for (int i = 0, varNamesSize = varNames.size(); i < varNamesSize; i++) {
            String name = varNames.get(i);
            for (int j = 0; j < info.names.length; j++) {
                if (info.names[j].equals(name)) {
                    if (terms == null) terms = toArray();
                    terms[info.positions[j].ordinal()] = row[i];
                    if (info.positions[j] == f) {
                        terms[requireNonNull(info.sharedVars.second()).ordinal()] = row[i];
                        if (info.sharedVars.third() != null)
                            terms[info.sharedVars.third().ordinal()] = row[i];
                    }
                }
            }
        }
        if (terms != null)
            return new TriplePattern(terms[0], terms[1], terms[2]);
        return this;
    }

    @Override public @NonNull Op bind(@NonNull Map<@NonNull String, Term> var2term) {
        VarsInfo info = collectVarsInfo();
        TermPosition f = info.sharedVars.first();
        Term[] bound = null;
        for (int i = 0; i < info.names.length; i++) {
            Term term = var2term.getOrDefault(info.names[i], null);
            if (term != null) {
                if (bound == null) bound = toArray();
                bound[info.positions[i].ordinal()] = term;
                if (info.positions[i] == f) {
                    bound[requireNonNull(info.sharedVars.second()).ordinal()] = term;
                    if (info.sharedVars.third() != null)
                        bound[info.sharedVars.third().ordinal()] = term;
                }
            }
        }
        if (bound != null)
            return new TriplePattern(bound[0], bound[1], bound[2]);
        return this;
    }

    @Override public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        if (!replacements.isEmpty())
            throw new IllegalArgumentException("A TriplePattern can have no children");
        return this;
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        return equals(other);
    }

    public @NonNull Term get(@NonNull TermPosition position) {
        return switch (position) {
            case SUB -> subject;
            case PRE -> predicate;
            case OBJ -> object;
        };
    }

    public @NonNull Term[] toArray() { return new Term[] {subject, predicate, object}; }

    public @NonNull Stream<Term> stream() {
        return Stream.of(subject, predicate, object);
    }

    /**
     * All possible situations where a some variable appears in more than one position.
     */
    public enum SharedVars {
        NONE,  // 0
        SP,    // 1
        SO,    // 2
        PO,    // 3
        ALL;   // 4

        public @Nullable TermPosition first() {
            return switch (this) {
                case SP, SO, ALL -> TermPosition.SUB;
                case PO          -> TermPosition.PRE;
                default          -> null;
            };
        }

        public @Nullable TermPosition second() {
            return switch (this) {
                case SP, ALL -> TermPosition.PRE;
                case SO, PO  -> TermPosition.OBJ;
                default -> null;
            };
        }

        public @Nullable TermPosition third() {
            return this == ALL ? TermPosition.OBJ : null;
        }

        public @NonNull SharedVars add(@NonNull SharedVars other) {
            int mine = ordinal(), theirs = other.ordinal(), sum = mine + theirs;
            return sum == 3 && mine > 0 && theirs > 0 ? ALL : values()[Math.min(sum, 4)];
        }

        public static @NonNull SharedVars valueOf(@NonNull TermPosition a,
                                                  @NonNull TermPosition b) {
            return values()[a.ordinal() + b.ordinal()];
        }
    }

    /**
     * A combo of var names in this {@link TriplePattern} an array of {@link TermPosition}s
     * where there the named variables are and a {@link SharedVars} denoting variables that
     * occur in more than one position..
     *
     * Both the {@link VarsInfo#names()} list and the
     * {@link VarsInfo#positions()} array share the same order: subject, predicate
     * and object (terms which are not variables are not included).
     */
    public record VarsInfo(@NonNull String @NonNull [] names,
                           @NonNull TermPosition @NonNull [] positions,
                           @NonNull SharedVars sharedVars) {
        public static final TriplePattern.@NonNull VarsInfo EMPTY =
                new VarsInfo(new String[0], new TermPosition[0], SharedVars.NONE);

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof VarsInfo varsInfo)) return false;
            return Arrays.equals(names, varsInfo.names)
                    && Arrays.equals(positions, varsInfo.positions)
                    && sharedVars == varsInfo.sharedVars;
        }

        @Override public int hashCode() {
            int result = Objects.hash(sharedVars);
            result = 31 * result + Arrays.hashCode(names);
            result = 31 * result + Arrays.hashCode(positions);
            return result;
        }

        @Override public @NonNull String toString() {
            return "VarsInfo{" +
                    "names=" + Arrays.toString(names) +
                    ", positions=" + Arrays.toString(positions) +
                    ", sharedVars=" + sharedVars +
                    '}';
        }
    }

    /**
     * Create a {@link VarsInfo} object for this {@link TriplePattern}
     * @return a non-null {@link VarsInfo}
     */
    public @NonNull VarsInfo collectVarsInfo() {
        if (varsInfo != null)
            return varsInfo;
        int bits = (subject.isVar()   ? 0x4 : 0)
                 | (predicate.isVar() ? 0x2 : 0)
                 | (object.isVar()    ? 0x1 : 0);
        if (bits == 0)
            return VarsInfo.EMPTY;
        SharedVars sv = SharedVars.NONE;
        if ((bits & 0x6) == 0x6 && subject  .content().equals(predicate.content())) {
            bits &= ~0x2;
            sv = sv.add(SharedVars.SP);
        }
        if ((bits & 0x5) == 0x5 && subject  .content().equals(object   .content())) {
            bits &= ~0x1;
            sv = sv.add(SharedVars.SO);
        }
        if ((bits & 0x3) == 0x3 && predicate.content().equals(object   .content())) {
            bits &= ~0x1;
            sv = sv.add(SharedVars.PO);
        }

        final TermPosition[] termPositions = TermPosition.values();
        int size = Integer.bitCount(bits);
        TermPosition[] ps = new TermPosition[size];
        String[] names = new String[size];
        for (int i = 0; i < size; i++) {
            int hi = Integer.highestOneBit(bits);
            bits &= ~hi;
            TermPosition position = termPositions[((hi & 0x2) >> 1) | ((hi & 0x1) << 1)];
            ps[i] = position;
            names[i] = get(position).content().toString();
        }
        return varsInfo = new VarsInfo(names, ps, sv);
    }

    @Override public @NonNull String toString() {
        return subject.sparql().toString() + ' ' + predicate.sparql() + ' ' + object.sparql();
    }
}
