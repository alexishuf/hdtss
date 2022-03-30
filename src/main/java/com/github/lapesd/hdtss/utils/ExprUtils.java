package com.github.lapesd.hdtss.utils;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.compile;

public class ExprUtils {
    /* --- --- --- constants --- --- --- */
    private static final @NonNull Pattern S_RX = compile("(\"(\"\")?|'('')?|<[= ?$]?|\\?|\\$)");
    private static final @NonNull Pattern CL_SHORT_SINGLE_RX = compile("[^\\\\]'");
    private static final @NonNull Pattern CL_SHORT_DOUBLE_RX = compile("[^\\\\]\"");
    private static final @NonNull Pattern CL_LONG_SINGLE_RX = compile("'*'''");
    private static final @NonNull Pattern CL_LONG_DOUBLE_RX = compile("\"*\"\"\"");
    private static final @NonNull Pattern CL_IRI_RX = compile(">");

    private enum State {
        S {
            @Override public @NonNull Pattern pattern() {return S_RX;}
            @Override public @NonNull State next(@NonNull Matcher matchedMatcher) {
                return switch (matchedMatcher.group()) {
                    case "?", "$", "<?", "<$" -> VAR_NAME;
                    case "\"" -> CL_SHORT_DOUBLE;
                    case "\"\"\"" -> CL_LONG_DOUBLE;
                    case "'" -> CL_SHORT_SINGLE;
                    case "'''" -> CL_LONG_SINGLE;
                    case "<" -> CL_IRI;
                    case "< ", "<=" -> S;
                    default -> throw new IllegalStateException();
                };
            }
        },
        VAR_NAME {
            @Override public @NonNull Pattern pattern() {return TokenPatterns.VAR_NAME_RX;}
            @Override public @NonNull State next(@NonNull Matcher matchedMatcher) {return S;}
        },
        CL_SHORT_SINGLE {
            @Override public @NonNull Pattern pattern() {return CL_SHORT_SINGLE_RX;}
            @Override public @NonNull State next(@NonNull Matcher matchedMatcher) {return S;}
        },
        CL_SHORT_DOUBLE {
            @Override public @NonNull Pattern pattern() {return CL_SHORT_DOUBLE_RX;}
            @Override public @NonNull State next(@NonNull Matcher matchedMatcher) {return S;}
        },
        CL_LONG_SINGLE {
            @Override public @NonNull Pattern pattern() {return CL_LONG_SINGLE_RX;}
            @Override public @NonNull State next(@NonNull Matcher matchedMatcher) {return S;}
        },
        CL_LONG_DOUBLE {
            @Override public @NonNull Pattern pattern() {return CL_LONG_DOUBLE_RX;}
            @Override public @NonNull State next(@NonNull Matcher matchedMatcher) {return S;}
        },
        CL_IRI {
            @Override public @NonNull Pattern pattern() {return CL_IRI_RX;}
            @Override public @NonNull State next(@NonNull Matcher matchedMatcher) {return S; }
        };

        public abstract @NonNull Pattern pattern();
        public abstract @NonNull State next(@NonNull Matcher matchedMatcher);
    }

    public static @NonNull String bindExpr(@NonNull String filter,
                                           @NonNull Map<@NonNull String, Term> v2t) {
        StringBuilder b = new StringBuilder(filter.length());
        int consumed = 0;
        State st = State.S;
        var m = st.pattern().matcher(filter);
        while (m.find()) {
            b.append(filter, consumed, m.start());
            State next = st.next(m);
            switch (st) {
                case S -> {
                    if (next != State.VAR_NAME)
                        b.append(filter, m.start(), m.end());
                    else if (m.end()-m.start() > 1)
                        b.append(filter, m.start(), m.end()-1);
                }
                case VAR_NAME -> {
                    Term term = v2t.getOrDefault(m.group(), null);
                    if (term != null) b.append(term.sparql());
                    else              b.append(filter, m.start()-1, m.end());
                }
                default -> b.append(filter, m.start(), m.end());
            }
            m = (st = next).pattern().matcher(filter).region(consumed = m.end(), filter.length());
        }
        return b.append(filter, consumed, filter.length()).toString();
    }

    public static void findVarNames(@NonNull String filter, @NonNull Set<@NonNull String> set) {
        State st = State.S;
        var m = st.pattern().matcher(filter);
        while (m.find()) {
            if (st == State.VAR_NAME)
                set.add(m.group());
            m = (st = st.next(m)).pattern().matcher(filter).region(m.end(), filter.length());
        }
    }
}
