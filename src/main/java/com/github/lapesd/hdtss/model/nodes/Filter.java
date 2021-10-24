package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.compile;

public final class Filter extends AbstractOp {
    /* --- --- --- constants --- --- --- */

    private static final @NonNull Pattern VAR_NAME_RX = compile("[a-zA-Z_0-9\u00B7\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0300-\u037D\u037F-\u1FFF\u200C\u200D\u203F-\u2040\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD\u10000-\uEFFFF]+");
    private static final @NonNull Pattern S_RX = compile("(\"(\"\")?|'('')?|<[= ?$]?|\\?|\\$)");
    private static final @NonNull Pattern CL_SHORT_SINGLE_RX = compile("[^\\\\]'");
    private static final @NonNull Pattern CL_SHORT_DOUBLE_RX = compile("[^\\\\]\"");
    private static final @NonNull Pattern CL_LONG_SINGLE_RX = compile("'*'''");
    private static final @NonNull Pattern CL_LONG_DOUBLE_RX = compile("\"*\"\"\"");
    private static final @NonNull Pattern CL_IRI_RX = compile(">");
    private static final @NonNull Term S0 = new Term("$0");

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
            @Override public @NonNull Pattern pattern() {return VAR_NAME_RX;}
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
    private static final Pattern WRAPPER_RX = compile("(?i)^\\s*FILTER\\((.*)\\)\\s*$");

    /* --- --- --- instance fields and constructor --- --- --- */
    private final @NonNull List<@NonNull String> filters;
    private @Nullable Set<String> filtersVarNames;

    public Filter(@NonNull Op inner,
                  @NonNull List<@NonNull String> filters) {
        super(Collections.singletonList(inner));
        this.filters = filters;
        assert filters.stream().noneMatch(WRAPPER_RX.asPredicate())
                : "Filter expressions must not be wrapped with FILTER()";
    }

    public Filter(@NonNull Op inner, String... filters) {
        this(inner, Arrays.asList(filters));
    }

    public static @NonNull Filter withFilters(@NonNull Op inner, @NonNull String... filters) {
        return withFilters(inner, Arrays.asList(filters));
    }
    public static @NonNull Filter withFilters(@NonNull Op inner,
                                              @NonNull List<@NonNull String> filters) {
        List<String> list;
        if (inner instanceof Filter o) {
            inner = o.inner();
            (list = new ArrayList<>(o.filters.size() + filters.size())).addAll(o.filters);
        } else {
            list = new ArrayList<>(filters.size());
        }
        list.addAll(filters);
        return new Filter(inner, list);
    }

    public @NonNull Op inner() {
        return children.get(0);
    }

    public @NonNull List<@NonNull String> filters() {
        return filters;
    }

    /**
     * Get the set of variables mentioned in filter expressions.
     *
     * @return a non-null, possibly empty set of non-null and non-empty variable names
     *         (not including leading '?' and '$').
     */
    public @NonNull Set<@NonNull String> filtersVarNames() {
        if (filtersVarNames == null) {
            Set<@NonNull String> set = new HashSet<>();
            for (String filter : filters)
                findVarNames(filter, set);
            filtersVarNames = Collections.unmodifiableSet(set);
        }
        return filtersVarNames;
    }

    /* --- --- --- Method implementations --- --- --- */

    @Override public @NonNull Type type() {
        return Type.FILTER;
    }

    @Override public @NonNull List<@NonNull String> varNames() {
        return children.get(0).varNames();
    }

    @Override public @NonNull Op bind(@NonNull Map<String, Term> v2t,
                                      @NonNull BindType bindType) {
        if (v2t.isEmpty())
            return this;
        if (bindType == BindType.ONLY_TRIPLES)
            return new Filter(inner().bind(v2t, bindType), filters);
        List<@NonNull String> boundFilters = new ArrayList<>(filters.size());
        for (String filter : filters)
            boundFilters.add(bindFilter(filter, v2t));
        return new Filter(inner().bind(v2t), boundFilters);
    }

    @Override
    public @NonNull Op bind(@NonNull List<String> varNames, Term @NonNull [] row,
                            @NonNull BindType bindType) {
        if (row.length != varNames.size())
            throw new IllegalArgumentException("varNames.size() != row.length");
        else if (row.length == 0)
            return this;
        Map<String, Term> v2t = new HashMap<>();
        for (int i = 0; i < row.length; i++) v2t.put(varNames.get(i), row[i]);
        return bind(v2t, bindType);
    }

    @Override
    public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        Op replacement = OpUtils.single(replacements);
        return replacement instanceof Filter f ? flatten(f) : new Filter(replacement, filters);
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        if (!(other instanceof Filter rhs)) return false;
        return filters.equals(rhs.filters) && children.get(0).deepEquals(rhs.children.get(0));
    }

    @Override public @NonNull String toString() {
        return "Filter"+filters+"("+inner()+")";
    }

    @NonNull Filter flatten(@NonNull Filter child) {
        if (child.inner() instanceof Filter f)
            child = child.flatten(f);
        List<String> filterUnion = new ArrayList<>(filters.size() + child.filters.size());
        filterUnion.addAll(filters);
        filterUnion.addAll(child.filters);
        return new Filter(child.inner(), filterUnion);
    }

    /* --- --- --- private methods --- --- --- */

    static @NonNull String bindFilter(@NonNull String filter, @NonNull Map<@NonNull String, Term> v2t) {
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

    static void findVarNames(@NonNull String filter, @NonNull Set<@NonNull String> set) {
        State st = State.S;
        var m = st.pattern().matcher(filter);
        while (m.find()) {
            if (st == State.VAR_NAME)
                set.add(m.group());
            m = (st = st.next(m)).pattern().matcher(filter).region(m.end(), filter.length());
        }
    }

}
