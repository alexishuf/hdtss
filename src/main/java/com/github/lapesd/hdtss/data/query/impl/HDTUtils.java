package com.github.lapesd.hdtss.data.query.impl;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.TermPosition;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.iterator.DictionaryTranslateIterator;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.triples.impl.EmptyTriplesIterator;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

public class HDTUtils {

    /**
     * Get a Term from a string obtained from HDT.
     *
     * HDT stores URIs without surrounding {@code <>}s and does not name variables, using
     * an empty string as a placeholder.
     *
     * @param hdtString a string that may have been obtained from an HDT iterator.
     * @return a non-null and non-empty {@link Term}, wrapping the proper NT (or SPARQL's
     *         {@code []} in case of placeholders) representation.
     */
    public static @NonNull Term fromHDT(@NonNull CharSequence hdtString) {
        char first = hdtString.isEmpty() ? '\0' : hdtString.charAt(0);
        return switch (first) {
            case '"' -> Term.fromNonXSDAbbrevDoubleQuotedUnescapedLiteral(hdtString);
            case '_', '\0' -> Term.fromBlank(hdtString);
            default -> Term.fromURI(hdtString);
        };
    }

    /**
     * Convert a NT or SPARQL (for {@code []} and vars) Term into a string as expected by HDT.
     *
     * @param term the {@link Term} to convert
     * @return the equivalent string that would be indexed in HDT.
     */
    public static @NonNull String toHDT(@NonNull Term term) {
        if      (!term.isGround()       ) return "";
        else if ( term.isURI()          ) return term.content().toString();
        else if ( term.isStringLiteral()) return term.quoted().toString();
        return term.sparql().toString();
    }

    /**
     * Convert a {@link Term} to a string ID within an HDT file.
     *
     * @param dictionary The HDT dictionary
     * @param term The {@link Term} to lookup
     * @param role The position of the Term in triples to be matched.
     * @return the {@code > 0} ID, a zero if Term is a blank node or variable or -1 if there is
     *         no triple in the HDT file with the given term at the given position.
     */
    public static long toHDTId(@NonNull Dictionary dictionary, @NonNull Term term,
                               @NonNull TripleComponentRole role) {
        var string = toHDT(term);
        if (string.isEmpty())
            return 0;
        var id = dictionary.stringToId(string, role);
        return id > 0 ? id : -1; // -1 means no match
    }

    public static @NonNull TripleComponentRole pos2role(@NonNull TermPosition pos) {
        return switch (pos) {
            case SUB -> TripleComponentRole.SUBJECT;
            case PRE -> TripleComponentRole.PREDICATE;
            case OBJ -> TripleComponentRole.OBJECT;
        };
    }

    /**
     * Convert an HDT id to a {@link Term}
     *
     * @param dictionary The HDT {@link Dictionary} where the id originated
     * @param id an ID
     * @param position the {@link TermPosition} of the {@link Term} to which the ID refers to.
     * @return a non-null and non-empty {@link Term}. Zero ids (i.e., wildcards) are converted
     *        to unlabeled blank nodes
     */
    public static @NonNull Term fromHDTId(@NonNull Dictionary dictionary, long id,
                                          @NonNull TermPosition position) {
        return fromHDT(dictionary.idToString(id, pos2role(position)).toString());
    }

    /**
     * Query a {@link TriplePattern} against an HDT file and iterate of triples of strings IDs.
     *
     * Note that HDT has no concept of variable names. Thus, if the same variable appears twice
     * in the {@link TriplePattern}, the resulting iterator will scan all matching triples of
     * an otherwise equivalent {@link TriplePattern} where no variable appears more than once.
     *
     * @param hdt the queryable HDT file.
     * @param tp the query to run
     * @return A non-null, possibly empty {@link IteratorTripleString} over
     *         {@link TripleID}s, which are triplets of string IDs.
     */
    public static @NonNull IteratorTripleID queryIds(@NonNull HDT hdt,
                                                     @NonNull TriplePattern tp) {
        var dict = hdt.getDictionary();
        long s = toHDTId(dict, tp.subject(), TripleComponentRole.SUBJECT);
        if (s == -1)
            return new EmptyTriplesIterator(TripleComponentOrder.SPO);
        long p = toHDTId(dict, tp.predicate(), TripleComponentRole.PREDICATE);
        if (p == -1)
            return new EmptyTriplesIterator(TripleComponentOrder.SPO);
        long o = toHDTId(dict, tp.object(), TripleComponentRole.OBJECT);
        if (o == -1)
            return new EmptyTriplesIterator(TripleComponentOrder.SPO);
        return hdt.getTriples().search(new TripleID(s, p, o));
    }

    /**
     * Equivalent to {@link HDTUtils#queryIds(HDT, TriplePattern)}, but will iterate over
     * {@link TripleString}s (triples of strings instead of triples of longs)
     *
     * @param hdt the HDT file to query
     * @param tp the query to run
     * @return A non-null, possibly empty {@link IteratorTripleString}.
     */
    public static @NonNull IteratorTripleString query(@NonNull HDT hdt,
                                                      @NonNull TriplePattern tp) {
        try {
            return hdt.search(toHDT(tp.subject()), toHDT(tp.predicate()), toHDT(tp.object()));
        } catch (NotFoundException e) {
            var idIterator = new EmptyTriplesIterator(TripleComponentOrder.SPO);
            return new DictionaryTranslateIterator(idIterator, hdt.getDictionary());
        }
    }

    private static final @NonNull List<Predicate<TripleString>> TS_FILTERS = Arrays.asList(
            ts -> true,                                        // NONE
            ts -> ts.  getSubject().equals(ts.getPredicate()), // SP
            ts -> ts.  getSubject().equals(ts.   getObject()), // SP
            ts -> ts.getPredicate().equals(ts.   getObject()), // PO
            ts -> {                                            // ALL
                var s = ts.getSubject();
                return s.equals(ts.getPredicate()) && s.equals(ts.getObject()); // ALL
            }
    );
    private static final @NonNull List<Predicate<TripleID>> TI_FILTERS = Arrays.asList(
            ts -> true,                                   // NONE
            ts -> ts.  getSubject() == ts.getPredicate(), // SP
            ts -> ts.  getSubject() == ts.   getObject(), // SP
            ts -> ts.getPredicate() == ts.   getObject(), // PO
            ts -> {                                       // ALL
                var s = ts.getSubject();
                return s == ts.getPredicate() && s == ts.getObject(); // ALL
            }
    );

    /**
     * Get a {@link Predicate} that tests whether a {@link TripleString} satisfies the variable
     * co-references within the queried triple pattern (i.e. same variable in 2+ term positions).
     *
     * HDT does not check this as it has no concept of variable names.
     *
     * @param sharedVars the type of co-referencing vars observed in the triple patter. Can be
     *                   obtained via {@link TriplePattern#collectVarsInfo()}.
     * @return A {@link Predicate} that returns true iff the {@link TripleString} satisfies the
     *         constraints modeled by co-referencing variables in the triple pattern.
     */
    public static @NonNull Predicate<TripleString>
    sharedVarStringFilter(TriplePattern. @NonNull SharedVars sharedVars) {
        return TS_FILTERS.get(sharedVars.ordinal());
    }

    /**
     * Version of {@link HDTUtils#sharedVarStringFilter(TriplePattern.SharedVars)} that applies
     * over {@link TripleID}s.
     *
     * @param sharedVars the type of observed variable co-reference.
     * @return true iff the {@link TripleID} satisfies all observed variable co-references.
     */
    public static @NonNull Predicate<TripleID>
    sharedVarIDFilter(TriplePattern.@NonNull SharedVars sharedVars) {
        return TI_FILTERS.get(sharedVars.ordinal());
    }

    public static @NonNull CharSequence get(@NonNull TripleString ts, @NonNull TermPosition pos) {
        return switch (pos) {
            case SUB -> ts.getSubject() ;
            case PRE -> ts.getPredicate();
            case OBJ -> ts.getObject();
        };
    }

    public static long get(@NonNull TripleID ti, @NonNull TermPosition pos) {
        return switch (pos) {
            case SUB -> ti.getSubject();
            case PRE -> ti.getPredicate();
            case OBJ -> ti.getObject();
        };
    }
}
