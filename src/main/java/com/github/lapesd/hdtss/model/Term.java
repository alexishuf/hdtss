package com.github.lapesd.hdtss.model;


import com.github.lapesd.hdtss.vocab.RDF;
import com.github.lapesd.hdtss.vocab.XSD;
import lombok.experimental.Accessors;
import lombok.val;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

@Accessors(fluent = true)
public record Term(@lombok.NonNull @NonNull CharSequence sparql) {
    private static final Pattern LANG_RX = Pattern.compile("(?i)[a-z][-a-z0-9]*");
    private static final Pattern URI_RX = Pattern.compile("^<[^<>]*>$");

    public enum Type {
        VAR,
        LITERAL,
        URI,
        BLANK;

        public boolean isGround() {
            return this != VAR && this != BLANK;
        }

        /**
         * Deduce a {@link Type} from the first char of a SPARQL variable or a NT term.
         *
         * @param c The character
         * @return the {@link Type} whose terms start with it.
         * @throws IllegalArgumentException if {@code c} is not a valid first char for any
         *         {@link Type}
         */
        public static @NonNull Type fromNTFirstChar(char c) {
            return switch (c) {
                case '?', '$' -> VAR;
                case '"' -> LITERAL;
                case '<' -> URI;
                case '_', '[' -> BLANK;
                default -> throw new IllegalArgumentException("Cannot deduce type from char "+c);
            };
        }
    }

    /**
     * Test if the SPARQL term string held by this record is valid.
     *
     * @throws IllegalArgumentException if the SPARQL string is invalid
     */
    public void validate() throws IllegalArgumentException {
        if (sparql.isEmpty())
            throw new IllegalArgumentException("A SPARQL term cannot be empty");

        Type type = Type.fromNTFirstChar(sparql.charAt(0));// will throw if first char is bad
        if (type == Type.VAR && content().isEmpty())
            throw new IllegalArgumentException("VAR \""+sparql+"\" has empty name!");

        CharSequence datatype = datatype();
        CharSequence lang = lang();
        if (type == Type.LITERAL) {
            if (datatype == null)
                throw new IllegalArgumentException("LITERAL '"+sparql+"' has null datatype()!");
            try {
                if (!new URI(datatype.toString()).isAbsolute())
                    throw new IllegalArgumentException("Datatype "+datatype+" in LITERAL '"+sparql+"' is relative");
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Datatype "+datatype+" in LITERAL '"+sparql+"' is not a valid URI: "+e.getMessage());
            }
            if (lang != null) {
                if (!RDF.langString.contentEquals(requireNonNull(datatype)))
                    throw new IllegalArgumentException("LITERAL '"+sparql+"' has lang() != null and datatype() != rdf:langString");
                if (!LANG_RX.matcher(lang).matches())
                    throw new IllegalArgumentException("Lang tag in '"+sparql+"' does not match "+LANG_RX.pattern());
                val tail = "\"@" + lang;
                if (!sparql.toString().endsWith(tail))
                    throw new IllegalArgumentException("LITERAL '"+sparql+"' does not end with "+tail);
            } else {
                val tail = "\"^^<"+datatype+">";
                if (!sparql.toString().endsWith(tail) && sparql.charAt(sparql.length()-1) != '"')
                    throw new IllegalArgumentException("LITERAL '"+sparql+"' does not end with "+tail+" nor '\"'");
            }
        } else {
            if (lang != null)
                throw new IllegalArgumentException("Non-LITERAL \""+sparql+"\" has lang()");
            if (datatype != null)
                throw new IllegalArgumentException("Non-LITERAL \""+sparql+"\" has datatype()");
        }

        if (type == Type.BLANK) {
            boolean bad = switch (sparql.charAt(0)) {
                case '[' -> sparql.charAt(sparql.length() - 1) != ']';
                case '_' -> sparql.length() < 2 || sparql.charAt(1) != ':';
                default -> true;
            };
            if (bad)
                throw new IllegalArgumentException("BLANK \""+sparql+"\" should start with \"[\" or \"_:\"");
        } else if (type == Type.URI && !URI_RX.matcher(sparql).matches()) {
            throw new IllegalArgumentException("URI "+sparql+" does not match "+URI_RX.pattern());
        }
    }

    /**
     * Version of {@link Term#validate()} that does not throw.
     *
     * @return true iff the SPARQL term held by the record is valid.
     */
    public boolean isValid() {
        try {
            validate();
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public @NonNull Type type() {
        try {
            return Type.fromNTFirstChar(sparql.charAt(0));
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("This Term wraps an empty String, which is not a valid SPARQL term");
        }
    }

    public boolean isVar()     { return type() == Type.VAR;     }
    public boolean isBlank()   { return type() == Type.BLANK;   }
    public boolean isURI()     { return type() == Type.URI;     }
    public boolean isLiteral() { return type() == Type.LITERAL; }
    public boolean isGround()  { return type().isGround();      }

    /**
     * Get the first position (which can be the length) after {@link Term#content()}.
     *
     * @return the end position (last char position +1) of the content
     * @throws IllegalArgumentException if the {@link Term#sparql()} is malformed
     */
    int contentEnd() {
        switch (type()) {
            case VAR -> { return sparql.length(); }
            case BLANK -> { return sparql.charAt(0) == '_' ? sparql.length() : 1; }
            case URI -> { return sparql.length() - 1; }
        }
        char last = sparql.charAt(sparql.length() - 1);
        if (last == '>') {
            int openIRI = -1;
            if (sparql instanceof String string) {
                openIRI = string.lastIndexOf('<');
            } else {
                for (int i = sparql.length()-2; openIRI == -1 && i >= 0; i--)
                    openIRI = sparql.charAt(i) == '<' ? i : openIRI;
            }
            if (openIRI < 0)
                throw new IllegalArgumentException("Malformed literal ends with >, but has no <: "+sparql);
            return openIRI - 3; // "^^<........
        } else if (last == '"') {
            return sparql.length()-1;
        } else if (sparql instanceof String string) {
            return string.lastIndexOf('@')-1; // "@..........;
        } else {
            for (int i = sparql.length()-2; i >= 0; i--) {
                if (sparql.charAt(i) == '@') return i-1;
            }
            throw new IllegalArgumentException("Malformed literal does not with \" nor >, but has no @: "+sparql);
        }
    }

    /**
     * Get the main string representation of the term: the URI, the var name, the blank node
     * label or the lexical form.
     *
     * @return A {@link CharSequence} with the content string. Only literals and blank nodes may
     *         have empty content strings.
     */
    public @NonNull CharSequence content() {
        char first;
        try {
            first = sparql.charAt(0);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Term wraps an empty string, which is not a valid SPARQL term");
        }
        return sparql.subSequence(first == '_' ? 2 : 1, contentEnd());
    }

    /**
     * Get {@link Term#content()} with surrounding quote symbols ({@code <>}, {@code ""},
     * {@code ?}, {@code $} or {@code _:}).
     *
     * Note that variables can start with either "?" or "$". Blank nodes may start with _: but
     * in case of "[]", the return will be the same as {@link Term#content()}.
     *
     * @return a non-null and non-empty subsequence with the {@link Term#content()},
     *         including SPARQL/N-Triples quote symbols.
     */
    public @NonNull CharSequence quoted() {
        return sparql.subSequence(0, Math.min(contentEnd()+1, sparql.length()));
    }

    /**
     * Get the datatype IRI or null if {@code !isLiteral()}.
     *
     * For language-tagged literals, this returns {@link RDF#langString}, the entailed
     * datatype as defined by the RDF standard
     *
     * @return A non-null and non-empty datatype IRI if {@link Term#isLiteral()} or null otherwise.
     */
    public @Nullable CharSequence datatype() {
        if (!isLiteral())
            return null;
        int end = contentEnd();
        if (end == sparql.length()-1)
            return XSD.string;
        try {
            return sparql.charAt(end + 1) == '@'
                    ? RDF.langString
                    : sparql.subSequence(end + 4, sparql.length() - 1); // "^^<...
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Bad NT typed literal: "+sparql);
        }
    }


    /**
     * If {@link Term#isStringLiteral()} and the xsd:string datatype is not explicit, create
     * a new equivalent Term where it is. Else, return this Term.
     *
     * @return this {@link Term} or an equivalent {@link Term} with explicit xsd:string type.
     */
    public @NonNull Term withExplicitString() {
        if (isStringLiteral() && sparql.charAt(sparql.length()-1) != '>')
            return new Term(quoted()+"^^<"+XSD.string+">");
        return this;
    }

    /**
     * Similar to {@link Term#withExplicitString()}, but the return will be implicitly typed if
     * it is a plain string.
     *
     * @return this {@link Term} or an equivalent version of it without the explicit xsd:string
     *         type if {@link Term#isStringLiteral()}.
     */
    public @NonNull Term withImplicitString() {
        if (isStringLiteral() && sparql.charAt(sparql.length()-1) == '>')
            return new Term(quoted());
        return this;
    }

    /**
     * Tests whether this Term is a plain or xsd:string-typed literal
     *
     * @return true iff this is a plain string or an explicitly-typed RDF string
     */
    public boolean isStringLiteral() {
        var dt = datatype();
        if (dt == null)
            return false;
        int len = dt.length();
        // #string
        // 76543210
        if (len == 39 && dt.charAt(len-7) == '#' && dt.charAt(len-6) == 's')
            return XSD.string.contentEquals(dt); // only run equals if likely to return true
        return false;
    }

    /**
     * True if and only if {@link Term#datatype()} is {@link RDF#langString}
     *
     * @return true iff this is a lang string (has a language tag).
     */
    public boolean isLangStringLiteral() {
        return lang() != null;
    }

    /**
     * Get the language tag or null if not a lang-tagged literal
     *
     * @return A non-null and non-empty language tag (without the leading @) if this
     *         is a lang-tagged literal, or null otherwise.
     */
    public @Nullable CharSequence lang() {
        if (!isLiteral())
            return null;
        int end = contentEnd();
        return end < sparql.length()-2 && sparql.charAt(end+1) == '@'
                ? sparql.subSequence(end+2, sparql.length())
                : null;
    }

    public @Nullable String langAsString() {
        CharSequence cs = lang();
        return cs == null ? null : cs.toString();
    }

    @Override public @NonNull String toString() {
        return sparql.toString();
    }
}
