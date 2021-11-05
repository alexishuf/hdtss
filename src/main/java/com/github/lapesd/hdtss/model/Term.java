package com.github.lapesd.hdtss.model;


import com.github.lapesd.hdtss.utils.TokenPatterns;
import com.github.lapesd.hdtss.vocab.RDF;
import com.github.lapesd.hdtss.vocab.XSD;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

@Accessors(fluent = true) @Slf4j
public record Term(@lombok.NonNull @NonNull CharSequence sparql) {
    private static final Pattern LANG_RX = Pattern.compile("(?i)[a-z][-a-z0-9]*");
    private static final Pattern URI_RX = Pattern.compile("^<[^<>]*>$");
    private static final char[] ESCAPE_CHARS         = {'"', '\'', '\\',  'b',  'f',  'n',  'r',  't'};
    private static final char[] ESCAPE_CHARS_REVERSE = {'"', '\'', '\\', '\b', '\f', '\n', '\r', '\t'};
    //                                                   0    1     2     3     4     5     6     7
    private static final int REQ_ESCAPE_SIG = (1     ) | (1 << 2) | (1 << 5) | (1 << 6);
    private static final @NonNull String XSD_TYPE_SUFFIX = "^^xsd:";

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
                case '"', '\'' -> LITERAL;
                case '<' -> URI;
                case '_', '[' -> BLANK;
                default -> throw new IllegalArgumentException("Cannot deduce type from char "+c);
            };
        }
    }

    /**
     * Create a Term from the given representation or return null if {@code sparql} is null.
     *
     * @param sparql An RDF term in NT syntax, {@code []} or a SPARQL variable.
     * @return null if sparql is null, else a new {@link Term} wrapping {@code sparql}.
     */
    public static Term fromEscaped(@Nullable CharSequence sparql) {
        return sparql == null ? null : new Term(sparql);
    }

    /**
     * Create a {@link Term} for the given URI (which need not include wrapping {@code <>}s.
     *
     * Occurrences of characters disallowed in the <a href="https://www.w3.org/TR/sparql11-query/#rIRIREF">
     * SPARQL IRIREF production</a> will be percent-escaped. If there is no such occurrence and
     * {@code uri} is wrapped by {@code <>}, it will be taken <strong>by reference</strong> by
     * the new {@link Term} instance.
     *
     * @param uri a URI, optionally including surrounding {@code <>}s
     * @return null iff uri is null or a {@link Term} with a valid SPARQL ur wrapped by {@code <>}.
     */
    public static Term fromURI(@Nullable CharSequence uri) {
        if (uri == null)
            return null;
        if (uri.isEmpty())
            return new Term("<>");
        boolean isWrapped = uri.charAt(0) == '<' && uri.charAt(uri.length() - 1) == '>';
        StringBuilder b = null;
        int start = isWrapped ? 1 : 0, end = uri.length()-start;
        for (int i = start; (i = nextBadURIChar(uri, i, end)) < end; ++i ) {
            if (b == null)
                b = new StringBuilder(uri.length()+16).append('<');
            b.append(uri, start, i).append('%').append(String.format("%X", uri.charAt(i)&0xFF));
            start = i+1;
        }
        if (b != null)
            return new Term(b.append(uri, start, end).append('>'));
        return new Term(isWrapped ? uri : "<"+uri+">");
    }

    private static int nextBadURIChar(@NonNull CharSequence cs, int from, int to) {
        // IRIREF := ([^<>"{}|^`\]-[#x00-#x20])
        // range
        // #x00-#x20   // range 1
        // " #x22      // range 1
        // < #x3C
        // > #x3E
        // [ #x5B      // range 2
        // \ #x5C      // range 2
        // ] #x5D      // range 2
        // ^ #x5E      // range 2
        // ` #x60
        // { #x7B      // range 3
        // | #x7C      // range 3
        // } #x7D      // range 3
        for (int i = from; i < to; i++) {
            char c = cs.charAt(i);
            if (c <= 0x22) {
                if (c != 0x21) return i;
            } else if (c <= 0x5E) {
                if (c >= 0x5B || c == '<' || c == '>') return i;
            } else if (c <= 0x7D) {
                if (c >= 0x7B || c == '`') return i;
            }
        }
        return to;
    }

    /**
     * Create a {@link Term} for the given literal.
     *
     * @param literal the literal lexical form or the literal in a valid Turtle syntax.
     * @param quote if null, literal is the lexical form, not wrapped by quotes. Else this
     *              is the quote style used to wrap the lexical form in literal. The literal
     *              can only contain language tag or datatype when quote is non-null.
     * @param escaped whether the lexical form has the forbidden chars (", \n, \r and \)
     *                already escaped. Escape
     * @return a {@link Term} for the given literal. If literal is already a valid (escaped)
     *         literal compliant with NT syntax ({@link LiteralQuote#DOUBLE}), then Term will keep
     *         a reference to it. If changes are required to make it valid, the {@link Term} a
     *         new {@link CharSequence} will hold those changes.
     */
    public static Term fromLiteral(@Nullable CharSequence literal, @Nullable LiteralQuote quote,
                                   boolean escaped) {
        if (literal == null)
            return null;
        var nt = escaped ? convertEscapedLiteralLexicalForm(literal, quote)
                         : escapeLiteralLexicalForm(literal, quote);
        return new Term(nt);
    }

    /**
     * Functionally equivalent to {@link Term#fromLiteral(CharSequence, LiteralQuote, boolean)}
     * called with quote={@link LiteralQuote#DOUBLE} and escaped={@code false}
     *
     * If the requirements for literal are satisfied, this factory method will be more efficient.
     *
     * @param literal The input literal quoted with a single double quote (").
     *                The literal may contain language tags and absolute datatypes. It
     *                <strong>cannot</strong> contain abbreviated datatypes (e.g.,
     *                {@code ^^xsd:string}). The literal is assumed to be wholy unescaped, any
     *                valid escape sequence will have the leading \ escaped into \\.
     * @return null iff literal is null, else a non-null {@link Term} with NT-compliant syntax.
     *         If the given literal requires no escapes, it will be wrapped by
     *         <strogn>reference</strogn> inside the new {@link Term}.
     */
    public static Term fromNonXSDAbbrevDoubleQuotedUnescapedLiteral(@Nullable CharSequence literal) {
        if (literal == null)
            return null;
        int closeIdx = literal.length()-1, start = 1;
        while (closeIdx >= 0 && literal.charAt(closeIdx) != '"') --closeIdx;
        StringBuilder b = escape(literal, 1, closeIdx, literal.length()+12);
        if (b == null)
            return new Term(literal);
        return new Term(b.append(literal, closeIdx+1, literal.length()));
    }

    /**
     * Create a Literal term from the given components of a literal.
     *
     * @param unquotedAndUnescapedContent The literal lexical form, without \-escapes.
     *                                    Any existing \ will be escaped into \\. This should also
     *                                    not be wrapped by double quotes ("). If it is, they will
     *                                    be escaped into \".
     * @param lang the language tag, optionally with a leading '@', or null.
     * @param datatype the datatype URI, with or without wrapping angle brackets ({@code <>}).
     * @return A non-null {@link Term} representing the literal made from the given components.
     *         {@link Term#unescapedContent()} will be equal to {@code unquotedAndUnescapedContent}.
     */
    public static @NonNull Term
    fromUnescapedLiteralParts(@NonNull CharSequence unquotedAndUnescapedContent,
                              @Nullable CharSequence lang,
                              @Nullable CharSequence datatype) {
        int capacity = unquotedAndUnescapedContent.length() + 2
                + (lang == null ? 0 : lang.length())
                + (datatype == null ? 0 : datatype.length());
        StringBuilder b = escape(unquotedAndUnescapedContent, 0, unquotedAndUnescapedContent.length(), capacity);
        if (b == null)
            b = new StringBuilder(capacity).append('"').append(unquotedAndUnescapedContent).append('"');
        if (lang != null && !lang.isEmpty()) {
            assert TokenPatterns.LANGTAG_RX.matcher(lang).matches() : "Invalid lang tag";
            if (lang.charAt(0) != '@')
                b.append("@");
            b.append(lang);
        } else if (datatype != null && !datatype.isEmpty()
                && !isXsdString(datatype, datatype.charAt(0) == '<')) {
            b.append("^^");
            if (datatype.charAt(0) == '<')
                b.append(datatype);
            else
                b.append('<').append(datatype).append('>');
        }
        return new Term(b);
    }

    private static @Nullable StringBuilder escape(@NonNull CharSequence cs, int start, int end,
                                                  int capacity) {
        StringBuilder b = null;
        for (int i = start; i < end; i++) {
            String escape = escapeString(cs.charAt(i));
            if (escape != null) {
                if (b == null)
                    b = new StringBuilder(capacity).append('"');
                b.append(cs, start, i).append(escape);
                start = i+1;
            }
        }
        return b != null ? b.append(cs, start, end).append('"') : null;
    }

    static int findCloseIdx(@NonNull CharSequence literal, @NonNull LiteralQuote quote) {
        char sym = quote.symbol();
        int quoteLen = quote.length(), closeIdx = literal.length();
        for (int c = 0; closeIdx > quoteLen && c != quoteLen; )
            c = literal.charAt(--closeIdx) == sym ? c + 1 : 0;
        if (closeIdx < quoteLen)
            throw new IllegalArgumentException("No closing "+quote+" in "+literal);
        return closeIdx;
    }

    private static @NonNull CharSequence
    escapeLiteralLexicalForm(@NonNull CharSequence literal, @Nullable LiteralQuote quote) {
        int quoteLen = quote == null ? 0 : quote.length();
        int closeIdx = quote == null ? literal.length() : findCloseIdx(literal, quote);
        assert closeIdx <= literal.length();
        StringBuilder b = escape(literal, quoteLen, closeIdx, literal.length()+12);
        if (b == null) {
            if (quote == LiteralQuote.DOUBLE && !isXsdTyped(literal, closeIdx, quoteLen)) {
                assert TokenPatterns.LITERAL_RX.matcher(literal).matches();
                return literal;
            }
            b = new StringBuilder(literal.length());
            b.append('"').append(literal, quoteLen, closeIdx).append('"');
        }
        return appendLiteralTypeOrLangTagSuffix(b, literal, quoteLen, closeIdx);
    }

    private static @Nullable String escapeString(char c) {
        return switch (c) {
            case '"'  -> "\\\"";
            case '\\' -> "\\\\";
            case '\n' -> "\\n";
            case '\r' -> "\\r";
            default   -> null;
        };
    }

    private static int ucharLen(@NonNull CharSequence cs, int idx, int end) {
        if (idx == end)
            return 0;
        assert idx < end && end <= cs.length() && cs.charAt(idx) == '\\';
        char u = cs.charAt(idx);
        int hexEnd = idx + switch (u) {
            case 'u' -> 2+4;
            case 'U' -> 2+8;
            default -> 0;
        };
        for (int i = idx+2; i < hexEnd; i++) {
            char c = cs.charAt(i);
            if (c < '0' || c > 'f' || (c > '9' && c < 'A') || (c > 'F' && c < 'a')) return 0;
        }
        return hexEnd-idx;
    }

    private static boolean isXsdTyped(@NonNull CharSequence literal, int closeIdx, int quoteLen) {
        int size = XSD_TYPE_SUFFIX.length();
        if (closeIdx+size > literal.length())
            return false;
        boolean is = true;
        for (int i = 0; is && i < size; i++)
            is = literal.charAt(closeIdx+quoteLen+i) == XSD_TYPE_SUFFIX.charAt(i);
        return is;
    }

    private static @NonNull CharSequence
    convertEscapedLiteralLexicalForm(@NonNull CharSequence literal, @Nullable LiteralQuote quote) {
        int quoteLen = quote == null ? 0 : quote.length(), start = quoteLen, len = literal.length();
        int closeIdx = quote == null ? literal.length() : findCloseIdx(literal, quote);
        if (quote == LiteralQuote.DOUBLE) {
            if (isXsdTyped(literal, closeIdx, quoteLen)) {
                StringBuilder b = new StringBuilder(literal.length() + XSD.NS.length());
                b.append(literal, 0, closeIdx+quoteLen/*"*/);
                return appendLiteralTypeOrLangTagSuffix(b, literal, quoteLen, closeIdx);
            }
            return literal;
        }
        assert closeIdx <= len;
        StringBuilder b = new StringBuilder(len + 8);
        b.append('"');
        for (int i = start; i < closeIdx; ) {
            char c0 = literal.charAt(i++);
            if (c0 == '\\' && i < closeIdx) {
                int skip = ucharLen(literal, i-1, closeIdx);
                if (skip > 0) {
                    i += skip-1;
                } else {
                    char c1 = literal.charAt(i++);
                    int eCharIdx = Arrays.binarySearch(ESCAPE_CHARS, c1);
                    if (eCharIdx < 0)
                        b.append(literal, start, --i).append('\\'); // escape '\\'
                    else if (((1 << eCharIdx) & REQ_ESCAPE_SIG) != 0)
                        continue; // keep required escape as is
                    else
                        b.append(literal, start, i-2).append(ESCAPE_CHARS_REVERSE[eCharIdx]); //un-escape optional escape
                }
                start = i;
            } else {
                String escape = escapeString(c0);
                if (escape != null)
                    b.append(literal, start, i-1).append(escape);
            }
        }
        b.append(literal, start, closeIdx).append('"');
        return appendLiteralTypeOrLangTagSuffix(b, literal, quoteLen, closeIdx);
    }

    /**
     * Get the original (unescaped) char represented by \{@code escapeCode}.
     *
     * @param escapeCode the char following a backslash in a NT escape sequence.
     * @return The unescaped char or {@code '\0'} if {@code escapeCode} is not a valid escape char
     */
    public static char expandEscape(char escapeCode) {
        int idx = Arrays.binarySearch(ESCAPE_CHARS, escapeCode);
        return idx < 0 ? '\0' : ESCAPE_CHARS_REVERSE[idx];
    }

    private static @NonNull CharSequence
    appendLiteralTypeOrLangTagSuffix(@NonNull StringBuilder b,
                                     @NonNull CharSequence literal, int quoteLen, int closeIdx) {
        if (quoteLen > 0 && isXsdTyped(literal, closeIdx, quoteLen)) {
            b.append("^^<").append(XSD.NS)
                    .append(literal, closeIdx+quoteLen+6/*^^xsd:*/, literal.length())
                    .append('>');
        } else {
            b.append(literal, closeIdx+quoteLen, literal.length());
        }
        assert TokenPatterns.LITERAL_RX.matcher(b).matches();
        return b;
    }

    /**
     * Create a variable {@link Term} from the given variable name or representation.
     *
     * @param varName either a variable name (will use {@code ?}in the resulting {@link Term}
     *                or a SPARQL <a href="https://www.w3.org/TR/sparql11-query/#rVAR1">VAR1</a>>
     *                or <a href=https://www.w3.org/TR/sparql11-query/#rVAR2>VAR2</a> production.
     * @return null iff varName is null, "[]" if varName is empty, else a SPARQL var.
     */
    public static Term fromVar(@Nullable CharSequence varName) {
        if (varName == null)
            return null;
        else if (varName.isEmpty())
            return new Term("[]");
        char first = varName.charAt(0);
        return new Term(switch (first) {
            case '?', '$' ->     varName;
            default       -> "?"+varName;
        });
    }

    /**
     * Create a blank node {@link Term} from the given representation.
     *
     * @param labelOrSPARQLOrNT null, a blank node label (not including the leading {@code _:}),
     *                          a NT blank node ({@code _:} followe by a label) or an empty
     *                          SPARQL blank node ({@code []}.
     * @return null iff labelOrSPARQLOrNT is null, else {@code []} or the NT representation of the
     *         blank node.
     */
    public static Term fromBlank(@Nullable CharSequence labelOrSPARQLOrNT) {
        if (labelOrSPARQLOrNT == null)
            return null;
        else if (labelOrSPARQLOrNT.isEmpty())
            return new Term("[]");
        char first = labelOrSPARQLOrNT.charAt(0);
        char second = labelOrSPARQLOrNT.length() > 1 ? labelOrSPARQLOrNT.charAt(1) : 'x';
        if (first == '_' && second == ':') {
            return new Term(labelOrSPARQLOrNT.length() == 2 ? "[]" : labelOrSPARQLOrNT);
        } else if (first == '[') {
            if (second != ']')
                throw new IllegalArgumentException();
            return new Term(labelOrSPARQLOrNT);
        } else {
            return new Term("_:"+labelOrSPARQLOrNT);
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
    public int contentEnd() {
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
            int idx = string.lastIndexOf('@');
            if (idx >= 0)
                return idx -1; // "@..........;
            if ((idx = string.lastIndexOf("^^")) < 0)
                throw new IllegalArgumentException("Malformed literal:"+sparql);
            return idx-1; // "^^
        } else {
            for (int hats = 0, i = sparql.length()-2; i >= 0; i--) {
                char c = sparql.charAt(i);
                switch (c) {
                    case '@' -> { return i-1; }
                    case '^' -> { if (++hats == 2) return i-1; }
                    default  ->   hats = 0;
                }
            }
            throw new IllegalArgumentException("Malformed literal: "+sparql);
        }
    }

    /**
     * Get the index of the char at which {@link Term#content()} starts.
     *
     * @return the index of the first {@link Term#content()} char.
     * @throws IllegalArgumentException if this Term wraps an empty string (not to be confused
     *                                  with {@code "\"\""}), which is invalid.
     */
    public int contentStart() {
        char first;
        try {
            first = sparql.charAt(0);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Term wraps an empty string, which is not a valid SPARQL term");
        }
        return first == '_' ? 2 : 1;
    }

    /**
     * Get the main string representation of the term: the URI, the var name, the blank node
     * label or the lexical form.
     *
     * @return A {@link CharSequence} with the content string. Only literals and blank nodes may
     *         have empty content strings.
     * @throws IllegalArgumentException if this Term wraps an empty string (not to be confused
     *                                  with {@code "\"\""}), which is invalid.
     */
    public @NonNull CharSequence content() {
        return sparql.subSequence(contentStart(), contentEnd());
    }

    /**
     * For literals, return {@link Term#content()} but replacing \n\r\\ and \" escape sequences
     * with the actual characters. For other types, simply return {@link Term#content()}.
     *
     * @return content() with literal escape sequences processed.
     */
    public @NonNull CharSequence unescapedContent() {
        if (type() != Type.LITERAL)
            return content();
        int start = 1, contentEnd = contentEnd();
        StringBuilder b = new StringBuilder(contentEnd-1);
        for (int i = start; i < contentEnd; ) {
            while (i < contentEnd && sparql.charAt(i) != '\\') ++i; //find next '\\'
            if (++i < contentEnd) { // set i to char after '\\'
                char c1 = sparql.charAt(i++); // consume char after \\
                int idx = Arrays.binarySearch(ESCAPE_CHARS, c1);
                if (idx >= 0) { // only append unescaped char if c0c1 is a valid escape
                    b.append(sparql, start, i-2).append(ESCAPE_CHARS_REVERSE[idx]);
                    start = i;
                }
            }
        }
        return b.append(sparql, start, contentEnd);
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
            if (sparql.charAt(end+1) == '@')
                return RDF.langString;
            if (sparql.charAt(end+3) == '<') {
                return sparql.subSequence(end+4, sparql.length()-1); // "^^<...
            } else { // ^^PREFIX:LOCAL_NAME
                String dt = sparql.subSequence(end + 3, sparql.length()).toString();
                if (dt.startsWith("xsd:"))
                    return XSD.NS+dt.substring(4);
                if (dt.startsWith("rdf:"))
                    return RDF.NS+dt.substring(4);
                log.warn("Invalid datatype syntax in {}. Returning datatype()={}", sparql, dt);
                return dt;
            }
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Bad NT typed literal: "+sparql);
        }
    }

    /**
     * Get a equivalent {@link Term} backed by a {@link String}, which may be {@code this}.
     *
     * @return a {@link Term} with the same {@link Term#sparql()}, but as a {@link String}.
     */
    public @NonNull Term withString() {
        return sparql instanceof String ? this : new Term(sparql.toString());
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
        return isXsdString(datatype(), false);
    }

    private static boolean isXsdString(@Nullable CharSequence cs, boolean isWrapped) {
        if (cs == null)
            return false;
        int start = isWrapped ? 1 : 0, end = cs.length() - start;
        // http://www.w3.org/2001/XMLSchema#string
        // 0123456789012345678901234567890123456789
        // 0         1         2         3
        return end-start == 39 && cs.charAt(32) == '#' && cs.charAt(33) == 's'
                && cs.charAt(11) == 'w' && cs.charAt(12) == '3' && cs.charAt(13) == '.';
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

    @Override public boolean equals(Object obj) {
        if (!(obj instanceof Term rt)) return false;
        CharSequence rs = rt.sparql;
        int length = sparql.length(), rLength = rs.length();
        boolean fullCompare = true;
        if (isStringLiteral() && rt.isStringLiteral()) {
            char end = sparql.charAt(length - 1), rEnd = rs.charAt(rLength - 1);
            if (end != rEnd) {
                if (end != '"') length = contentEnd()+1;
                else            rLength = rt.contentEnd()+1;
                fullCompare = false;
            }
        }
        if (fullCompare && sparql instanceof String && rs instanceof String)
            return sparql.equals(rs);
        if (length != rLength)
            return false;
        for (int i = 0; i < length; i++) {
            if (sparql.charAt(i) != rs.charAt(i))
                return false;
        }
        return true;
    }

    @Override public int hashCode() {
        return sparql instanceof String ? sparql.hashCode()
                                        : sparql.toString().hashCode();
    }
}
