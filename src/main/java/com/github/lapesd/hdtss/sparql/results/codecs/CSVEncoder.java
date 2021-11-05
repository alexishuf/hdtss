package com.github.lapesd.hdtss.sparql.results.codecs;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.codec.CodecConfiguration;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.Writer;
import java.util.UUID;
import java.util.regex.Pattern;

@Singleton
public class CSVEncoder extends AbstractSVEncoder {
    private static final @NonNull Pattern ESCAPE_RX = Pattern.compile("(^|^\\\\)\"|\\\\([rn])");

    @Inject
    public CSVEncoder(@Named("sparqlCSV") @Nullable CodecConfiguration configuration) {
        super(',', "", "\r\n", SparqlMediaTypes.RESULTS_CSV_TYPE, configuration);
    }

    public static @NonNull CharSequence sanitize(@NonNull Term term) {
        if (term.sparql().charAt(0) == '[')
            return "_:" + UUID.randomUUID();
        else if (term.isBlank())
            return term.sparql();
        CharSequence content = term.unescapedContent();
        boolean needsQuotes = false;
        for (int i = 0, size = content.length(); !needsQuotes && i < size; i++) {
            char c = content.charAt(i);
            needsQuotes = c == '\r' || c == '\n' || c == '"' || c == ',';
        }
        if (needsQuotes) {
            var m = ESCAPE_RX.matcher(term.content());
            StringBuilder b = new StringBuilder(content.length() + 16).append('"');
            while (m.find()) {
                if (m.group(1) != null)
                    m.appendReplacement(b, "$1\"\"");
                else if ("r".equals(m.group(2)))
                    m.appendReplacement(b, "\r");
                else if ("n".equals(m.group(2)))
                    m.appendReplacement(b, "\n");
            }
            return m.appendTail(b).append('"');
        }
        return content;
    }

    public static void writeTerm(@NonNull Writer writer, @NonNull Term term) throws IOException {
        CharSequence sparql = term.sparql();
        if (sparql.charAt(0) == '[') {
            writer.append("_:").append(UUID.randomUUID().toString());
        } else if (term.isBlank()) {
            writer.append(sparql);
        } else if (term.isURI()) {
            writer.append(sparql, term.contentStart(), term.contentEnd());
        } else if (term.isVar()) {
            assert false : "vars should no be sent in SPARQL results, something is wrong!";
            writer.append(sparql);
        } else {
            assert term.isLiteral();
            int start = term.contentStart(), end = term.contentEnd();
            boolean needsQuotes = false;
            for (int i = start, escape = 0; !needsQuotes && i < end; i++) {
                char c = sparql.charAt(i);
                if (c == ',') {
                    needsQuotes = true;
                } else if (escape == 0) {
                    escape = c == '\\' ? 1 : 0;
                } else {
                    needsQuotes = c == 'n' || c == 'r' || c == '"';
                    escape = 0;
                }
            }
            if (needsQuotes) {
                writer.append('"');
                for (int i = start; i < end; ++i) {
                    for (char c = '\0'; i < end && c != '\\'; ++i) c = sparql.charAt(i);
                    if (i < end) {
                        writer.append(sparql, start, i-1);
                        start = i+1;
                        char c1 = sparql.charAt(i);
                        char expanded = Term.expandEscape(c1);
                        if (expanded == 0) // invalid escape sequence, keep as is
                            writer.append('\\').append(c1);
                        else if (c1 == '\"') // " must become ""
                            writer.append(expanded).append(expanded);
                        else
                            writer.append(expanded);
                    }
                }
                writer.append(sparql, start, end).append('"');
            } else {
                writer.append(sparql, start, end);
            }
        }
    }

    @Override protected void encode(@NonNull Term term, @NonNull Writer writer) throws IOException {
        writeTerm(writer, term);
    }
}
