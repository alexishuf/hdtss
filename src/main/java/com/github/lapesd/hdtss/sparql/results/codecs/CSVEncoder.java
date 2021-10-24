package com.github.lapesd.hdtss.sparql.results.codecs;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.io.buffer.ByteBuffer;
import io.micronaut.http.codec.CodecConfiguration;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
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
        CharSequence content = term.content();
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

    @Override protected void encode(@NonNull Term term, @NonNull ByteBuffer<?> buffer) {
        buffer.write(sanitize(term), StandardCharsets.UTF_8);
    }

    @Override protected void encode(@NonNull Term term, @NonNull Writer writer) throws IOException {
        writer.append(sanitize(term));
    }
}
