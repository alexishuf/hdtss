package com.github.lapesd.hdtss.sparql.results.codecs;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.io.buffer.ByteBuffer;
import io.micronaut.core.type.Argument;
import io.micronaut.http.codec.CodecConfiguration;
import io.micronaut.http.codec.CodecException;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

@Singleton
public class TSVCodec extends AbstractSVEncoder {
    @Inject
    public TSVCodec(@Named("sparqlTSV") @Nullable CodecConfiguration configuration) {
        super('\t', "?", "\n", SparqlMediaTypes.RESULTS_TSV_TYPE, configuration);
    }

    public static @NonNull CharSequence sanitize(@NonNull Term term) {
        CharSequence s = term.withImplicitString().sparql();
        if (s.charAt(0) == '[')
            return "_:"+ UUID.randomUUID();
        return s;
    }

    @Override protected void encode(@NonNull Term term, @NonNull ByteBuffer<?> buffer) {
        buffer.write(sanitize(term).toString().getBytes(UTF_8));
    }

    @Override protected void encode(@NonNull Term term, @NonNull Writer writer) throws IOException {
        writer.append(sanitize(term));
    }

    private static final @NonNull Pattern VAR_RX = Pattern.compile("\\??([^\t]+)(?:\t|$)");
    private static final @NonNull Pattern TERM_RX = Pattern.compile("([^\t]*)(?:\t|$)");

    @SuppressWarnings("unchecked") @Override public <T> T decode(Argument<T> type, InputStream inputStream) throws CodecException {
        if (!type.isAssignableFrom(QuerySolutions.class))
            throw new CodecException(this+" only decodes into QuerySolutions instances");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, UTF_8))) {
            String hLine = reader.readLine();
            if (hLine == null)
                return (T) BatchQuerySolutions.ASK_FALSE;
            List<String> vars = new ArrayList<>();
            for (Matcher m = VAR_RX.matcher(hLine); m.find(); )
                vars.add(m.group(1));
            int nVars = vars.size();
            List<SolutionRow> rows = new ArrayList<>();
            for (String ln = reader.readLine(); ln != null; ln = reader.readLine()) {
                Term[] terms = new Term[nVars];
                Matcher m = TERM_RX.matcher(ln);
                for (int i = 0; m.find() && i < nVars; ++i)
                    terms[i] = toTerm(m.group(1));
                rows.add(new SolutionRow(terms));
            }
            return (T) new BatchQuerySolutions(vars, rows);
        } catch (IOException e) {
            throw new CodecException(e.getMessage(), e);
        }
    }

    private Term toTerm(String s) {
        return s == null || s.isBlank() ? null : new Term(s).withImplicitString();
    }
}
