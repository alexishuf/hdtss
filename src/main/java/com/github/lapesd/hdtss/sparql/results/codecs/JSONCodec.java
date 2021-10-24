package com.github.lapesd.hdtss.sparql.results.codecs;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import com.github.lapesd.hdtss.vocab.XSD;
import io.micronaut.context.BeanProvider;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.io.buffer.ByteBuffer;
import io.micronaut.core.io.buffer.ByteBufferFactory;
import io.micronaut.core.type.Argument;
import io.micronaut.core.util.functional.ThrowingFunction;
import io.micronaut.http.MediaType;
import io.micronaut.http.codec.CodecConfiguration;
import io.micronaut.http.codec.CodecException;
import io.micronaut.http.codec.MediaTypeCodec;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions.ASK_FALSE;
import static com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions.ASK_TRUE;

@Singleton
public class JSONCodec implements MediaTypeCodec  {
    private static final String D_QUOTE = "\"";
    private final @NonNull List<MediaType> typeList;
    private final @NonNull BeanProvider<ObjectMapper> objectMapperProvider;
    private ObjectMapper objectMapper;

    @Inject
    public JSONCodec(@Named("sparqlJSON") @Nullable CodecConfiguration configuration,
                     @NonNull BeanProvider<ObjectMapper> objectMapperProvider) {
        typeList = new ArrayList<>(1);
        typeList.add(SparqlMediaTypes.RESULTS_JSON_TYPE);
        if (configuration != null)
            typeList.addAll(configuration.getAdditionalTypes());
        this.objectMapperProvider = objectMapperProvider;
    }

    protected @NonNull ObjectMapper objectMapper() {
        if (objectMapper == null)
            objectMapper = objectMapperProvider.get();
        return objectMapper;
    }

    @Override public boolean supportsType(Class<?> type) {
        return QuerySolutions.class.isAssignableFrom(type);
    }

    @Override public Collection<MediaType> getMediaTypes() {
        return typeList;
    }

    @Introspected static record Head(List<String> vars) {}
    @Introspected static record Value(String type,
                                      String value,
                                      String datatype,
                                      @JsonProperty("xml:lang") String lang) {
        public static final Value NULL = new Value(null, null, null, null);
        private static final Pattern ESCAPE_RX = Pattern.compile("\"\\\\\r\n");

        private @NonNull String quote() {
            String string = value == null ? "" : value;
            Matcher m = ESCAPE_RX.matcher(string);
            StringBuilder builder = new StringBuilder(string.length() + 8).append('"');
            while (m.find()) {
                m.appendReplacement(builder, switch (string.charAt(m.start())) {
                    case '"' -> "\\\"";
                    case '\\' -> "\\\\";
                    case '\r' -> "\\r";
                    case '\n' -> "\\n";
                    default -> throw new RuntimeException("unexpected char");
                });
            }
            return m.appendTail(builder).append('"').toString();
        }

        public Term asTerm() {
            if (type == null)
                return null;
            switch (type.strip().toLowerCase()) {
                case "literal" -> {
                    if (lang != null)
                        return new Term(quote()+"@"+lang);
                    else if (datatype == null || datatype.equals(XSD.string))
                        return new Term(quote());
                    else
                        return new Term(quote()+"^^<"+datatype+">");
                }
                case "uri" -> {
                    return new Term("<"+value+">");
                }
                case "bnode" -> {
                    return value.startsWith("_:") ? new Term(value) : new Term("_:"+value);
                }
                default -> throw new CodecException("Illegal binding value type="+type);
            }
        }
    }
    @Introspected static record ResultsObj(List<Map<String, Value>> bindings) {}
    @Introspected static record Results(Head head,
                                        @JsonProperty("boolean") Boolean boolResult,
                                        ResultsObj results) {
        @NonNull List<String> vars() {
            return head == null || head.vars == null ? List.of() : head.vars;
        }
        @NonNull List<Map<String, Value>> bindings() {
            return results == null || results.bindings == null ? List.of() : results.bindings;
        }
    }

    @SuppressWarnings("unchecked") @Override public <T> T decode(Argument<T> type, InputStream inputStream) throws CodecException {
        if (!type.isAssignableFrom(QuerySolutions.class))
            throw new CodecException(this+" only decode()s into QuerySolutions instances");
        try {
            Results results = objectMapper().createParser(inputStream).readValueAs(Results.class);
            if      (Boolean.TRUE.equals(results.boolResult)) return (T) ASK_TRUE;
            else if (Boolean.FALSE.equals(results.boolResult)) return (T) ASK_FALSE;

            var vars = results.vars();
            int nVars = vars.size();
            var bindings = results.bindings();
            List<SolutionRow> rows = new ArrayList<>(bindings.size());
            for (Map<String, Value> binding : bindings) {
                Term[] terms = new Term[nVars];
                for (int i = 0; i < nVars; i++)
                    terms[i] = binding.getOrDefault(vars.get(i), Value.NULL).asTerm();
                rows.add(new SolutionRow(terms));
            }
            return (T) new BatchQuerySolutions(vars, rows);
        } catch (IOException e) {
            throw new CodecException(e.getMessage(), e);
        }
    }

    public static void writeRow(@NonNull List<@NonNull String> vars, @NonNull Term[] terms,
                                 @NonNull ThrowingFunction<CharSequence, ?, IOException> writer)
            throws IOException {
        boolean first = true;
        writer.apply("{");
        for (int i = 0; i < terms.length; i++) {
            Term term = terms[i];
            if (term == null)
                continue;
            CharSequence content = term.content();
            if (term.isBlank() && content.isEmpty())
                content = UUID.randomUUID().toString();
            if (first) first = false;
            else       writer.apply(",");
            writer.apply(D_QUOTE);
            writer.apply(vars.get(i));
            writer.apply("\":{\"type\":\"");
            writer.apply(switch (term.type()) {
                case URI -> "uri";
                case LITERAL -> "literal";
                case BLANK -> "bnode";
                default -> "unknown"; //unreachable
            });
            writer.apply("\",\"value\":\"");
            writer.apply(content);
            writer.apply(D_QUOTE);
            if (term.isLangStringLiteral()) {
                writer.apply(",\"xml:lang\":\"");
                writer.apply(term.lang());
                writer.apply(D_QUOTE);
            } else if (term.isLiteral()) {
                writer.apply(",\"datatype\":\"");
                writer.apply(term.datatype());
                writer.apply(D_QUOTE);
            }
            writer.apply("}");
        }
        writer.apply("}");
    }

    @Override public <T> void
    encode(@NonNull T object, @NonNull OutputStream outputStream) throws CodecException {
        if (!QuerySolutions.class.isAssignableFrom(object.getClass()))
            throw new CodecException(this+" only encodes QuerySolutions instances");
        QuerySolutions solutions = (QuerySolutions) object;
        try (Writer w = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {
            if (solutions.isAsk()) {
                w.append("{\"head\":{\"vars\":[]},\"boolean\":")
                        .append(Boolean.toString(solutions.askResult())).append("}");
            } else {
                w.append("{\"head\":{\"vars\":[");
                var vars = solutions.varNames();
                int count = 0;
                for (String name : vars)
                    (count++ == 0 ? w : w.append(',')).append('"').append(name).append('"');
                w.append("]},\"results\":{\"bindings\":[");
                count = 0;
                for (SolutionRow row : solutions)
                    writeRow(vars, row.terms(), (count++ == 0 ? w : w.append(','))::append);
                w.append("]}}");
            }
        } catch (IOException e) {
            throw new CodecException(e.getMessage(), e);
        }
    }

    @Override public <T> byte[] encode(@NonNull T object) throws CodecException {
        var bOS = new ByteArrayOutputStream();
        encode(object, bOS);
        return bOS.toByteArray();
    }

    @Override
    public <T, B> ByteBuffer<B>
    encode(@NonNull T object, @NonNull ByteBufferFactory<?, B> allocator) throws CodecException {
        return allocator.wrap(encode(object));
    }
}
