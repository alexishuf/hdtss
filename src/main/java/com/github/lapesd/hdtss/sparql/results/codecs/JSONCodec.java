package com.github.lapesd.hdtss.sparql.results.codecs;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
import com.github.lapesd.hdtss.utils.ByteArrayWriter;
import io.micronaut.context.BeanProvider;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.io.buffer.ByteBuffer;
import io.micronaut.core.io.buffer.ByteBufferFactory;
import io.micronaut.core.type.Argument;
import io.micronaut.http.MediaType;
import io.micronaut.http.codec.CodecConfiguration;
import io.micronaut.http.codec.CodecException;
import io.micronaut.http.codec.MediaTypeCodec;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.*;
import java.util.*;

import static com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions.ASK_FALSE;
import static com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions.ASK_TRUE;
import static java.nio.charset.StandardCharsets.UTF_8;

@Singleton
public class JSONCodec implements MediaTypeCodec  {
    private static final String D_QUOTE = "\"";
    private final @NonNull List<MediaType> typeList;
    private final @NonNull BeanProvider<ObjectMapper> objectMapperProvider;
    private ObjectMapper objectMapper;

    @Inject
    public JSONCodec(@Named("sparqlJSON") @io.micronaut.core.annotation.Nullable
                                 CodecConfiguration configuration,
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

    @Introspected record Head(List<String> vars) {}
    @Introspected record Value(String type,
                               String value,
                               String datatype,
                               @JsonProperty("xml:lang") String lang) {
        public static final Value NULL = new Value(null, null, null, null);

        public Term asTerm() {
            if (type == null)
                return null;
            return switch (type.strip().toLowerCase()) {
                case "literal" -> Term.fromUnescapedLiteralParts(value, lang, datatype);
                case "uri" -> Term.fromURI(value);
                case "bnode" -> Term.fromBlank(value);
                default -> throw new CodecException("Illegal binding value type="+type);
            };
        }
    }
    @Introspected record ResultsObj(List<Map<String, Value>> bindings) {}
    @Introspected record Results(Head head,
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
            List<@Nullable Term @NonNull[]> rows = new ArrayList<>(bindings.size());
            for (Map<String, Value> binding : bindings) {
                Term[] terms = new Term[nVars];
                for (int i = 0; i < nVars; i++)
                    terms[i] = binding.getOrDefault(vars.get(i), Value.NULL).asTerm();
                rows.add(terms);
            }
            return (T) new BatchQuerySolutions(vars, rows);
        } catch (IOException e) {
            throw new CodecException(e.getMessage(), e);
        }
    }

    private static final byte[] TERM_TYPE = "\":{\"type\":\"".getBytes(UTF_8);
    private static final byte[] TERM_VALUE = "\",\"value\":\"".getBytes(UTF_8);
    private static final byte[] TERM_LANG = ",\"xml:lang\":\"".getBytes(UTF_8);
    private static final byte[] TERM_DATATYPE = ",\"datatype\":\"".getBytes(UTF_8);

    public static void writeRowToBytes(@NonNull List<@NonNull String> vars,
                                       @Nullable Term @NonNull[] terms,
                                       @NonNull ByteArrayWriter writer) {
        boolean first = true;
        writer.append('{');
        for (int i = 0; i < terms.length; i++) {
            Term term = terms[i];
            if (term == null)
                continue;

            CharSequence content = term.sparql();
            int contentStart = term.contentStart(), contentEnd = term.contentEnd();
            if (term.isBlank() && contentEnd == contentStart) {
                content = UUID.randomUUID().toString();
                contentStart = 0;
                contentEnd = content.length();
            }
            if (first) first = false;
            else       writer.append(',');
            writer.append(D_QUOTE).append(vars.get(i)).append(TERM_TYPE)
                    .append(switch (term.type()) {
                        case URI -> "uri";
                        case LITERAL -> "literal";
                        case BLANK -> "bnode";
                        default -> "unknown"; //unreachable
                    }).append(TERM_VALUE).append(content, contentStart, contentEnd).append(D_QUOTE);

            if (term.isLangStringLiteral()) {
                CharSequence cs = term.sparql();
                writer.append(TERM_LANG).append(cs, term.langStart(), cs.length()).append(D_QUOTE);
            } else if (term.isLiteral()) {
                writer.append(TERM_DATATYPE).append(term.datatype()).append(D_QUOTE);
            }
            writer.append('}');
        }
        writer.append('}');
    }

    public static void writeRow(@NonNull List<@NonNull String> vars,
                                @Nullable Term @NonNull[] terms,
                                @NonNull Writer writer)
            throws IOException {
        boolean first = true;
        writer.append('{');
        for (int i = 0; i < terms.length; i++) {
            Term term = terms[i];
            if (term == null)
                continue;
            CharSequence content = term.content();
            if (term.isBlank() && content.isEmpty())
                content = UUID.randomUUID().toString();
            if (first) first = false;
            else       writer.append(',');
            writer.append(D_QUOTE).append(vars.get(i)).append("\":{\"type\":\"")
                    .append(switch (term.type()) {
                            case URI -> "uri";
                            case LITERAL -> "literal";
                            case BLANK -> "bnode";
                            default -> "unknown"; //unreachable
                        }).append("\",\"value\":\"").append(content).append(D_QUOTE);
            if (term.isLangStringLiteral()) {
                writer.append(",\"xml:lang\":\"").append(term.lang()).append(D_QUOTE);
            } else if (term.isLiteral()) {
                writer.append(",\"datatype\":\"").append(term.datatype()).append(D_QUOTE);
            }
            writer.append('}');
        }
        writer.append('}');
    }

    @Override public <T> void
    encode(@NonNull T object, @NonNull OutputStream outputStream) throws CodecException {
        if (!QuerySolutions.class.isAssignableFrom(object.getClass()))
            throw new CodecException(this+" only encodes QuerySolutions instances");
        QuerySolutions solutions = (QuerySolutions) object;
        try (Writer w = new OutputStreamWriter(outputStream, UTF_8)) {
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
                for (@Nullable Term @NonNull[] row : solutions)
                    writeRow(vars, row, count++ == 0 ? w : w.append(','));
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
