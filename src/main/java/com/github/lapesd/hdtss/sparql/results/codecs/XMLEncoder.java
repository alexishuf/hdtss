package com.github.lapesd.hdtss.sparql.results.codecs;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
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
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

@Singleton
public class XMLEncoder implements MediaTypeCodec {
    private final @NonNull List<@NonNull MediaType> typeList;

    @Inject
    public XMLEncoder(@Named("sparqlXML") @Nullable CodecConfiguration configuration) {
        typeList = new ArrayList<>(1);
        typeList.add(SparqlMediaTypes.RESULTS_XML_TYPE);
        if (configuration != null)
            typeList.addAll(configuration.getAdditionalTypes());
    }

    @Override public boolean supportsType(Class<?> type) {
        return QuerySolutions.class.isAssignableFrom(type);
    }

    @Override public Collection<MediaType> getMediaTypes() {
        return typeList;
    }

    @Override public <T> T decode(Argument<T> type, InputStream inputStream) throws CodecException {
        throw new CodecException(this+" does not support decode()");
    }

    @SneakyThrows public static void writeRow(@NonNull List<@NonNull String> vars,
                                              Term @NonNull[] terms,
                                              @NonNull StringBuilder builder) {
        writeRow(vars, terms, builder::append);
    }

    private static final @NonNull Pattern ANGLE_RX = Pattern.compile("[<>\r\n\t]");

    public static void writeRow(@NonNull List<@NonNull String> vars, Term @NonNull[] terms,
                                @NonNull ThrowingFunction<CharSequence, ?, IOException> writer)
            throws IOException {
        writer.apply("<result>");
        for (int i = 0; i < terms.length; i++) {
            Term term = terms[i];
            if (term == null)
                continue;
            writer.apply("<binding name=\"");
            writer.apply(vars.get(i));
            writer.apply("\">");
            switch (term.type()) {
                case URI -> {
                    writer.apply("<uri>");
                    writer.apply(term.content());
                    writer.apply("</uri>");
                }
                case BLANK -> {
                    writer.apply("<bnode>");
                    writer.apply(term.sparql().charAt(0) == '['
                            ? UUID.randomUUID().toString() : term.content());
                    writer.apply("</bnode>");
                }
                case LITERAL -> {
                    writer.apply("<literal");
                    if (term.isLangStringLiteral()) {
                        writer.apply(" xml:lang=\"");
                        writer.apply(term.lang());
                        writer.apply("\"");
                    } else {
                        writer.apply(" datatype=\"");
                        writer.apply(term.datatype());
                        writer.apply("\"");
                    }
                    writer.apply(">");
                    CharSequence content = term.content();
                    if (ANGLE_RX.matcher(content).find()) {
                        writer.apply("<![CDATA[");
                        writer.apply(content);
                        writer.apply("]]>");
                    } else {
                        writer.apply(content);
                    }
                    writer.apply("</literal>");
                }
            }
            writer.apply("</binding>");
        }
        writer.apply("</result>");
    }

    @Override public <T> void encode(T object, OutputStream outputStream) throws CodecException {
        if (!QuerySolutions.class.isAssignableFrom(object.getClass()))
            throw new CodecException(this+".encode() expects QuerySolutions instances");
        QuerySolutions solutions = (QuerySolutions) object;
        try (Writer w = new OutputStreamWriter(outputStream)) {
            w.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
                    .append("<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">");
            w.append("<head>");
            var vars = solutions.varNames();
            for (String name : vars)
                w.append("<variable name=\"").append(name).append("\"/>");
            w.append("</head>");
            if (solutions.isAsk()) {
                w.append("<boolean>").append(Boolean.toString(solutions.askResult()))
                                     .append("</boolean>");
            } else {
                w.append("<results>");
                for (SolutionRow row : solutions)
                    writeRow(vars, row.terms(), w::append);
                w.append("</results>");
            }
            w.append("</sparql>");
        } catch (IOException e) {
            throw new CodecException(e.getMessage(), e);
        }
    }

    @Override public <T> byte[] encode(@NonNull T object) throws CodecException {
        ByteArrayOutputStream bOS = new ByteArrayOutputStream();
        encode(object, bOS);
        return bOS.toByteArray();
    }

    @Override
    public <T, B> ByteBuffer<B>
    encode(@NonNull T object, @NonNull ByteBufferFactory<?, B> allocator) throws CodecException {
        return allocator.wrap(encode(object));
    }
}
