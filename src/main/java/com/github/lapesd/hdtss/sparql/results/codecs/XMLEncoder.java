package com.github.lapesd.hdtss.sparql.results.codecs;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Singleton
public class XMLEncoder implements MediaTypeCodec {
    private final @NonNull List<@NonNull MediaType> typeList;

    @Inject
    public XMLEncoder(@Named("sparqlXML") @io.micronaut.core.annotation.Nullable
                                  CodecConfiguration configuration) {
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

    private static @NonNull Writer writeEscapedContent(@NonNull Writer w,
                                                       @NonNull Term term) throws IOException {
        CharSequence sparql = term.sparql();
        int start = term.contentStart(), end = term.contentEnd();
        boolean escaped = false;
        for (int j = start; j < end; j++) {
            char c = sparql.charAt(j);
            if (escaped) {
                escaped = false;
                char expanded = Term.expandEscape(c);
                if (expanded != 0) {
                    w.append(sparql, start, j-1).append(expanded);
                    start = j+1;
                }
            } else {
                switch (c) {
                    case '&'  -> { w.append(sparql, start, j).append("&amp;"); start = j + 1; }
                    case '<'  -> { w.append(sparql, start, j).append("&lt;");  start = j + 1; }
                    case '>'  -> { w.append(sparql, start, j).append("&gt;");  start = j + 1; }
                    case '\\' ->  escaped = true;
                }
            }
        }
        return w.append(sparql, start, end);
    }

    public static void writeRow(@NonNull List<@NonNull String> vars, Term @NonNull[] terms,
                                @NonNull Writer w)
            throws IOException {
        w.append("<result>");
        for (int i = 0; i < terms.length; i++) {
            Term term = terms[i];
            if (term == null)
                continue;
            w.append("<binding name=\"").append(vars.get(i)).append("\">");
            switch (term.type()) {
                case URI -> writeEscapedContent(w.append("<uri>"), term).append("</uri>");
                case BLANK -> {
                    boolean anon = term.sparql().charAt(0) == '[';
                    w.append("<bnode>")
                            .append(anon ? UUID.randomUUID().toString() : term.content())
                            .append("</bnode>");
                }
                case LITERAL -> {
                    w.append("<literal");
                    if (term.isLangStringLiteral()) {
                        w.append(" xml:lang=\"").append(term.lang()).append("\"");
                    } else {
                        w.append(" datatype=\"").append(term.datatype()).append("\"");
                    }
                    writeEscapedContent(w.append(">"), term).append("</literal>");
                }
            }
            w.append("</binding>");
        }
        w.write("</result>");
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
                for (@Nullable Term @NonNull[] row : solutions)
                    writeRow(vars, row, w);
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
