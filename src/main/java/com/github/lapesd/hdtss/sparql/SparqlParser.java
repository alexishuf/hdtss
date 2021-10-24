package com.github.lapesd.hdtss.sparql;

import com.github.lapesd.hdtss.model.nodes.Op;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CharSequenceReader;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Converts a SPARQL query string into a SPARQL algebra represented as tree of {@link Op}s.
 */
public interface SparqlParser {

    /**
     * Convert a SPARQL string to an SPARQL algebra represented with {@link Op}s.
     *
     * @param sparql the SPARQL string to parse
     * @return the root of the equivalent SPARQL algebra
     * @throws FeatureNotSupportedException if the SPARQL query uses some feature not yet
     *         supported by this parser.
     * @throws SparqlSyntaxException if the query has a syntax error
     */
    default @NonNull Op
    parse(@NonNull CharSequence sparql) throws SparqlParserException {
        try (CharSequenceReader reader = new CharSequenceReader(sparql)) {
            return parse(reader);
        } catch (IOException e) {
            throw new RuntimeException("Unexpected IOException", e);
        }
    }

    /**
     * Convert a SPARQL string to an SPARQL algebra represented with {@link Op}s.
     *
     * @param sparql A UTF-8 encoded {@link InputStream} with a SPARQL query string
     * @return the root of the equivalent SPARQL algebra
     * @throws IOException if an error occurs while reading the {@link InputStream} sparql
     * @throws FeatureNotSupportedException if the SPARQL query uses some feature not yet
     *         supported by this parser.
     * @throws SparqlSyntaxException if the query has a syntax error
     */
    default @NonNull Op
    parse(@NonNull InputStream sparql) throws IOException, SparqlParserException {
        try (Reader reader = new InputStreamReader(sparql, StandardCharsets.UTF_8)) {
            return parse(reader);
        }
    }

    /**
     * Convert a SPARQL string to an SPARQL algebra represented with {@link Op}s.
     *
     * @param sparql A {@link Reader} over a SPARQL string
     * @return the root of the equivalent SPARQL algebra
     * @throws IOException if an error occurs while reading the {@link InputStream} sparql
     * @throws FeatureNotSupportedException if the SPARQL query uses some feature not yet
     *         supported by this parser.
     * @throws SparqlSyntaxException if the query has a syntax error
     */
    default @NonNull Op
    parse(@NonNull Reader sparql) throws IOException, SparqlParserException {
        return parse(IOUtils.toString(sparql));
    }
}
