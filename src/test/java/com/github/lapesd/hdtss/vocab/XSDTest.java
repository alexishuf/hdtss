package com.github.lapesd.hdtss.vocab;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class XSDTest {
    public static final String[] XSD_URIs = {
            XSD.xboolean,
            XSD.integer,
            XSD.xint,
            XSD.xshort,
            XSD.decimal,
            XSD.xdouble,
            XSD.xfloat,
            XSD.string,
            XSD.duration,
            XSD.dateTime,
            XSD.date,
            XSD.time,
            XSD.anyType,
            XSD.anySimpleType,
            XSD.gYearMonth,
            XSD.gYear,
            XSD.gMonthDay,
            XSD.gDay,
            XSD.gMonth,
            XSD.base64Binary,
            XSD.hexBinary,
            XSD.anyURI
    };

    public static Stream<Arguments> testIntern() {
        List<Arguments> data = new ArrayList<>();
        for (String uri : XSD_URIs)
            data.add(arguments(uri, 0, uri.length(), uri));
        for (String uri : XSD_URIs)
            data.add(arguments("<"+uri+">", 1, uri.length()+1, uri));
        for (String uri : XSD_URIs)
            data.add(arguments("asd<"+uri+">", 4, uri.length()+4, uri));
        data.add(arguments(RDF.List, 0, RDF.List.length(), null));
        data.add(arguments(RDF.langString, 0, RDF.langString.length(), null));
        data.add(arguments("<"+RDF.langString+">", 1, RDF.langString.length()+1, null));
        for (String suffix : List.of("bullshit", "bsYear", "hDay", "ggDay", "xint")) {
            String uri = XSD.NS + suffix;
            data.add(arguments(uri, 0, uri.length(), null));
            data.add(arguments("<"+uri+">", 1, uri.length()+1, null));
        }
        return data.stream();
    }

    @ParameterizedTest @MethodSource
    void testIntern(String input, int begin, int end, @Nullable String expected) {
        assertSame(expected, XSD.intern(input, begin, end));
        assertSame(expected, XSD.intern(new StringBuilder(input), begin, end));
    }

    @ParameterizedTest @MethodSource("testIntern")
    void testTermDatatypeInters(String input, int begin, int end, @Nullable String expected) {
        String expectedDatatype = input.substring(begin, end);
        CharSequence datatype = new Term("\"\"^^<"+expectedDatatype+">").datatype();
        assertNotNull(datatype);
        assertEquals(expectedDatatype , datatype.toString());
        if (expected != null)
            assertSame(expected, datatype);
    }
}