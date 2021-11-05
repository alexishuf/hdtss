package com.github.lapesd.hdtss.sparql.results;

import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.results.codecs.XMLEncoder;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class XMLEncoderTest extends CodecTestBase {

    public XMLEncoderTest() {
        super(XMLEncoder.class);
    }

    static Stream<Arguments> data() {
        return Stream.of(
                arguments(ASK_FALSE, """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <sparql xmlns="http://www.w3.org/2005/sparql-results#">
                          <head></head>
                          <boolean>false</boolean>
                        </sparql>"""),
                arguments(ASK_TRUE, """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <sparql xmlns="http://www.w3.org/2005/sparql-results#">
                          <head></head>
                          <boolean>true</boolean>
                        </sparql>"""),
                arguments(ONE_ONE, """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <sparql xmlns="http://www.w3.org/2005/sparql-results#">
                          <head>
                            <variable name="x"/>
                          </head>
                          <results>
                            <result>
                              <binding name="x"><uri>http://example.org/Alice</uri></binding>
                            </result>
                          </results>
                        </sparql>"""),
                arguments(ONE_ROW, """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <sparql xmlns="http://www.w3.org/2005/sparql-results#">
                          <head>
                            <variable name="x"/>
                            <variable name="y"/>
                            <variable name="z"/>
                            <variable name="w"/>
                          </head>
                          <results>
                            <result>
                              <binding name="x"><literal xml:lang="en">Alice</literal></binding>
                              <binding name="y"><literal datatype="http://www.w3.org/2001/XMLSchema#string">bob</literal></binding>
                              <binding name="z"><literal datatype="http://www.w3.org/2001/XMLSchema#integer">23</literal></binding>
                              <binding name="w"><bnode>blank1</bnode></binding>
                            </result>
                          </results>
                        </sparql>"""),
                arguments(TWO_ROWS, """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <sparql xmlns="http://www.w3.org/2005/sparql-results#">
                          <head>
                            <variable name="x"/>
                          </head>
                          <results>
                            <result></result>
                            <result>
                              <binding name="x"><uri>http://example.org/Charlie</uri></binding>
                            </result>
                          </results>
                        </sparql>"""),
                arguments(XML_PROBLEMATIC, """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <sparql xmlns="http://www.w3.org/2005/sparql-results#">
                          <head>
                            <variable name="x"/>
                            <variable name="y"/>
                          </head>
                          <results>
                            <result>
                              <binding name="x"><literal datatype="http://www.w3.org/2001/XMLSchema#string">&amp;</literal></binding>
                              <binding name="y"><literal datatype="http://www.w3.org/2001/XMLSchema#string">&lt;&amp;&gt;</literal></binding>
                            </result>
                            <result>
                              <binding name="x"><uri>http://example.org/search?q=1&amp;o=2</uri></binding>
                              <binding name="y"><literal datatype="http://www.w3.org/2001/XMLSchema#string">x &gt; 2
                        ${{FORCE_LF}}&amp;&amp; y &lt; 3</literal></binding>
                            </result>
                          </results>
                        </sparql>"""),
                arguments(CSV_PROBLEMATIC, """
                        <?xml version="1.0" encoding="UTF-8"?>
                        <sparql xmlns="http://www.w3.org/2005/sparql-results#">
                          <head>
                            <variable name="x"/>
                            <variable name="y"/>
                          </head>
                          <results>
                            <result>
                              <binding name="x"><literal datatype="http://www.w3.org/2001/XMLSchema#string">"1
                        ${{FORCE_LF}}2"</literal></binding>
                              <binding name="y"><literal xml:lang="en">a,b</literal></binding>
                            </result>
                          </results>
                        </sparql>""")
        );
    }

    private @NonNull String strip(@NonNull String s) {
        return s.replaceAll(" *\n *", "").replaceAll("\\$\\{\\{FORCE_LF}}", "\n");
    }

    @ParameterizedTest @MethodSource("data")
    public void testEncodeToStream(@NonNull QuerySolutions solutions, @NonNull String expected) {
        doTestEncodeToStream(solutions, strip(expected));
    }

    @ParameterizedTest @MethodSource("data")
    public void testEncodeToArray(@NonNull QuerySolutions solutions, @NonNull String expected) {
        doTestEncodeToArray(solutions, strip(expected));
    }

    @ParameterizedTest @MethodSource("data")
    public void testEncodeToBuffer(@NonNull QuerySolutions solutions, @NonNull String expected) {
        doTestEncodeToBuffer(solutions, strip(expected));
    }
}