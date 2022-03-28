package com.github.lapesd.hdtss.model;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("fast")
class LiteralQuoteTest {

    @ParameterizedTest @ValueSource(strings = {
            /* no content */
            "\"\"             | DOUBLE",
            "''               | SINGLE",
            "\"\"\"\"\"\"     | LONG_DOUBLE",
            "''''''           | LONG_SINGLE",
            /* asd as content */
            "\"asd\"          | DOUBLE",
            "'asd'            | SINGLE",
            "\"\"\"asd\"\"\"  | LONG_DOUBLE",
            "'''asd'''        | LONG_SINGLE",
            /* ^ as content */
            "\"^\"            | DOUBLE",
            "'^'              | SINGLE",
            "\"\"\"^\"\"\"    | LONG_DOUBLE",
            "'''^'''          | LONG_SINGLE",
            /* \ as content */
            "\"\\\"          | DOUBLE",
            "'\\'            | SINGLE",
            "\"\"\"\\\"\"\"  | LONG_DOUBLE",
            "'''\\'''        | LONG_SINGLE",
            /* single quote char as content */
            "\"\"\"          | DOUBLE",
            "'''             | SINGLE",
            "\"\"\"\"\"\"\"  | LONG_DOUBLE",
            "'''''''         | LONG_SINGLE",

            /* add language tag */
                /* no content */
                "\"\"@en                | DOUBLE",
                "''@en                  | SINGLE",
                "\"\"\"\"\"\"@en        | LONG_DOUBLE",
                "''''''@en              | LONG_SINGLE",
                /* asd as content */
                "\"asd\"@pt-BR          | DOUBLE",
                "'asd'@pt-BR            | SINGLE",
                "\"\"\"asd\"\"\" @pt-BR | LONG_DOUBLE",
                "'''asd'''@pt-BR        | LONG_SINGLE",
                /* ^ as content */
                "\"^\"@en               | DOUBLE",
                "'^'@en                 | SINGLE",
                "\"\"\"^\"\"\"@en       | LONG_DOUBLE",
                "'''^'''@en             | LONG_SINGLE",
                /* \ as content */
                "\"\\\"@en              | DOUBLE",
                "'\\'@en                | SINGLE",
                "\"\"\"\\\"\"\"@en      | LONG_DOUBLE",
                "'''\\'''@en            | LONG_SINGLE",
                /* single quote char as content */
                "\"\"\"@pt-BR           | DOUBLE",
                "'''@pt-BR              | SINGLE",
                "\"\"\"\"\"\"\"@pt-BR   | LONG_DOUBLE",
                "'''''''@pt-BR          | LONG_SINGLE",

            /* add  datatype */
                /* no content */
                "\"\"^^<http://www.w3.org/2001/XMLSchema#string>           | DOUBLE",
                "''^^<http://www.w3.org/2001/XMLSchema#string>             | SINGLE",
                "\"\"\"\"\"\"^^<http://www.w3.org/2001/XMLSchema#string>   | LONG_DOUBLE",
                "''''''^^<http://www.w3.org/2001/XMLSchema#string>         | LONG_SINGLE",
                /* asd as content */
                "\"asd\"^^xsd:string                                       | DOUBLE",
                "'asd'^^xsd:string                                         | SINGLE",
                "\"\"\"asd\"\"\"^^xsd:string                               | LONG_DOUBLE",
                "'''asd'''^^xsd:string                                     | LONG_SINGLE",
                /* ^ as content */
                "\"^\"^^<http://www.w3.org/2001/XMLSchema#string>          | DOUBLE",
                "'^'^^<http://www.w3.org/2001/XMLSchema#string>            | SINGLE",
                "\"\"\"^\"\"\"^^<http://www.w3.org/2001/XMLSchema#string>  | LONG_DOUBLE",
                "'''^'''^^<http://www.w3.org/2001/XMLSchema#string>        | LONG_SINGLE",
                /* \ as content */
                "\"\\\"^^xsd:string                                        | DOUBLE",
                "'\\'^^xsd:string                                          | SINGLE",
                "\"\"\"\\\"\"\"^^xsd:string                                | LONG_DOUBLE",
                "'''\\'''^^xsd:string                                      | LONG_SINGLE",
                /* single quote char as content */
                "\"\"\"^^<http://www.w3.org/2001/XMLSchema#string>         | DOUBLE",
                "'''^^<http://www.w3.org/2001/XMLSchema#string>            | SINGLE",
                "\"\"\"\"\"\"\"^^<http://www.w3.org/2001/XMLSchema#string> | LONG_DOUBLE",
                "'''''''^^<http://www.w3.org/2001/XMLSchema#string>        | LONG_SINGLE",
    })
    void testFromLiteral(String data) {
        String[] parts = data.split(" *\\| *");
        LiteralQuote expected = LiteralQuote.valueOf(parts[1]);
        assertEquals(expected, LiteralQuote.fromLiteral(parts[0]));
    }
}