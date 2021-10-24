package com.github.lapesd.hdtss.sparql.impl;

import com.github.lapesd.hdtss.sparql.SparqlParser;
import com.github.lapesd.hdtss.sparql.SparqlParserTestBase;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;

@Tag("fast")
class JenaSparqlParserTest extends SparqlParserTestBase {
    @Override protected @NonNull SparqlParser createParser() {
        return new JenaSparqlParser();
    }
}