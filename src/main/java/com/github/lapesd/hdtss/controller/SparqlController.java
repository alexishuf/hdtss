package com.github.lapesd.hdtss.controller;

import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.SparqlParser;
import org.checkerframework.checker.nullness.qual.NonNull;

public interface SparqlController {
    @NonNull SparqlParser parser();
    @NonNull OpExecutorDispatcher dispatcher();
}
