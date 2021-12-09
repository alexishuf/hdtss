package com.github.lapesd.hdtss.sparql;

public class EmptySparqlException extends SparqlParserException {
    public EmptySparqlException() {
        super("No (or empty) query provided", "");
    }
}
