package com.github.lapesd.hdtss.vocab;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;

@SuppressWarnings("unused")
public class RDFS {
    public static final @NonNull String NS = "http://www.w3.org/2000/01/rdf-schema#";

    public static final @NonNull String domain = NS+"domain";
    public static final @NonNull String subPropertyOf = NS+"subPropertyOf";
    public static final @NonNull String comment = NS+"comment";
    public static final @NonNull String Resource = NS+"Resource";
    public static final @NonNull String seeAlso = NS+"seeAlso";
    public static final @NonNull String subClassOf = NS+"subClassOf";
    public static final @NonNull String Datatype = NS+"Datatype";
    public static final @NonNull String range = NS+"range";
    public static final @NonNull String member = NS+"member";
    public static final @NonNull String Container = NS+"Container";
    public static final @NonNull String Class = NS+"Class";
    public static final @NonNull String ContainerMembershipProperty = NS+"ContainerMembershipProperty";
    public static final @NonNull String isDefinedBy = NS+"isDefinedBy";
    public static final @NonNull String Literal = NS+"Literal";
    public static final @NonNull String label = NS+"label";

    public static final @NonNull Term domainTerm = new Term("<"+domain+">");
    public static final @NonNull Term subPropertyOfTerm = new Term("<"+subPropertyOf+">");
    public static final @NonNull Term commentTerm = new Term("<"+comment+">");
    public static final @NonNull Term ResourceTerm = new Term("<"+Resource+">");
    public static final @NonNull Term seeAlsoTerm = new Term("<"+seeAlso+">");
    public static final @NonNull Term subClassOfTerm = new Term("<"+subClassOf+">");
    public static final @NonNull Term DatatypeTerm = new Term("<"+Datatype+">");
    public static final @NonNull Term rangeTerm = new Term("<"+range+">");
    public static final @NonNull Term memberTerm = new Term("<"+member+">");
    public static final @NonNull Term ContainerTerm = new Term("<"+Container+">");
    public static final @NonNull Term ClassTerm = new Term("<"+Class+">");
    public static final @NonNull Term ContainerMembershipPropertyTerm = new Term("<"+ContainerMembershipProperty+">");
    public static final @NonNull Term isDefinedByTerm = new Term("<"+isDefinedBy+">");
    public static final @NonNull Term LiteralTerm = new Term("<"+Literal+">");
    public static final @NonNull Term labelTerm = new Term("<"+label+">");

}
