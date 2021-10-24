package com.github.lapesd.hdtss.vocab;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;

@SuppressWarnings("unused")
public class RDF {
    public static final @NonNull String NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    public static final @NonNull String XMLLiteral = NS+"XMLLiteral";
    public static final @NonNull String Property = NS+"Property";
    public static final @NonNull String JSON = NS+"JSON";
    public static final @NonNull String direction = NS+"direction";
    public static final @NonNull String rest = NS+"rest";
    public static final @NonNull String Statement = NS+"Statement";
    public static final @NonNull String Seq = NS+"Seq";
    public static final @NonNull String type = NS+"type";
    public static final @NonNull String first = NS+"first";
    public static final @NonNull String langString = NS+"langString";
    public static final @NonNull String predicate = NS+"predicate";
    public static final @NonNull String HTML = NS+"HTML";
    public static final @NonNull String Alt = NS+"Alt";
    public static final @NonNull String value = NS+"value";
    public static final @NonNull String PlainLiteral = NS+"PlainLiteral";
    public static final @NonNull String Bag = NS+"Bag";
    public static final @NonNull String language = NS+"language";
    public static final @NonNull String List = NS+"List";
    public static final @NonNull String subject = NS+"subject";
    public static final @NonNull String CompoundLiteral = NS+"CompoundLiteral";
    public static final @NonNull String nil = NS+"nil";
    public static final @NonNull String object = NS+"object";

    public static final @NonNull Term XMLLiteralTerm = new Term("<"+XMLLiteral+">");
    public static final @NonNull Term PropertyTerm = new Term("<"+Property+">");
    public static final @NonNull Term JSONTerm = new Term("<"+JSON+">");
    public static final @NonNull Term directionTerm = new Term("<"+direction+">");
    public static final @NonNull Term restTerm = new Term("<"+rest+">");
    public static final @NonNull Term StatementTerm = new Term("<"+Statement+">");
    public static final @NonNull Term SeqTerm = new Term("<"+Seq+">");
    public static final @NonNull Term typeTerm = new Term("<"+type+">");
    public static final @NonNull Term firstTerm = new Term("<"+first+">");
    public static final @NonNull Term langStringTerm = new Term("<"+langString+">");
    public static final @NonNull Term predicateTerm = new Term("<"+predicate+">");
    public static final @NonNull Term HTMLTerm = new Term("<"+HTML+">");
    public static final @NonNull Term AltTerm = new Term("<"+Alt+">");
    public static final @NonNull Term valueTerm = new Term("<"+value+">");
    public static final @NonNull Term PlainLiteralTerm = new Term("<"+PlainLiteral+">");
    public static final @NonNull Term BagTerm = new Term("<"+Bag+">");
    public static final @NonNull Term languageTerm = new Term("<"+language+">");
    public static final @NonNull Term ListTerm = new Term("<"+List+">");
    public static final @NonNull Term subjectTerm = new Term("<"+subject+">");
    public static final @NonNull Term CompoundLiteralTerm = new Term("<"+CompoundLiteral+">");
    public static final @NonNull Term nilTerm = new Term("<"+nil+">");
    public static final @NonNull Term objectTerm = new Term("<"+object+">");


}
