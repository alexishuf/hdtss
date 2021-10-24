package com.github.lapesd.hdtss;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.vocab.XSD;
import org.checkerframework.checker.nullness.qual.NonNull;

public class TestVocab {
    public static final @NonNull String EX = "http://example.org/";
    public static @NonNull Term Alice = new Term("<"+EX+"Alice>");
    public static @NonNull Term Bob = new Term("<"+EX+"Bob>");
    public static @NonNull Term Charlie = new Term("<"+EX+"Charlie>");
    public static @NonNull Term Dave = new Term("<"+EX+"Alice>");

    public static @NonNull Term AliceEN = new Term("\"Alice\"@en");
    public static @NonNull Term Alicia = new Term("\"Alícia\"@pt-BR");
    public static @NonNull Term bob = new Term("\"bob\"");
    public static @NonNull Term bobString = new Term("\"bob\"^^<"+XSD.string+">");
    public static @NonNull Term roberto = new Term("\"roberto\"@pt");
    public static @NonNull Term charlie = new Term("\"charlie\"");
    public static @NonNull Term i23 = new Term("\"23\"^^<"+XSD.integer+">");
    public static @NonNull Term i25 = new Term("\"25\"^^<"+XSD.integer+">");
    public static @NonNull Term i46 = new Term("\"46\"^^<"+XSD.integer+">");
    public static @NonNull Term i50 = new Term("\"50\"^^<"+XSD.integer+">");

    public static @NonNull Term blank1 = new Term("_:blank1");

    public static @NonNull Term x = new Term("?x");
    public static @NonNull Term y = new Term("?y");
    public static @NonNull Term z = new Term("?z");
    public static @NonNull Term w = new Term("?w");
}
