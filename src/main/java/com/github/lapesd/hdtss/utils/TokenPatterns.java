package com.github.lapesd.hdtss.utils;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.regex.Pattern;

import static java.util.regex.Pattern.compile;

public class TokenPatterns {
    private static final String PN_CHARS_BASE = "[A-Za-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD]";
    private static final String PN_CHARS_U = PN_CHARS_BASE+"|[_:]";
    private static final String PN_CHARS = PN_CHARS_U+"|[-0-9\u00B7\u0300-\u036F\u203F-\u2040]";
    private static final String UCHAR = "\\\\(?:u[0-9a-fA-F]{4}|U[0-9a-fA-F]{8})";
    private static final String LANGTAG = "[a-zA-Z]+(?:-[a-zA-Z0-9]+)*";
    private static final String IRIREF = "<([^\u0000-\u0020<>\"{}|^`\\\\]|"+UCHAR+")*>";
    private static final String LITERAL_CHAR = "[^\"\n\r\\\\]|\\\\[tbnrf\"'\\\\]|"+UCHAR;


    public static final @NonNull Pattern BNODE_RX = Pattern.compile("(_:)?((?:"+PN_CHARS_U+"|[0-9])(?:(?:"+PN_CHARS+"|\\.)*"+PN_CHARS+")?)");
    public static final @NonNull Pattern VAR_NAME_RX = compile("[a-zA-Z_0-9\u00B7\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0300-\u037D\u037F-\u1FFF\u200C\u200D\u203F-\u2040\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD\u10000-\uEFFFF]+");
    public static final @NonNull Pattern LANGTAG_RX = compile(LANGTAG);
    public static final @NonNull Pattern IRIREF_RX = compile(IRIREF);
    public static final @NonNull Pattern LITERAL_RX = compile(
            "\"("+LITERAL_CHAR+")*\"(@"+LANGTAG+"|\\^\\^"+IRIREF+")?");
}
