package com.github.lapesd.hdtss.vocab;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings("unused")
public class XSD {
    public static final @NonNull String NS = "http://www.w3.org/2001/XMLSchema#";

    public static final @NonNull String xboolean = NS+"boolean";
    public static final @NonNull String integer  = NS+"integer";
    public static final @NonNull String xint  = NS+"int";
    public static final @NonNull String xshort  = NS+"short";
    public static final @NonNull String decimal  = NS+"decimal";
    public static final @NonNull String xdouble  = NS+"double";
    public static final @NonNull String xfloat  = NS+"float";
    public static final @NonNull String string  = NS+"string";
    public static final @NonNull String duration  = NS+"duration";
    public static final @NonNull String dateTime  = NS+"dateTime";
    public static final @NonNull String date  = NS+"date";
    public static final @NonNull String time  = NS+"time";
    public static final @NonNull String anyType  = NS+"anyType";
    public static final @NonNull String anySimpleType  = NS+"anySimpleType";
    public static final @NonNull String gYearMonth  = NS+"gYearMonth";
    public static final @NonNull String gYear  = NS+"gYear";
    public static final @NonNull String gMonthDay  = NS+"gMonthDay";
    public static final @NonNull String gDay  = NS+"gDay";
    public static final @NonNull String gMonth  = NS+"gMonth";
    public static final @NonNull String base64Binary  = NS+"base64Binary";
    public static final @NonNull String hexBinary  = NS+"hexBinary";
    public static final @NonNull String anyURI  = NS+"anyURI";

    public static @Nullable String intern(CharSequence cs, int begin, int end) {
        // http://www.w3.org/2001/XMLSchema#
        // 012345678901234567890123456789012
        // 0         1         2         3
        if (end-begin <= 32) return null;
        if (cs.charAt(begin+23) != 'X' || cs.charAt(begin+24) != 'M') return null;
        if (cs.charAt(begin+25) != 'L' || cs.charAt(begin+32) != '#') return null;
        char e1 = cs.charAt(end - 1), e2 = cs.charAt(end - 2);
        @Nullable String candidate = switch (e1) {
            case 'g' -> string;
            case 'n' -> switch (e2) {case 'a'->xboolean; case 'o'->duration; default->null;};
            case 'r' -> switch (e2) {case 'e'->integer; case 'a'->gYear; default->null;};
            case 't' -> switch (e2) {case 'n'->xint; case 'r'->xshort; case 'a'->xfloat; default->null;};
            case 'e' -> switch (e2) {
                case 'l' -> xdouble;
                case 't' -> date;
                case 'p' -> cs.charAt(end-5) == 'y' ? anyType : anySimpleType;
                case 'm' -> cs.charAt(end-5) == 'e' ? dateTime : time;
                default -> null;
            };
            case 'I' -> anyURI;
            case 'l' -> decimal;
            case 'h' -> cs.charAt(end-6) == 'r' ? gYearMonth : gMonth;
            case 'y' -> switch (e2) {
                case 'a' -> cs.charAt(end-4) == 'h' ? gMonthDay : gDay;
                case 'r' -> cs.charAt(end-7) == '4' ? base64Binary : hexBinary;
                default -> null;
            };
            default -> null;
        };
        if (candidate == null)
            return null;
        for (int i = 33, len = candidate.length(); i < len; i++) {
            if (cs.charAt(begin+i) != candidate.charAt(i)) return null;
        }
        return candidate; // candidate equals cs subsequence
    }

    public static final @NonNull Term xbooleanTerm = new Term("<"+xboolean+">");
    public static final @NonNull Term integerTerm = new Term("<"+integer+">");
    public static final @NonNull Term xintTerm = new Term("<"+xint+">");
    public static final @NonNull Term xshortTerm = new Term("<"+xshort+">");
    public static final @NonNull Term decimalTerm = new Term("<"+decimal+">");
    public static final @NonNull Term xdoubleTerm = new Term("<"+xdouble+">");
    public static final @NonNull Term xfloatTerm = new Term("<"+xfloat+">");
    public static final @NonNull Term stringTerm = new Term("<"+string+">");
    public static final @NonNull Term durationTerm = new Term("<"+duration+">");
    public static final @NonNull Term dateTimeTerm = new Term("<"+dateTime+">");
    public static final @NonNull Term dateTerm = new Term("<"+date+">");
    public static final @NonNull Term timeTerm = new Term("<"+time+">");
    public static final @NonNull Term anyTypeTerm = new Term("<"+anyType+">");
    public static final @NonNull Term anySimpleTypeTerm = new Term("<"+anySimpleType+">");
    public static final @NonNull Term gYearMonthTerm = new Term("<"+gYearMonth+">");
    public static final @NonNull Term gYearTerm = new Term("<"+gYear+">");
    public static final @NonNull Term gMonthDayTerm = new Term("<"+gMonthDay+">");
    public static final @NonNull Term gDayTerm = new Term("<"+gDay+">");
    public static final @NonNull Term gMonthTerm = new Term("<"+gMonth+">");
    public static final @NonNull Term base64BinaryTerm = new Term("<"+base64Binary+">");
    public static final @NonNull Term hexBinaryTerm = new Term("<"+hexBinary+">");
    public static final @NonNull Term anyURITerm = new Term("<"+anyURI+">");

    public static final @NonNull Term xfalse = new Term("\"false\"^^<"+xboolean+">");
    public static final @NonNull Term xtrue = new Term("\"true\"^^<"+xboolean+">");
}
