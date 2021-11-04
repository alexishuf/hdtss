package com.github.lapesd.hdtss.model;

import org.checkerframework.checker.nullness.qual.NonNull;

public enum LiteralQuote {
    SINGLE,
    DOUBLE,
    LONG_SINGLE,
    LONG_DOUBLE;

    @Override public @NonNull String toString() {
        return switch (this) {
            case DOUBLE -> "\"";
            case SINGLE -> "'";
            case LONG_DOUBLE -> "\"\"\"";
            case LONG_SINGLE -> "'''";
        };
    }

    public int length() {
        return switch (this) {
            case DOUBLE, SINGLE -> 1;
            case LONG_DOUBLE, LONG_SINGLE -> 3;
        };
    }

    public char symbol() {
        return switch (this) {
            case DOUBLE, LONG_DOUBLE ->  '"';
            case SINGLE, LONG_SINGLE -> '\'';
        };
    }

    public boolean isSingle() {
        return switch (this) {
            case SINGLE, LONG_SINGLE -> true;
            default -> false;
        };
    }

    public boolean isDouble() {
        return switch (this) {
            case DOUBLE, LONG_DOUBLE -> true;
            default -> false;
        };
    }

    public boolean isLong() {
        return switch (this) {
            case LONG_DOUBLE,LONG_SINGLE -> true;
            default -> false;
        };
    }

    public static @NonNull LiteralQuote fromLiteral(@NonNull CharSequence literal) {
        int n = 0, type = 0;
        for (int i = 0, length = literal.length(); i < length; i++) {
            if (type == 0) {
                type = literal.charAt(i);
                if (type != '\'' && type != '"')
                    break;
            } else if (literal.charAt(i) != type) {
                break;
            }
            ++n;
        }
        if (n == 0)
            throw new IllegalArgumentException("Given NT is not a quoted literal: "+literal);
        int lastQuote = literal.length()-1;
        while (lastQuote >= n && literal.charAt(lastQuote) != type) --lastQuote;
        if (n >= 3 && lastQuote >= 5)
            return type == '"' ? LONG_DOUBLE : LONG_SINGLE;
        return type == '"' ? DOUBLE : SINGLE;
    }
}
