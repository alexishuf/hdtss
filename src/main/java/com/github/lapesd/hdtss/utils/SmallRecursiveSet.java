package com.github.lapesd.hdtss.utils;

import lombok.ToString;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

/**
 * A small (less than 16 elements on most cases) set with recursive add/remove semantics,
 * i.e., after {@code n} {@code add(e)}s, {@code n} {@code remove(e)} calls must be done to
 * remove {@code e}.
 */
public class SmallRecursiveSet<T> extends AbstractCollection<T> {
    private @Nullable Object @NonNull [] values;
    private int @NonNull [] counters;
    private int iterationEnd = 0, nextFreeSlot = 0, size = 0;

    public SmallRecursiveSet() { this(16); }

    public SmallRecursiveSet(int capacity) {
        values = new String[capacity];
        counters = new int[capacity];
    }

    public static <U> @NonNull SmallRecursiveSet<U>
    fromDistinct(@NonNull Collection<U> initial) {
        if (initial instanceof SmallRecursiveSet<U> s)
            return s;
        var set = new SmallRecursiveSet<U>(Math.max(16, initial.size() + 8));
        int i = 0;
        for (U s : initial) {
            set.values[i] = s;
            set.counters[i++] = 1;
        }
        set.size = set.nextFreeSlot = set.iterationEnd = i;
        return set;
    }

    @SuppressWarnings("unchecked")
    @ToString(onlyExplicitlyIncluded = true)
    private final class It implements Iterator<T> {
        @ToString.Include private int last = -1;
        private @Nullable T next = null;

        @Override public boolean hasNext() {
            for (int i = last+1; next == null && i < iterationEnd; i++) {
                if (values[i] != null)
                    next = (T)values[last = i];
            }
            return next != null;
        }

        @Override public T next() {
            if (!hasNext())
                throw new NoSuchElementException();
            T next = this.next;
            assert next != null;
            this.next = null;
            return next;
        }
    }

    @Override public Iterator<T> iterator() { return new It(); }

    @Override public int size() { return size; }

    @Override public boolean contains(Object o) {
        if (o instanceof String string) {
            for (int i = 0; i < iterationEnd; i++) {
                if (values[i] != null && string.equals(values[i]))
                    return true;
            }
        }
        return false;
    }

    public boolean containsAny(@NonNull Collection<?> collection) {
        if (collection instanceof Set || collection.size() < size) {
            for (int i = 0; i < iterationEnd; i++) {
                if (values[i] != null && collection.contains(values[i]))
                    return true;
            }
        } else {
            for (Object e : collection) {
                if (contains(e)) return true;
            }
        }
        return false;
    }

    public @NonNull List<T> intersectionList(@NonNull Collection<T> other) {
        int oSize = other.size();
        List<T> result = new ArrayList<>(oSize);
        for (T s : other) {
            if (contains(s)) result.add(s);
        }
        int rSize = result.size();
        if      (rSize ==     0) return List.of();
        else if (rSize == oSize) return other instanceof List<T> ? (List<T>) other : result;
        else                     return result;
    }

    @SuppressWarnings({"SuspiciousMethodCalls", "unchecked"})
    public @NonNull Set<T> intersectionSet(@NonNull Collection<T> other) {
        int oSize = other.size();
        Set<T> result = new HashSet<>((int)Math.max(4, oSize/0.75f + 1));
        for (int i = 0; i < iterationEnd; i++) {
            if (values[i] != null && other.contains(values[i]))
                result.add((T)values[i]);
        }
        int rSize = result.size();
        if      (rSize ==     0) return Set.of();
        else if (rSize == oSize) return other instanceof Set ? (Set<T>) other : result;
        else                     return result;
    }

    @Override public boolean add(@NonNull T value) {
        boolean match = false;
        for (int i = 0; !match && i < iterationEnd; i++) {
            match = value.equals(values[i]);
            if (match) counters[i]++;
        }
        if (!match) {
            int length = values.length;
            if (nextFreeSlot >= length) {
                length += 8;
                values = Arrays.copyOf(values, length);
                counters = Arrays.copyOf(counters, length);
            }
            values[nextFreeSlot] = value;
            counters[nextFreeSlot] = 1;
            ++size;
            while (nextFreeSlot < length && values[nextFreeSlot] != null)
                ++nextFreeSlot;
            iterationEnd = Math.max(iterationEnd, nextFreeSlot);
        }
        return true;
    }

    @Override public void clear() {
        Arrays.fill(values, null);
        Arrays.fill(counters, 0);
        iterationEnd = nextFreeSlot = size = 0;
    }

    @Override public boolean remove(Object o) {
        if ((o instanceof String s)) {
            for (int i = 0; i < iterationEnd; i++) {
                if (s.equals(values[i])) {
                    decrement(i);
                    return true;
                }
            }
        }
        return false;
    }

    @Override public boolean removeAll(@NonNull Collection<?> collection) {
        boolean match = false;
        for (int i = 0; i < iterationEnd; i++) {
            Object mine = values[i];
            if (mine != null && collection.contains(mine)) {
                match = true;
                decrement(i);
            }
        }
        int i = Math.max(nextFreeSlot, iterationEnd-1);
        while (i > nextFreeSlot && values[i] == null)
            --i;
        iterationEnd = values[i] == null ? i : i+1;
        return match;
    }

    private void decrement(int i) {
        if (--counters[i] == 0) {
            values[i] = null;
            nextFreeSlot = Math.min(nextFreeSlot, i);
            --size;
        }
    }
}
