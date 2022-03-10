package com.github.lapesd.hdtss.data.query.impl;

import com.github.lapesd.hdtss.data.query.CardinalityEstimator;
import com.github.lapesd.hdtss.data.query.HdtQueryService;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.github.lapesd.hdtss.vocab.RDF;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.index.qual.Positive;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.rdfhdt.hdt.enums.TripleComponentRole.*;

@Slf4j
@Singleton
@Requires(property = "hdt.estimator", value = "PEEK", defaultValue = "PEEK")
public class PeekCardinalityEstimator implements CardinalityEstimator {
    // 1 int = 4 bytes, 1 Integer = int+pointer+2*overhead = 4+8+2*4 = 20
    // 1 entry = 2 Integer + reference + overhead = 40+8+4 = 52
    // round up to 64 and estimate capacity of 512 MiB
    private static final int MAX_CLASSES_CACHED = (512*1024*1024) / 64;

    // capacity of 512 MiB int[]
    private static final int MAX_PREDICATES_CACHED = (512*1024*1024) / 4;

    private final @NonNull HDT hdt;
    private final long typeId;
    private final long nPred, nSub, nObj, nTriples;
    private final @Nullable Cache predicateCache;
    private final @Nullable Cache classCache;
    private final @NonNull Lookup lookup;

    /* --- --- --- public interface --- --- --- */

    public enum Lookup {
        NEVER,
        CACHED,
        ALWAYS
    }

    public PeekCardinalityEstimator(
            @NonNull HdtQueryService hdtQueryService,
            @Property(name = "hdt.estimator.prefetch", defaultValue = "true") boolean prefetch,
            @Property(name = "hdt.estimator.lookup", defaultValue = "ALWAYS")
                    @NonNull Lookup lookup) {
        this.lookup = lookup;
        hdt = hdtQueryService.hdt();
        Dictionary dict = hdt.getDictionary();
        typeId = HDTUtils.toHDTId(dict, RDF.typeTerm, PREDICATE);
        nPred = dict.getNpredicates();
        nSub  = dict.getNsubjects();
        nObj  = dict.getNobjects();
        nTriples = hdt.getTriples().size();
        if (nTriples > 0) {
            assert nPred > 0 : "nTriples > 0, but no predicates in dictionary";
            assert nSub  > 0 : "nTriples > 0, but no subjects in dictionary";
            assert nObj  > 0 : "nTriples > 0, but no objects in dictionary";
        }
        if (lookup.ordinal() >= Lookup.CACHED.ordinal()) {
            predicateCache = new PredicateCache((int)Math.min(MAX_PREDICATES_CACHED, nPred));
            classCache = new ClassCache();
            if (prefetch) {
                predicateCache.startPrefetch();
                classCache.startPrefetch();
            }
        } else {
            predicateCache = null;
            classCache = null;
        }
    }

    @Override public long estimate(TriplePattern tp) {
        if (nTriples == 0)
            return 0;
        Dictionary dictionary = hdt.getDictionary();
        long s = HDTUtils.toHDTId(dictionary, tp.  subject(), SUBJECT);
        long p = HDTUtils.toHDTId(dictionary, tp.predicate(), PREDICATE);
        long o = HDTUtils.toHDTId(dictionary, tp.   object(), OBJECT);
        if (s == -1 || p == -1 || o == -1)
            return 0;
        if (s == 0 && p == 0 && o == 0)
            return nTriples;
        if (s > 0 && p > 0 && o > 0)
            return 1;
        if (lookup == Lookup.NEVER) // only dictionary size heuristics is allowed
            return patternEstimate(s, p, o);
        if (p > 0 && s == 0) { //try to match this pattern to a cache
            if (o == 0)
                return estimate(predicateCache, p, s, p, o);
            else if (p == typeId)
                return estimate(classCache, o, s, p, o);
        }
        if (lookup == Lookup.CACHED) // s p o is not cached, but we can use the cache to weigh
            return weighedPatternEstimate(s, p, o);
        else // lookup both s p o and the cache to weigh heuristic (if lookup is not exact)
            return estimate(null, 0, s, p, o);
    }

    @Override public String toString() {
        return format("%s@%x", getClass().getSimpleName(), System.identityHashCode(this));
    }

    /* --- --- --- estimate logic --- --- --- */

    private long estimate(@Nullable Cache cache, @NonNegative long id,
                          @NonNegative long s, @NonNegative long p, @NonNegative long o) {
        assert cache == null || id > 0 : "cache given, but id <= 0";
        long peek = cache != null ? cache.get(id) : peek(s, p, o);
        if (peek >= 0)
            return peek; // peek() got an EXACT estimation
        //else: compute a heuristic and normalize it with cached "?s <p> ?o" or "?s a <class>"
        long weighedHeuristic = weighedPatternEstimate(s, p, o);
        if (peek == -1) { // pek got UNKNOWN estimation, use (weighed) heuristic
            return weighedHeuristic;
        } else { // neither EXACT nor UNKNOWN,
            long hdtEstimate = -1 * (peek + 1);
            return Math.max(1, (weighedHeuristic + hdtEstimate) / 2);
        }
    }

    private long peek(@NonNegative long s, @NonNegative long p, @NonNegative long o) {
        IteratorTripleID it = hdt.getTriples().search(new TripleID(s, p, o));
        ResultEstimationType type = it.numResultEstimation();
        long estimate = it.estimatedNumResults();
        if (type == ResultEstimationType.EXACT)
            return estimate;
        else if (type != ResultEstimationType.UNKNOWN)
            return -1 * Math.max(1, estimate) - 1;
        // it.numResultEstimation() cannot be relied upon: even if UNKNOWN is returned the
        // estimate is exact (by design) or an overestimation (also by design).
        // if there are indices and only one bound term, it is simply an enumeration of an
        // index looking up the bound term
        int nGround = (s != 0 ? 1 : 0) + (p != 0 ? 1 : 0) + (o != 0 ? 1 : 0);
        if (nGround < 2)
            return estimate;
        // else, maybe UNKNOWN is really UNKNOWN
        return -1 * Math.max(1, estimate) - 1;
    }

    private long
    weighedPatternEstimate(@NonNegative long s, @NonNegative long p, @NonNegative long o) {
        long heuristic = patternEstimate(s, p, o);
        if (p > 0 && predicateCache != null)
            return Math.max(1, (long) (heuristic * (0.5 + 0.5*predicateCache.normalized(p))));
        else
            return heuristic;
    }

    private long patternEstimate(@NonNegative long s, @NonNegative long p, @NonNegative long o) {
        /* When estimating, always take these assumptions:
         * 1. Every unique subject has at least one rdf:type triple
         * 2. There are very few classes
         * 3. triples are linearly distributed among subjects: nTriples=k*nSub
         * 4. triples are linearly distributed among non-rdf:type predicates */
        if (p == typeId) {
            if (s == 0 && o == 0) // ?s a ?class
                return nSub;                 // assume each subject has a single type
            else if (s == 0) // ?s a <class>
                return Math.max(1, nSub / 2); // assume only two classes
            else // <s> a ?class
                return 5;                   // assume 5 classes per subject
        } else if (p > 0) { // <p> != rdf:type
            long nonTypeTriples = nTriples - nSub;
            long nonTypePredicates = Math.max(1, nPred - 1);
            long triplesWithPredicate = nonTypeTriples / nonTypePredicates;
            if (s == 0 && o == 0) // ?s <p> ?o
                return Math.max(500, triplesWithPredicate);
            else if (s == 0)      // ?s <p> <o>
                return Math.max(100, triplesWithPredicate / nObj);
            else                  // <s> <p> ?o
                return Math.max(10, triplesWithPredicate / nSub);
        } else {
            if (s == 0 && o == 0) {   // ?s ?p ?o
                return nTriples;
            } else if (s == 0) {      // ?s ?p <o>
                // this can underestimate a lot, thus enforce a large lower bound
                return Math.max(300, nTriples / nObj);
            } else {                  // <s> ?p ?o
                // typically a subject has few PO pairs, thus use a smaller lower bound
                return Math.max(50, nTriples / nSub);
            }
        }
    }

    /* --- --- --- caches machinery --- --- --- */

    @PreDestroy
    public void abortPrefetch() {
        if (predicateCache != null)
            predicateCache.stopPrefetch();
        if (classCache != null)
            classCache.stopPrefetch();
    }

    private abstract sealed static class Cache {
        protected final AtomicLong max = new AtomicLong();
        protected volatile boolean stopPrefetch;
        private Thread prefetchThread;

        abstract protected void doPrefetch();
        abstract long get(@Positive long id);

        void startPrefetch() {
            prefetchThread = new Thread(Thread.currentThread().getThreadGroup(), this::doPrefetch);
            prefetchThread.setDaemon(true);
            prefetchThread.setName(this.toString());
            prefetchThread.setPriority(Thread.MIN_PRIORITY);
            prefetchThread.start();
        }

        void stopPrefetch() {
            stopPrefetch = true;
            if (prefetchThread != null) {
                try {
                    prefetchThread.join();
                } catch (InterruptedException e) {
                    log.info("{}.stopPrefetch() interrupted", this);
                    Thread.currentThread().interrupt();
                }
            }
        }

        /** Get an estimate in the {@code [0,1]} range, normalized to the maximum observed get(). */
        double normalized(@Positive long id) {
            long estimate = get(id);
            if (estimate == -1)
                return 1.0; // peek() returned UNKNOWN, do not weigh
            else if (estimate < 0)
                estimate = -1 * (estimate + 1);
            long max = this.max.get();
            return estimate >= max ? 1 : estimate/(double)max;
        }

        protected void offerMax(Long current) {
            for (long old = max.get(); current > old && !max.compareAndSet(old, current); )
                old = max.get();
        }
    }

    private final class PredicateCache extends Cache {
        private final long @NonNull[] cache;

        public PredicateCache(int size) {
            cache = new long[size];
        }

        @Override protected void doPrefetch() {
            long start = System.nanoTime();
            int done = 0;
            try {
                for (int i = 1; i <= cache.length && !stopPrefetch; i++, done++)
                    get(i);
                double ms = NANOSECONDS.toMicros(System.nanoTime()-start)/1000.0;
                log.info("Cached cardinality of {} ?s <p> ?o patterns in {}ms, max={}",
                         done, ms, max.get());
            } catch (Exception e) {
                double ms = NANOSECONDS.toMicros(System.nanoTime()-start)/1000.0;
                log.error("{} when caching cardinality of ?s <p> ?o triples. " +
                          "Completed {} in {}ms", e.getClass().getSimpleName(), done, ms);
            }
        }

        @Override long get(@Positive long id) {
            if (id > cache.length) {
                log.error("{}.get({}): id above MAX_PREDICATES_CACHED={}, will not cache",
                          this, id, MAX_PREDICATES_CACHED);
                return peek(0, id, 0);
            }
            int idx = (int) id - 1;
            long estimate = cache[idx];
            if (estimate != 0)
                return estimate;
            offerMax(estimate = cache[idx] = peek(0, id, 0));
            return estimate;
        }

        @Override public String toString() {
            return PeekCardinalityEstimator.this+".PredicateCache";
        }
    }

    private final class ClassCache extends Cache {
        private final Map<Long, Long> map = new ConcurrentHashMap<>();
        private boolean loggedOverflow = false;

        @Override protected void doPrefetch() {
            long start = System.nanoTime();
            try {
                var it = hdt.getTriples().search(new TripleID(0, typeId, 0));
                while (it.hasNext() && !stopPrefetch)
                    get(it.next().getObject());
                double ms = NANOSECONDS.toMicros(System.nanoTime()-start)/1000.0;
                log.info("Cached cardinality of {} ?s a <class> patterns in {}ms, max={}",
                         map.size(), ms, max.get());
            } catch (Exception e) {
                double ms = NANOSECONDS.toMicros(System.nanoTime()-start)/1000.0;
                log.error("{} occurred when caching cardinality of ?s a <class> triples, " +
                          "completed {} triples after {}ms.",
                          e.getClass().getSimpleName(), map.size(), ms);
            }
        }

        @Override long get(@Positive long id) {
            Long current = map.getOrDefault(id, null);
            if (current == null) {
                current = peek(0, typeId, id);
                if (map.size() < MAX_CLASSES_CACHED) {
                    map.put(id, current);
                } else  if (!loggedOverflow) {
                    loggedOverflow = true;
                    log.error("{}.get({}): cache reached MAX_CLASSES_CACHED={}, will not cache. " +
                              "This message will not be repeated .",
                              this, id, MAX_CLASSES_CACHED);
                }
                offerMax(current);
            }
            return current;
        }

        @Override public String toString() {
            return PeekCardinalityEstimator.this+".ClassCache";
        }
    }
}
