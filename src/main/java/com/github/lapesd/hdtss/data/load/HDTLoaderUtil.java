package com.github.lapesd.hdtss.data.load;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.rdfhdt.hdt.hdt.HDT;

import javax.validation.constraints.NotEmpty;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

@Singleton
public class HDTLoaderUtil {
    private static final Pattern URI_RX = Pattern.compile("(?i)^[a-z][-+.a-z0-9]*:.*$");
    private static final Pattern FILE_URI_PARSER = Pattern.compile("^file:(/*)(.*)$");

    private final @NonNull List<HDTLoader> loaders;
    private final @NonNull HdtConfig cfg;

    @Inject public HDTLoaderUtil(@NonNull List<HDTLoader> loaders, @NonNull HdtConfig cfg) {
        this.loaders = loaders;
        this.cfg = cfg;
    }

    private static record ScoredLoader(@NonNull HDTLoader loader, int score)
            implements Comparable<ScoredLoader> {
        @Override public int compareTo(@NonNull ScoredLoader o) {
            return Integer.compare(score, o.score);
        }
    }

    static boolean isFilePath(@NonNull String location) {
        return location.startsWith("file:") || !URI_RX.matcher(location).matches();
    }

    static @NonNull String toFilePath(@NonNull String location) {
        var m = FILE_URI_PARSER.matcher(location);
        if (m.matches()) {
            assert m.group(1) != null;
            assert m.group(2) != null;
            return m.group(1).length() != 2 ? "/"+m.group(2) : m.group(2);
        }
        return location;
    }

    /**
     * Select the most adequate loader that can load the HDT file at given location.
     *
     * The selected {@link HDTLoader} implementation will be one such that
     * {@link HDTLoader#canLoad(String)} is {@code true} for {@code location} and that most
     * closely satisfies the following configuration properties:
     *
     * <table>
     *     <thead>
     *         <th>Config property</th>
     *         <th>Default value</th>
     *         <th>HDTLoader method</th>
     *         <th>Weight</th>
     *     </thead>
     *     <tr>
     *         <td>hdt.load.mmap</td> <td>true</td> <td>{@link HDTLoader#isMemoryMapped()}</td> <td>4</td>
     *     </tr>
     *     <tr>
     *         <td>hdt.load.native</td> <td>true</td> <td>{@link HDTLoader#isNative()}</td> <td>2</td>
     *     </tr>
     *     <tr>
     *         <td>hdt.load.can-create-index</td> <td>true</td> <td>{@link HDTLoader#canCreateIndex()}</td> <td>1</td>
     *     </tr>
     * </table>
     *
     * A {@link HDTLoader} scores the points listed under weight in the above table when
     * the result of the {@link HDTLoader} method matches the value of the configuration property.
     * When {@code hdt.load.require-all} is {@code false}, which is the <strong>default</strong>,
     * then the {@link HDTLoader} that sums more points is selected. If {@code hdt.load.require-all}
     * is {@code true} then {@link NoHDTLoaderException} will be thrown if no {@link HDTLoader}
     * satisfies all configuration properties.
     *
     * @param location the HDT file to be loaded using the returned loader
     * @return A non-null {@link HDTLoader} that {@link HDTLoader#canLoad(String)} location.
     * @throws NoHDTLoaderException if there is no {@link HDTLoader} that can load location and
     *         satisfies the configuration requirements.
     */
    public @NonNull HDTLoader loaderFor(@NonNull String location) {
        return loaders.stream()
                .filter(l -> l.canLoad(location))
                .map(l -> new ScoredLoader(l, score(l)))
                .max(Comparator.naturalOrder())
                .orElseThrow(() -> noLoaderEx(location)).loader();
    }

    private int score(@NonNull HDTLoader loader) {
        HdtConfig.LoadOptions req = cfg.getLoadOptions();
        boolean wantsCanIndex = req.isCanCreateIndex();
        int weight = (req.isMmap() == loader.isMemoryMapped() ? 4 : 0)
                  + (req.isNative() == loader.isNative() ? 2 : 0)
                  + (!wantsCanIndex || loader.canCreateIndex() ? 1 : 0);
        return req.isRequireAll() && (weight != 5 || wantsCanIndex != loader.canCreateIndex())
                ? -1 /* discard loader  */
                : weight;
    }

    private @NonNull NoHDTLoaderException noLoaderEx(@NonNull String location) {
        HdtConfig.LoadOptions req = cfg.getLoadOptions();
        return new NoHDTLoaderException(String.format("No HDTLoader that canLoad(%s) " +
                "and satisfies the following characteristics: native=%b, mmap=%b, " +
                "can-create-index=%b. Hint: require-all is set to %b.", location,
                req.isNative(), req.isMmap(), req.isCanCreateIndex(), req.isRequireAll()));
    }

    /**
     * Call {@link HDTLoader#load(String, boolean)} on the result of
     * {@link HDTLoaderUtil#loaderFor(String)}.
     *
     * @param location forwarded th {@link HDTLoader#load(String, boolean)}
     * @param indexIfMissing forwarded th {@link HDTLoader#load(String, boolean)}
     * @return See {@link HDTLoader#load(String, boolean)}
     * @throws IOException See {@link HDTLoader#load(String, boolean)}
     * @throws NoHDTLoaderException see {@link HDTLoaderUtil#loaderFor(String)}
     */
    public @NonNull HDT load(@NonNull @NotEmpty String location,
                             boolean indexIfMissing) throws IOException {
        return loaderFor(location).load(location, indexIfMissing);
    }

    /**
     * Call {@link HDTLoaderUtil#load(String, boolean)} with the value
     * of {@code hdt.load.indexed}.
     *
     * @param location see {@link HDTLoader#load(String, boolean)}
     * @return see {@link HDTLoader#load(String, boolean)}
     * @throws IOException thrown by {@link HDTLoader#load(String, boolean)}
     * @throws NoHDTLoaderException see {@link HDTLoaderUtil#loaderFor(String)}
     */
    public @NonNull HDT load(@NonNull @NotEmpty String location) throws IOException {
        return loaderFor(location).load(location, cfg.getLoadOptions().isIndexed());
    }

    /**
     * Call {@link HDTLoaderUtil#load(String)} with the HDT file set in
     * {@code hdt.location}.
     *
     * @return See {@link HDTLoaderUtil#load(String)}
     * @throws IOException see {@link HDTLoaderUtil#load(String)}
     * @throws NoHDTLoaderException see {@link HDTLoaderUtil#loaderFor(String)}
     */
    public @NonNull HDT load() throws IOException {
        return load(cfg.getLocation());
    }
}
