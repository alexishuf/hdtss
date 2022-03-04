package com.github.lapesd.hdtss;

import io.micronaut.runtime.Micronaut;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class Application {
    private static final Pattern OPTION_RX = Pattern.compile("^--?");
    private static final Pattern HDT_LOCATION_RX = Pattern.compile("^--?hdt.location");
    private static final Pattern SERVER_CONFIG_RX = Pattern.compile(
            "^(--?)(http-version|thread-selection|default-charset|port|host|read-timeout" +
                    "|server-header|max-request-size|read-idle-timeout|write-idle-timeout" +
                    "|idle-timeout|date-header|log-handled-exeptions|client-address-header" +
                    "|context-path|dual-protocol|http-to-https-redirect|netty\\..*)(=|$)");
    private static final Pattern VERBOSITY_RX = Pattern.compile("^(--?)(v)(=|$)");

    public static void main(@NonNull String @NonNull[] args) {
        args = promoteOrphansToHdtLocation(args);
        scopeServerProperties(args);
        scopeVerbosity(args);
        Micronaut.run(Application.class, args);
    }

    private static void scopeVerbosity(@NonNull String @NonNull[] args) {
        for (int i = 0; i < args.length; i++) {
            Matcher matcher = VERBOSITY_RX.matcher(args[i]);
            if (matcher.find())
                args[i] = matcher.replaceAll("$1logger.levels.com.github.lapesd.hdtss$3");
        }
    }

    private static void
    scopeServerProperties(@NonNull String @NonNull[] args) {
        for (int i = 0; i < args.length; i++) {
            Matcher matcher = SERVER_CONFIG_RX.matcher(args[i]);
            if (matcher.find())
                args[i] = matcher.replaceAll("$1micronaut.server.$2$3");
        }
    }

    private static @NonNull String @NonNull []
    promoteOrphansToHdtLocation(@NonNull String @NonNull [] args) {
        List<String> orphans = new ArrayList<>();
        boolean expectValue = false;
        boolean hadHdtLocation = false;
        for (String arg : args) {
            if (OPTION_RX.matcher(arg).find()) {
                expectValue = arg.indexOf('=') < 0;
                hadHdtLocation = HDT_LOCATION_RX.matcher(arg).find();
            } else if (!expectValue) {
                orphans.add(arg);
            } else {
                expectValue = false;
            }
        }
        if (orphans.size() > 1 || (orphans.size() == 1 && hadHdtLocation)) {
            System.err.println("Multiple HDT files are not supported (yet)!\n");
            System.exit(1);
        } else if (orphans.size() == 1) {
            log.debug("Taking orphan argument {} as -hdt.location", orphans.get(0));
            List<String> list = new ArrayList<>(Arrays.asList(args));
            list.add("-hdt.location="+orphans.get(0));
            args = list.toArray(String[]::new);
        }
        return args;
    }
}
