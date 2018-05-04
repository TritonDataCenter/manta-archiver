/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.FileAppender;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.util.MantaVersion;
import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static com.joyent.manta.archiver.MantaArchiverCLI.MantaSubCommand.CommandLogLevel.DEBUG;
import static com.joyent.manta.archiver.MantaArchiverCLI.MantaSubCommand.CommandLogLevel.INFO;

@CommandLine.Command(name = "java-manta-cli", sortOptions = false,
        header = {
                "@|cyan                 .     .             |@",
                "@|cyan                 |_.-._|             |@",
                "@|cyan               ./       \\.          |@",
                "@|cyan          _.-'`           `'-._      |@",
                "@|cyan       .-'        Java         '-.   |@",
                "@|cyan     ,'_.._       Manta       _.._', |@",
                "@|cyan     '`    `'-.           .-'`    `' |@",
                "@|cyan               '.       .'           |@",
                "@|cyan                 \\_/|\\_/           |@",
                "@|cyan                    |                |@",
                "@|cyan                    |                |@",
                "@|cyan                    |                |@",
                ""},
        description = {
                "",
                "Manta Archiver - Bulk upload and download utility", },
        optionListHeading = "@|bold %nOptions|@:%n",
        subcommands = {
                MantaArchiverCLI.ConnectTest.class,
                MantaArchiverCLI.GenerateKey.class,
                MantaArchiverCLI.ValidateKey.class,
                MantaArchiverCLI.Upload.class,
                MantaArchiverCLI.Download.class,
                MantaArchiverCLI.VerifyLocal.class,
                MantaArchiverCLI.VerifyRemote.class
        })
// Documented through CLI annotations
@SuppressWarnings({"checkstyle:javadocmethod", "checkstyle:javadoctype", "checkstyle:javadocvariable"})
public class MantaArchiverCLI {
    private static final MantaClientSupplier MANTA_CLIENT_SUPPLIER =
            new MantaClientSupplier();

    static {
        // We reference the following classes so they get picked up in the uber jar
        @SuppressWarnings("unused")
        Class<?>[]  clasess = new Class<?>[] {
                com.fasterxml.jackson.databind.ext.Java7Support.class,
                com.fasterxml.jackson.databind.ext.Java7SupportImpl.class
        };
    }

    @CommandLine.Option(names = {"-v", "--version"}, help = true)
    private boolean isVersionRequested;

    @CommandLine.Option(names = {"-h", "--help"}, help = true)
    private boolean isHelpRequested;

    public static void main(final String[] args) {
        final CommandLine application = new CommandLine(new MantaArchiverCLI());
        application.registerConverter(Path.class, Paths::get);

        List<CommandLine> parsedCommands = null;
        try {
            parsedCommands = application.parse(args);
        } catch (CommandLine.ParameterException ex) {
            System.err.println(ex.getMessage());
            CommandLine.usage(new MantaArchiverCLI(), System.err);
            return;
        }
        MantaArchiverCLI cli = (MantaArchiverCLI) parsedCommands.get(0).getCommand();
        if (cli.isHelpRequested) {
            application.usage(System.out);
            return;
        }
        if (cli.isVersionRequested) {
            System.out.println("java-manta-client: " +  MantaVersion.VERSION);
            return;
        }
        if (parsedCommands.size() == 1) {
            // no subcmd given
            application.usage(System.err);
            return;
        }
        CommandLine deepest = parsedCommands.get(parsedCommands.size() - 1);

        MantaSubCommand subcommand = (MantaSubCommand) deepest.getCommand();
        if (subcommand.isHelpRequested) {
            CommandLine.usage(deepest.getCommand(), System.err);
            return;
        }

        configureLogger(subcommand);

        subcommand.run();
    }

    @SuppressWarnings("fallthrough")
    private static void configureLogger(final MantaSubCommand subcommand) {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

        if (subcommand.logLevel != null) {
            root.setLevel(Level.valueOf(subcommand.logLevel.toString()));
        } else {
            root.setLevel(Level.WARN);
        }

        // Wire log level is way too much information for even debug mode
        Logger wire = (Logger) LoggerFactory.getLogger("org.apache.http.wire");

        if (subcommand.logLevel != null
                && (subcommand.logLevel.equals(DEBUG) || subcommand.logLevel.equals(INFO))) {
            wire.setLevel(Level.INFO);
        } else {
            wire.setLevel(Level.WARN);
        }

        @SuppressWarnings("unchecked")
        LoggerContext lc = (LoggerContext)LoggerFactory.getILoggerFactory();

        PatternLayoutEncoder patternLayoutEncoder = new PatternLayoutEncoder();
        patternLayoutEncoder.setContext(lc);
        patternLayoutEncoder.setPattern("[%thread] %-5level %logger - %msg%n");
        patternLayoutEncoder.start();

        String cAppenderDefaultTarget = "System.err";
        Appender<ILoggingEvent> appender;

        final MantaSubCommand.LogDestination destination;

        if (subcommand.logDestination == null) {
            destination = MantaSubCommand.LogDestination.STDERR;
        } else {
            destination = subcommand.logDestination;
        }

        switch (destination) {
            case FILE:
                FileAppender<ILoggingEvent> fappender = new FileAppender<>();
                appender = fappender;
                fappender.setEncoder(patternLayoutEncoder);
                fappender.setFile("manta-archiver.log");
                fappender.setImmediateFlush(true);
                fappender.setAppend(true);
                break;
            case STDOUT:
                cAppenderDefaultTarget = "System.out";
                // fall through
            case STDERR:
                // fall through
            default:
                ConsoleAppender<ILoggingEvent> cappender = new ConsoleAppender<>();
                appender = cappender;
                cappender.setEncoder(patternLayoutEncoder);
                cappender.setTarget(cAppenderDefaultTarget);
                cappender.setImmediateFlush(true);
        }

        appender.setContext(lc);
        appender.start();

        root.detachAndStopAllAppenders();
        root.addAppender(appender);
    }

    @CommandLine.Command(sortOptions = false,
            headerHeading = "@|bold,underline Usage:|@%n%n",
            synopsisHeading = "%n",
            descriptionHeading = "%n@|bold,underline Description:|@%n%n",
            parameterListHeading = "%n@|bold,underline Parameters:|@%n",
            optionListHeading = "%n@|bold,underline Options:|@%n")
    public abstract static class MantaSubCommand {
        public enum CommandLogLevel { TRACE, DEBUG, INFO, WARN, ERROR };
        public enum LogDestination { STDOUT, STDERR, FILE };

        protected static final String BR = System.lineSeparator();

        protected static final String INDENT = "  ";

        @CommandLine.Option(names = {"-h", "--help"}, help = true)
        private boolean isHelpRequested;

        @CommandLine.Option(names = {"--log-level"},
                description = "TRACE, DEBUG, INFO, WARN(default), ERROR")
        private CommandLogLevel logLevel;

        @CommandLine.Option(names = {"--log-destination"},
                description = "STDOUT, STDERR(default), FILE")
        private LogDestination logDestination;

        public abstract void run();
    }

    @CommandLine.Command(name = "connect-test",
            header = "Try to connect",
            description = "Attempts to connect to Manta using system properties "
                    + "and environment variables for configuration")
    public static class ConnectTest extends MantaSubCommand {

        @Override
        public void run() {
            final StringBuilder b = new StringBuilder();

            b.append("Creating new connection object").append(BR);
            try (MantaClient client = MANTA_CLIENT_SUPPLIER.get()) {
                b.append(INDENT).append(client).append(BR);

                b.append("Connection configuration").append(BR);
                ConfigContext config = client.getContext();
                b.append(INDENT).append(ConfigContext.toString(config)).append(BR);

                String homeDirPath = config.getMantaHomeDirectory();
                b.append("Attempting HEAD request to: ").append(homeDirPath).append(BR);

                MantaObjectResponse response = client.head(homeDirPath);
                b.append(INDENT).append(response).append(BR);
                b.append("Request was successful");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            System.out.println(b.toString());
        }
    }

    @CommandLine.Command(name = "generate-key",
            header = "Generate an encryption key",
            description = "Generates a client-side encryption key with the specified "
                    + "cipher and bits at the specified path.")
    public static class GenerateKey extends MantaSubCommand {

        @CommandLine.Parameters(index = "0", description = "cipher to generate key for")
        private String cipher;
        @CommandLine.Parameters(index = "1", description = "number of bits of the key")
        private int bits;
        @CommandLine.Parameters(index = "2", description = "path to write the key to")
        private Path path;

        @Override
        public void run() {
            StringBuilder b = new StringBuilder();

            try {
                b.append("Generating key").append(BR);
                SecretKey key = SecretKeyUtils.generate(cipher, bits);

                b.append(String.format("Writing [%s-%d] key to [%s]", cipher, bits, path));
                SecretKeyUtils.writeKeyToPath(key, path);
            } catch (NoSuchAlgorithmException e) {
                System.err.printf("The running JVM [%s/%s] doesn't support the "
                                + "supplied cipher name [%s]", System.getProperty("java.version"),
                        System.getProperty("java.vendor"), cipher);
                System.err.println();
                return;
            } catch (IOException e) {
                String msg = String.format("Unable to write key to path [%s]",
                        path);
                throw new UncheckedIOException(msg, e);
            }

            System.out.println(b.toString());
        }
    }

    @CommandLine.Command(name = "validate-key",
            header = "Validate an encryption key",
            description = "Validates that the supplied key is supported by the "
                    + "SDK's client-side encryption functionality.")
    public static class ValidateKey extends MantaSubCommand {
        @CommandLine.Parameters(index = "0", description = "cipher to validate the key against")
        private String cipher;
        @CommandLine.Parameters(index = "1", description = "path to read the key from")
        private Path path;

        @Override
        public void run() {
            StringBuilder b = new StringBuilder();

            try {
                b.append(String.format("Loading key from path [%s]", path)).append(BR);
                SecretKeySpec key = SecretKeyUtils.loadKeyFromPath(path, cipher);

                if (key.getAlgorithm().equals(cipher)) {
                    b.append("Cipher of key is [")
                            .append(cipher)
                            .append("] as expected")
                            .append(BR);
                } else {
                    b.append("Cipher of key is [")
                            .append(key.getAlgorithm())
                            .append("] - it doesn't match the expected cipher of [")
                            .append(cipher)
                            .append("]").append(BR);
                }

                b.append("Key format is [")
                        .append(key.getFormat())
                        .append("]").append(BR);
            } catch (NoSuchAlgorithmException e) {
                System.err.printf("The running JVM [%s/%s] doesn't support the "
                                + "supplied cipher name [%s]", System.getProperty("java.version"),
                        System.getProperty("java.vendor"), cipher);
                System.err.println();
                return;
            } catch (IOException e) {
                String msg = String.format("Unable to read key from path [%s]",
                        path);
                throw new UncheckedIOException(msg, e);
            }

            System.out.println(b.toString());
        }
    }

    public abstract static class ArchiveSubCommand extends MantaSubCommand {
        /**
         * Validates a local directory path.
         *
         * @param localDirectory local directory as input by the user
         * @return Path to local directory
         */
        protected Path findLocalPath(final String localDirectory) {
            final Path localRoot = Paths.get(localDirectory);

            if (!localRoot.toFile().exists()) {
                System.err.println("Local directory path supplied doesn't exist: " + localDirectory);
                System.exit(1);
            }

            if (!localRoot.toFile().isDirectory()) {
                System.err.println("Local directory path is not a directory: " + localDirectory);
                System.exit(1);
            }

            return localRoot;
        }
    }


    @CommandLine.Command(name = "upload",
            header = "Uploads a directory to Manta",
            description = "Uploads a local directory to a remote directory in Manta.")
    public static class Upload extends ArchiveSubCommand {

        @CommandLine.Parameters(paramLabel = "local-directory",
                index = "0", description = "directory to upload files from")
        private String localDirectory;
        @CommandLine.Parameters(paramLabel = "manta-directory",
                index = "1", description = "directory in Manta to upload files to")
        private String mantaDirectory;

        @CommandLine.Option(names = {"-p", "--mkdirp"})
        private boolean mkdirp;

        @Override
        public void run() {
            final Path localRoot = findLocalPath(localDirectory);

            MantaTransferClient mantaTransferClient = new MantaTransferClient(
                    MANTA_CLIENT_SUPPLIER, mantaDirectory, localRoot, mkdirp);

            try (TransferManager manager = new TransferManager(mantaTransferClient,
                    localRoot)) {
                manager.uploadAll();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                System.err.println("Unrecoverable error uploading files to Manta");
                e.printStackTrace(System.err);
            }
        }
    }

    @CommandLine.Command(name = "download",
            header = "Downloads a directory from Manta",
            description = "Downloads the contents of a remote directory in Manta.")
    public static class Download extends ArchiveSubCommand {

        @CommandLine.Parameters(paramLabel = "local-directory",
                index = "0", description = "directory to download files to")
        private String localDirectory;
        @CommandLine.Parameters(paramLabel = "manta-directory",
                index = "1", description = "directory in Manta to download files from")
        private String mantaDirectory;

        @Override
        public void run() {
            final Path localRoot = findLocalPath(localDirectory);

            MantaTransferClient mantaTransferClient = new MantaTransferClient(
                    MANTA_CLIENT_SUPPLIER, mantaDirectory);

            try (TransferManager manager = new TransferManager(mantaTransferClient,
                    localRoot)) {
                manager.downloadAll();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                System.err.println("Unrecoverable error downloading files from Manta");
                e.printStackTrace(System.err);
            }
        }
    }

    @CommandLine.Command(name = "verify-local",
            header = "Verifies the contents of a local directory",
            description = "Verifies a local directory to a remote directory in Manta.")
    public static class VerifyLocal extends ArchiveSubCommand {
        @CommandLine.Parameters(paramLabel = "local-directory",
                index = "0", description = "directory to verify files from")
        private String localDirectory;
        @CommandLine.Parameters(paramLabel = "manta-directory",
                index = "1", description = "directory in Manta to verify to")
        private String mantaDirectory;
        @CommandLine.Option(names = {"-f", "--fix"}, help = true,
                type = Boolean.class, description = "reupload objects that don't match remote")
        private Boolean fix;

        @Override
        public void run() {
            final Path localRoot = findLocalPath(localDirectory);

            MantaTransferClient mantaTransferClient = new MantaTransferClient(
                    MANTA_CLIENT_SUPPLIER, mantaDirectory);

            boolean verificationSuccess = false;

            try (TransferManager manager = new TransferManager(mantaTransferClient,
                    localRoot)) {
                verificationSuccess = manager.verifyLocal(BooleanUtils.isTrue(fix));
            } catch (RuntimeException e) {
                System.err.println("Unrecoverable error verifying files on Manta");
                e.printStackTrace(System.err);
                System.exit(1);
            }

            if (verificationSuccess) {
                System.exit(0);
            } else {
                System.exit(1);
            }
        }
    }

    @CommandLine.Command(name = "verify-remote",
            header = "Verifies the contents of a remote directory",
            description = "Verifies that all of the objects in a remote directory in Manta have the right checksums.")
    public static class VerifyRemote extends ArchiveSubCommand {
        @CommandLine.Parameters(paramLabel = "manta-directory",
                index = "0", description = "directory in Manta to verify to")
        private String mantaDirectory;

        @Override
        public void run() {
            MantaTransferClient mantaTransferClient = new MantaTransferClient(
                    MANTA_CLIENT_SUPPLIER, mantaDirectory);

            boolean verificationSuccess = false;

            try (TransferManager manager = new TransferManager(mantaTransferClient,
                    null)) {
                verificationSuccess = manager.verifyRemote();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                System.err.println("Unrecoverable error verifying files on Manta");
                e.printStackTrace(System.err);
                System.exit(1);
            }

            if (verificationSuccess) {
                System.exit(0);
            } else {
                System.exit(1);
            }
        }
    }
}
