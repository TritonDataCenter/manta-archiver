/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import org.apache.commons.lang3.StringUtils;

import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.function.Predicate;

import static com.joyent.manta.config.EnvVarConfigContext.MANTA_ACCOUNT_ENV_KEY;
import static com.joyent.manta.config.EnvVarConfigContext.MANTA_ENCRYPTION_KEY_ID_ENV_KEY;
import static com.joyent.manta.config.EnvVarConfigContext.MANTA_ENCRYPTION_PRIVATE_KEY_PATH_ENV_KEY;
import static com.joyent.manta.config.EnvVarConfigContext.MANTA_KEY_ID_ENV_KEY;
import static com.joyent.manta.config.EnvVarConfigContext.MANTA_KEY_PATH_ENV_KEY;

/**
 * Helper class for interactively generating an env.sh file.
 */
@SuppressWarnings({"checkstyle:JavaDocVariable", "checkstyle:JavaDocMethod", "checkstyle:JavaDocType"})
final class EnvGeneratorPrompt {

    public static final String VAR_MANTA_USER = templatizeEnvVar(MANTA_ACCOUNT_ENV_KEY);
    public static final String VAR_MANTA_KEY_PATH = templatizeEnvVar(MANTA_KEY_PATH_ENV_KEY);
    public static final String VAR_MANTA_KEY_ID = templatizeEnvVar(MANTA_KEY_ID_ENV_KEY);
    public static final String VAR_MANTA_ENCRYPTION_KEY_PATH = templatizeEnvVar(MANTA_ENCRYPTION_PRIVATE_KEY_PATH_ENV_KEY);
    public static final String VAR_MANTA_CLIENT_ENCRYPTION_KEY_ID = templatizeEnvVar(MANTA_ENCRYPTION_KEY_ID_ENV_KEY);

    private static final Prompt[] PROMPTS_GENERAL = new Prompt[]{
            new Prompt(
                    VAR_MANTA_USER,
                    "Manta username (account or account/subuser): ",
                    (u) -> !u.isEmpty()),
            new Prompt(
                    VAR_MANTA_KEY_PATH,
                    "Path to private key: ",
                    (k) -> {
                        final Path path = Paths.get(k);

                        if (!Files.isReadable(path) || !Files.isRegularFile(path)) {
                            return false;
                        }

                        final String firstLine;
                        try (Scanner keyReader = new Scanner(path.toFile())) {
                            firstLine = keyReader.nextLine();
                        } catch (FileNotFoundException e) {
                            throw new UncheckedIOException(e); // not likely since we just checked
                        }

                        return firstLine.contains("-----BEGIN") && firstLine.contains("PRIVATE KEY-----");
                    }),
    };

    private static final Prompt[] PROMPTS_ENCRYPTION = new Prompt[]{
            new Prompt(
                    VAR_MANTA_ENCRYPTION_KEY_PATH,
                    "Path to secret key (from generate-key): ",
                    (k) -> {
                        final Path path = Paths.get(k);

                        return Files.isReadable(path) && Files.isRegularFile(path);
                    }),
            new Prompt(
                    VAR_MANTA_CLIENT_ENCRYPTION_KEY_ID,
                    "Unique identifier for secret key (non-whitespace ASCII only): ",
                    (id) -> {
                        final boolean asciiPrintable = StringUtils.isAsciiPrintable(id);
                        final boolean containswhite = StringUtils.containsWhitespace(id);
                        return asciiPrintable && !containswhite;
                    }),
    };

    private final Scanner input;

    private final String template;

    private final ArrayList<Prompt> prompts;

    /**
     *
     * @param input input reader
     * @param template template string
     * @param encryptionDesired whether we should ask about encryption
     */
    EnvGeneratorPrompt(final Scanner input, final String template, final boolean encryptionDesired) {
        this.input = input;
        this.template = template;
        this.prompts = new ArrayList<>();

        this.prompts.addAll(Arrays.asList(PROMPTS_GENERAL));

        if (encryptionDesired) {
            this.prompts.addAll(Arrays.asList(PROMPTS_ENCRYPTION));
        } else {
            for (final Prompt encryptionPrompt : PROMPTS_ENCRYPTION) {
                this.prompts.add(Prompt.withEmptyAnswer(encryptionPrompt));
            }
        }
    }

    List<Prompt> getPrompts() {
        return this.prompts;
    }

    void collect() {
        for (final Prompt prompt : this.prompts) {
            if (prompt.answer != null) {
                continue;
            }

            do {
                prompt.display();
            } while (!prompt.accept(this.input.nextLine()));
        }
    }

    String render() {
        String result = template;
        for (final Prompt prompt : this.prompts) {
            result = prompt.interpolateAnswer(result);
        }

        return result;
    }

    private static String templatizeEnvVar(final String envKey) {
        return String.format("__%s__", envKey);
    }

    static final class Prompt {

        private final String templateVar;

        private final String question;

        private final Predicate<String> validator;

        private String answer;

        Prompt(final String templateVar, final String question, final Predicate<String> validator) {
            this.templateVar = templateVar;
            this.question = question;
            this.validator = validator;
        }

        public String getTemplateVar() {
            return this.templateVar;
        }

        public String getAnswer() {
            return this.answer;
        }

        void display() {
            System.out.print(question);
        }

        boolean accept(final String response) {
            if (this.validator == null) {
                this.answer = response;
                return true;
            }

            final boolean valid = this.validator.test(response);
            if (valid) {
                this.answer = response;
            }

            return valid;
        }

        String interpolateAnswer(final String template) {
            return template.replace(this.templateVar, this.answer);
        }

        static Prompt withEmptyAnswer(final Prompt prompt) {
            final Prompt answered = new Prompt(prompt.templateVar, prompt.question, prompt.validator);
            answered.answer = "";
            return answered;
        }
    }
}
