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
import java.util.Scanner;
import java.util.function.Predicate;

/**
 * Helper class for interactively generating an env.sh file.
 */
@SuppressWarnings({"checkstyle:JavaDocVariable", "checkstyle:JavaDocMethod", "checkstyle:JavaDocType"})
final class EnvGeneratorPrompt {

    private static final Prompt[] PROMPTS_GENERAL = new Prompt[]{
            new Prompt(
                    "__MANTA_USER__",
                    "Manta username (account or account/subuser): ",
                    (u) -> !u.isEmpty()),
            new Prompt(
                    "__MANTA_KEY_PATH__",
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
                    "__MANTA_ENCRYPTION_KEY_PATH__",
                    "Path to secret key? (if empty, one will be generated)",
                    (k) -> {
                        final Path path = Paths.get(k);

                        return !Files.isReadable(path) || !Files.isRegularFile(path);
                    }),
            new Prompt(
                    "__MANTA_CLIENT_ENCRYPTION_KEY_ID__",
                    "Unique identifier for secret key (non-whitespace ASCII only): ",
                    (id) -> StringUtils.isAsciiPrintable(id) && !StringUtils.containsWhitespace(id)),
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

    void collect() {
        for (final Prompt prompt : this.prompts) {
            do {
                prompt.display();
            } while (!prompt.accept(this.input.next()));
        }
    }

    String render() {
        String result = template;
        for (final Prompt prompt : this.prompts) {
            result = result.replace(prompt.templateVar, prompt.answer);
        }

        return result;
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

        String apply(final String template) {
            if (this.answer == null) {
                return template.replace(this.templateVar, "");
            }

            return template.replace(this.templateVar, this.answer);
        }

        static Prompt withEmptyAnswer(final Prompt prompt) {
            return new Prompt(prompt.templateVar, prompt.question, prompt.validator);
        }
    }
}
