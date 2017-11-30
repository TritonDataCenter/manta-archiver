/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.archiver;

import picocli.CommandLine;

import java.nio.file.Path;
import java.nio.file.Paths;

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
                "Basic manta cli commands using the java-client. Not a stable interface.", },
        optionListHeading = "@|bold %nOptions|@:%n",
        subcommands = {
        })
// Documented through CLI annotations
@SuppressWarnings({"checkstyle:javadocmethod", "checkstyle:javadoctype", "checkstyle:javadocvariable"})
public class MantaArchiverCLI {
    @CommandLine.Option(names = {"-v", "--version"}, help = true)
    private boolean isVersionRequested;

    @CommandLine.Option(names = {"-h", "--help"}, help = true)
    private boolean isHelpRequested;

    public static void main(final String[] argv) {
        final CommandLine application = new CommandLine(new MantaArchiverCLI());
        application.registerConverter(Path.class, Paths::get);
    }

}
