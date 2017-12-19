# Manta Archiver

Manta Archiver is a simple CLI tool that allows you to back up all files from
a local directory to a remote Manta path. All files will be stored in Manta
in a compressed state with optional encryption provided by the Manta Java SDK.

## Usage

You will need to configure Manta Archiver in the [same way as you configure the
Java Manta SDK](https://github.com/joyent/java-manta/blob/master/USAGE.md#parameters) 
by using environment variables or Java system properties. This will require
a valid secure key configuration.


