# Manta Archiver

Manta Archiver is a simple CLI tool that allows you to back up all files from
a local directory to a remote Manta path. All files will be stored in Manta
in a compressed state with optional encryption provided by the Manta Java SDK.

## Usage

Manta Archiver is configured in the same fashion as [java-manta](https://github.com/joyent/java-manta) but provides a way
to interactively generate a configuration file so you don't need to learn about java-manta in order to use this project.
See the following wiki articles for setup instructions:

- [Quick, no encryption](https://github.com/joyent/manta-archiver/wiki/Quick-setup,-no-encryption)
- [Quick, with encryption](https://github.com/joyent/manta-archiver/wiki/Quick-setup,-with-encryption)
- [Advanced](https://github.com/joyent/manta-archiver/wiki/Advanced-setup)

### Commands

#### Universal options relevant to all commands
`--log-destination`: `STDOUT`, `STDERR` (default), `FILE`
`--log-level`: `TRACE`, `DEBUG`, `INFO`, `WARN` (default), `ERROR`

#### generate-env
> Arguments: [ bits ]
>
> **bits**: Optional key strength in bits. May be omitted or one of the following: `128`, `192`, `256`.

Interactively builds a configuration file from a [template](/src/main/java/resources/env.sh). If `bits` is provided the generated configuration will have encryption enabled. See the footnote about [encryption strength requirements](#encryption) if selecting a value greater than 128.

#### connect-test

This command validates that the utility can connect properly to Manta with the
current configuration. Additionally, it outputs the final configuration.

##### Example output:

```
$ java -jar target/manta-archiver-1.0.0-SNAPSHOT.jar connect-test
  Creating new connection object
    com.joyent.manta.client.MantaClient@6e4784bc
  Connection configuration
    BaseChainedConfigContext{mantaURL='https://us-east.manta.joyent.com', user='user', mantaKeyId='00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00', mantaKeyPath='/home/user/.ssh/id_rsa', timeout=20000, retries=3, maxConnections=24, httpBufferSize='4096', httpsProtocols='TLSv1.2', httpsCiphers='TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_128_GCM_SHA256,TLS_RSA_WITH_AES_256_CBC_SHA256,TLS_RSA_WITH_AES_128_CBC_SHA256', noAuth=false, disableNativeSignatures=false, tcpSocketTimeout=10000, connectionRequestTimeout=15000, verifyUploads=true, uploadBufferSize=16384, skipDirectoryDepth=null, clientEncryptionEnabled=false, permitUnencryptedDownloads=false, encryptionAuthenticationMode=Mandatory, encryptionKeyId=null, encryptionAlgorithm=AES/CTR/NoPadding, encryptionPrivateKeyPath=null, encryptionPrivateKeyBytesLength=null object}
  Attempting HEAD request to: /user
    com.joyent.manta.client.MantaObjectResponse{path='/user', contentLength=null, contentType='application/x-json-stream; type=directory', etag='null', mtime='null', type='directory', requestId='2949dbbc-0169-11e8-bc16-57a48844193f', httpHeaders=MantaHttpHeaders{wrappedHeaders={connection=keep-alive, x-response-time=18, x-server-name=a28d054b-d8e0-40ab-befc-e5728ef85650, x-request-id=2949dbbc-0169-11e8-bc16-57a48844193f, content-type=application/x-json-stream; type=directory, x-load-balancer=165.225.164.14, server=Manta, date=Thu, 25 Jan 2018 00:46:21 GMT, result-set-size=4}}, directory=true}
  Request was successful
```

#### generate-key
> Arguments: `<encryption-strength-bits> <path>`
>  
> **bits**: `128`, `192`, `256` (only supported values)  
> **path**: path on local filesystem to save key

This command generates an a new encryption key using the specified parameters
and saves it to a local path. This is your secret key and it must be proctected.

#### validate-key
> Arguments: `<path>`
>
> **path**: path to key on local filesystem to check

This command validates an existing encryption key to determine if it is a valid
key.

#### upload
> Arguments: `<local-directory> <manta-directory>`  
>  
> **local-directory**: the directory path on the local file system to send to Manta  
> **manta-directory**: the remote directory path on Manta to upload data to

This command uploads all of the files and directories under the specified
directory to Manta to Manta.

#### download
> Arguments: `<local-directory> <manta-directory>`  
>  
> **local-directory**: the directory path on the local file system to copy files to from Manta  
> **manta-directory**: the remote directory path on Manta to download data from

This command downloads all of the directories and files from Manta the specified
remote Manta path.

#### verify-local
> Arguments: `[--fix] <local-directory> <manta-directory>`  
>  
> **--fix**: optional flag that indicates we upload any missing or different files to Manta  
> **local-directory**: the directory path on the local file system to copy files to from Manta  
> **manta-directory**: the remote directory path on Manta to download data from

This command verifies that the contents of a local directory (files and
directories) match the contents of a remote Manta path. If the `--fix` flag is
present, then this command will upload any missing files/directories or files
that do not match the remote file. Additionally, this command compares the local
file system with the files stored on Manta.

#### verify-remote
> Arguments: `<manta-directory>`

This command verifies that the contents of a remote directory match the checksum
stored in Manta's metadata. It does this by downloading each file and performing
a checksum on the contents and comparing it to the checksum in the metadata.

### Encryption
Using stronger encryption modes (192 and 256-bit) with the Oracle and Azul JVMs requires installation of the
[Java Cryptography Extensions](http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html) for Oracle JVMs and the [Zulu Cryptography Extension Kit](https://www.azul.com/products/zulu-and-zulu-enterprise/zulu-cryptography-extension-kit/) for Azul JVMs. This does not apply as of Java 8 update 161, which includes JCE by default for both [Oracle](http://www.oracle.com/technetwork/java/javase/8u161-relnotes-4021379.html#JDK-8170157) and [Azul](https://support.azul.com/hc/en-us/articles/115001122623-Java-Cryptography-Extension-JCE-for-Zing). OpenJDK distributions do not need any modifications to support stronger encryption modes.
