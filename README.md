# Manta Archiver

Manta Archiver is a simple CLI tool that allows you to back up all files from
a local directory to a remote Manta path. All files will be stored in Manta
in a compressed state with optional encryption provided by the Manta Java SDK.

## Usage

You will need to configure Manta Archiver in the [same way as you configure the
Java Manta SDK](https://github.com/joyent/java-manta/blob/master/USAGE.md#parameters) 
by using environment variables or Java system properties. This will require
a valid secure key configuration.

### Encryption

Encryption is configured in the [same way as the Java Manta SDK](https://github.com/joyent/java-manta/blob/master/USAGE.md#client-side-encryption).

### Commands

#### Universal options relevant to all commands
`--log-destination`: `STDOUT`, `STDERR` (default), `FILE`
`--log-level`: `TRACE`, `DEBUG`, `INFO`, `WARN` (default), `ERROR`

#### connect-test
Options: None

This command validates that the utility can connect properly to Manta with the
current configuration. Additionally, it outputs the final configuration.
For example:

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
Options: `<cipher> <bits> <path>`

cipher: `AES` (only supported value)

bits: `128`, `192`, `256` (only supported values)

path: path on local filesystem to save key

This command generates an a new encryption key using the specified parameters
and saves it to a local path. This is your secret key and it must be proctected.

#### validate-key
Options: `<cipher> <path>`

cipher: `AES` (only supported value)

path: path on local filesystem to load key from

This command validates an existing encryption key to determine if it is a valid
key.

#### upload
Options: `<local-directory> <manta-directory>`

local-directory: the directory path on the local file system to send to Manta

manta-directory: the remote directory path on Manta to upload data to

This command uploads all of the files and directories under the specified 
directory to Manta to Manta.

#### download
Options: `<local-directory> <manta-directory>`

local-directory: the directory path on the local file system to copy files to from Manta

manta-directory: the remote directory path on Manta to download data from

This command downloads all of the directories and files from Manta the specified
remote Manta path.

#### verify-local
Options: `[--fix] <local-directory> <manta-directory>`

--fix: optional flag that indicates we upload any missing or different files to Manta

local-directory: the directory path on the local file system to copy files to from Manta

manta-directory: the remote directory path on Manta to download data from

This command verifies that the contents of a local directory (files and 
directories) match the contents of a remote Manta path. If the `--fix` flag is
present, then this command will upload any missing files/directories or files
that do not match the remote file. Additionally, this command compares the local
file system with the files stored on Manta.

#### verify-remote
Options: `<manta-directory>`

This command verifies that the contents of a remote directory match the checksum
stored in Manta's metadata. It does this by downloading each file and performing
a checksum on the contents and comparing it to the checksum in the metadata. 