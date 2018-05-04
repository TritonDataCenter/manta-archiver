#!/bin/bash

export MANTA_URL=https://us-east.manta.joyent.com:443

### Credentials

# subusers are specified as "account/subuser"
export MANTA_USER=__MANTA_USER__

# Absolute path to an SSH private key registered with $MANTA_USER
export MANTA_KEY_PATH=__MANTA_KEY_PATH__
# MD5 or SHA256 fingerprint of the key specified by MANTA_KEY_PATH
export MANTA_KEY_ID=__MANTA_KEY_ID__

# TODO: consider computing the key ID ourselves?
# Save the user a step by computing key ID automatically
# export MANTA_KEY_ID=$(ssh-keygen -l -f ${MANTA_KEY_PATH} -E md5 | cut -d' ' -f 2)

### Encryption settings

export MANTA_CLIENT_ENCRYPTION=__MANTA_CLIENT_ENCRYPTION__
export MANTA_ENCRYPTION_ALGORITHM=__MANTA_ENCRYPTION_ALGORITHM__

# Path to secret key. See the `generate-key` command.
export MANTA_ENCRYPTION_KEY_PATH=__MANTA_ENCRYPTION_KEY_PATH__
# Unique identifier for the key referenced by MANTA_ENCRYPTION_KEY_PATH. Must be ASCII-only and not contain whitespace.
export MANTA_CLIENT_ENCRYPTION_KEY_ID=__MANTA_CLIENT_ENCRYPTION_KEY_ID__

### General client settings

export MANTA_TIMEOUT=1200000
export MANTA_MAX_CONNS=128
export MANTA_HTTP_RETRIES=16
export MANTA_VERIFY_UPLOADS=true

### Validate Configuration
echo

if [[ -z "$MANTA_USER" ]]; then
    echo "\nError: please verify that MANTA_USER is not empty"
fi

if [[ -z "$MANTA_KEY_PATH" || ! -f "$MANTA_KEY_PATH" || ! -s "$MANTA_KEY_PATH" ]]; then
    echo "\nError: please verify that MANTA_KEY_PATH exists, is readable and not empty"
elif [[ ! grep 'PRIVATE KEY-----' "$MANTA_KEY_PATH" ]]; then
    echo "\nError: please verify that MANTA_KEY_PATH is a valid OpenSSH-formatted private key"
fi

if [[ -z "$MANTA_KEY_PATH" || ! -f "$MANTA_KEY_PATH" || ! -s "$MANTA_KEY_PATH" ]]; then
    echo "\nError: please verify that MANTA_ENCRYPTION_KEY_PATH exists, is readable and not empty"
fi

if [[ -z "$MANTA_KEY_PATH" ]]; then
    echo "\nError: please verify that MANTA_KEY_PATH is not empty"
fi

if [[ "$MANTA_CLIENT_ENCRYPTION" == "true" ]]; then

    # check that the supplied encryption key:
    #  - exists and is a regular file
    #  - is readable
    #  - is not empty
    if [[ ! -f  "$MANTA_ENCRYPTION_KEY_PATH" || ! -r "$MANTA_ENCRYPTION_KEY_PATH" || ! -s "$MANTA_ENCRYPTION_KEY_PATH" ]]; then
        echo "\nError: please verify that MANTA_ENCRYPTION_KEY_PATH exists, is readable and not empty"
    fi

    if [[ -z "$MANTA_CLIENT_ENCRYPTION_KEY_ID" ]]; then
        echo "\nError: please verify that MANTA_CLIENT_ENCRYPTION_KEY_ID is not empty"
    fi

    if [[ -z "$MANTA_ENCRYPTION_ALGORITHM" ]]; then
        echo "\nError: please verify that MANTA_ENCRYPTION_ALGORITHM is not empty"
    elif [[ "$MANTA_ENCRYPTION_ALGORITHM" != *"128"*  ]]; then

        if command -v jrunscript > /dev/null; then
            # Check that JCE or an unrestricted JVM are installed
            if [[ ! $(jrunscript -e 'java.lang.System.out.println(javax.crypto.Cipher.getMaxAllowedKeyLength("AES"));') -gt 128 ]]; then
                echo "\nError: please verify that MANTA_CLIENT_ENCRYPTION_KEY_ID is not empty"
            fi
         else
            echo "\nWarning: encryption algorithm may require Java Cryptography Extensions"
            echo "but jrunscript is not available to validate that the requirement is met"
         fi
    fi
fi
