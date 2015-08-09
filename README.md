# YAKVS [![Build Status](https://travis-ci.org/sci4me/yakvs.svg?branch=master)](https://travis-ci.org/sci4me/yakvs)

YAKVS (Yet Another Key Value Store) is a tiny, lightweight, networked, in-memory key-value store written in Go.

## Install

    go get github.com/sci4me/yakvs

## Usage

	yakvs <port>

## Protocol

YAKVS uses a text-based TCP protocol. Commands and results are newline delimited. Here are the supported commands:

    PUT <key> <value>
    Associates the specified value with the specified key in the store
    Returns: 
      OK - success
      ERROR - incorrect arguments

    GET <key>
    Returns the value to which the specified key is mapped
    Returns:
      VALUE - the value to which the specified key is mapped
      nil - store contains no mapping for the key
      ERROR - incorrect arguments

    HASKEY <key>
    Returns true if this store contains a mapping for the specified key
    Returns:
      TRUE - store contains a mapping for the key
      FALSE - store contains no mapping for the key

    HASVALUE <value>
    Returns true if this store contains a mapping for the specified value
    Returns:
      TRUE - store contains a mapping for the value
      FALSE - store contains no mapping for the value

    REMOVE <key>
    Removes the mapping for the specified key from this map if present
    Returns:
      OK - success
      ERROR - incorrect arguments

    SIZE
    Returns the number of key-value mappings in this store
    Returns:
      SIZE - the number of key-value mappings in this store
      ERROR - too many arguments

    CLEAR
    Removes all of the mappings from this store
    Returns:
      OK - success
      ERROR - too many arguments

    LIST
    Lists the key-value mappings in this store
    Returns:
      KVPs - the key-value mappings in the store
      nil - there are no key-value mappings in the store
      ERROR - too many arguments

    LIST KEYS
    Lists the keys in this store
    Returns:
      KEYS - the keys in the store
      nil - there are no key-value mappings in the store
      ERROR - incorrect arguments

    LIST VALUES
    Lists the values in this store
    Returns:
      VALUES - the values in the store
      nil - there are no key-value mappings in the store
      ERROR - incorrect arguments

    QUIT
    Closes the connection
    Returns:
      BYE - success
      ERROR - incorrect arguments

If the server is stopped while there are active connections, the following message will be sent to the connections:
```
SERVER STOPPED
```
The connections will then be closed. All connections will be closed in this fashion before the server stops.

If a connection is attempted during the time between the start and end of the Stop() call, the connection will be denied. The client will receive the following message:
```
CONNECTION DENIED
```
The connection will then be closed.