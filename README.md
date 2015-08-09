# YAKVS [![Build Status](https://travis-ci.org/sci4me/yakvs.svg?branch=master)](https://travis-ci.org/sci4me/yakvs)

YAKVS (Yet Another Key Value Store) is a tiny, lightweight, networked, in-memory key-value store written in Go.

## Install

    go get github.com/sci4me/yakvs

## Usage

	yakvs <port>

## Protocol

YAKVS uses a text-based TCP protocol. Commands and results are newline delimited. Here are the supported commands:

    PUT <key> <value>
    Returns: 
      OK - success
      ERROR - incorrect arguments

    GET <key>
    Returns:
      VALUE - the value to which the specified key is mapped
      nil - store contains no mapping for the key
      ERROR - incorrect arguments

    HAS <key>
    Returns:
      TRUE - store contains a mapping for the key
      FALSE - store contains no mapping for the key

    REMOVE <key>
    Returns:
      OK - success
      ERROR - incorrect arguments

    SIZE
    Returns:
      SIZE - the number of key-value mappings in this store
      ERROR - too many arguments

    CLEAR
    Returns:
      OK - success
      ERROR - too many arguments

    LIST
    Returns:
      KVPs - the key-value mappings in the store
      nil - there are no key-value mappings in the store
      ERROR - too many arguments

    LIST KEYS
    Returns:
      KEYS - the keys in the store
      nil - there are no key-value mappings in the store
      ERROR - incorrect arguments

    LIST VALUES
    Returns:
      VALUES - the values in the store
      nil - there are no key-value mappings in the store
      ERROR - incorrect arguments

    QUIT
    Returns:
      OK - success
      ERROR - incorrect arguments