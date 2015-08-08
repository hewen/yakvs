# YAKVS

YAKVS (Yet Another Key Value Store) is a tiny, lightweight, networked key-value store written in Go.

## Install

    go get github.com/sci4me/yakvs

## Usage

	yakvs <port>

## Protocol

This package implements a TCP network service which exposes a key-value store to clients. The client can perform the following commands:

 - PUT
 - GET
 - HAS
 - REMOVE
 - SIZE
 - QUIT

The server can send the following responses:

 - OK
 - ERROR
 - ITEM
 - TRUE
 - FALSE
 - SIZE

Keys and values may not have any spaces in them.