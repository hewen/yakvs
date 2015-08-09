# YAKVS [![Build Status](https://travis-ci.org/sci4me/yakvs.svg?branch=master)](https://travis-ci.org/sci4me/yakvs)

YAKVS (Yet Another Key Value Store) is a tiny, lightweight, networked, in-memory key-value store written in Go.

## Install

    go get github.com/sci4me/yakvs

## Usage

	yakvs <port>

## Protocol

This package implements a TCP network service which exposes an in-memory key-value store to clients. The client can perform the following commands:

 - PUT
 - GET
 - HAS
 - REMOVE
 - SIZE
 - CLEAR
 - LIST
 - LIST KEYS
 - LIST VALUES
 - QUIT

The server can send the following responses:

 - OK
 - ERROR
 - ITEM
 - TRUE
 - FALSE
 - SIZE

Keys and values may not have any spaces in them.