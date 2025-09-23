#!/bin/sh
set -eu

VALUE='urn:li:dataset:(urn:li:dataPlatform:postgres,postgres.schema.customers,PROD)%foo'
OUTPUT=$(printf '%s\n' "$VALUE")
if [ "$OUTPUT" != "$VALUE" ]; then
  printf '%s\n' 'printf unsafe' >&2
  exit 1
fi
