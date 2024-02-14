#!/bin/sh
set -eu

grep 'cws:' "$1" \
| sed -E 's/^.*time="....-..-..T(..:..:..)\....Z".*msg="/\1:\t/g' \
| sed 's/" namespace.*$//g'
