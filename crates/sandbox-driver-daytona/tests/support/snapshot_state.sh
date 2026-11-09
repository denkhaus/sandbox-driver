#!/usr/bin/env bash
# Check the restored files and the named process, without confusing a reused
# PID's shell arguments with the original process's argv[0].
set -eu
test "$(cat "$1")" = preserved || exit 21
pid=$(cat "$2")
case "$pid" in ''|*[!0-9]*) exit 22 ;; esac
cmdline="${5:-/proc}/$pid/cmdline"
process_name=
if test -r "$cmdline"; then
    IFS= read -r -d '' process_name < "$cmdline" || true
fi
case "$4" in
    cold) test "$process_name" != "$3" || exit 23 ;;
    live)
        test "$process_name" = "$3" || exit 24
        printf '%s preserved' "$pid"
        ;;
    *) exit 25 ;;
esac
