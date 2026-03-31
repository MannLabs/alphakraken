#!/bin/bash

PROGRAM="spectronaut"

# Run the program with all arguments
"$PROGRAM" "$@"
exit_code=$?

exit $exit_code
