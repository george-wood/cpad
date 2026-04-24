#!/usr/bin/env bash
# Run the targets pipeline, filtering a benign polars warning that's
# written to raw stderr by the Rust engine and can't be caught from R.
#
# Usage:
#   ./make.sh                # build everything (tar_make)
#   ./make.sh outdated       # show stale targets
#   ./make.sh visnetwork     # open the DAG viewer
#   ./make.sh destroy        # nuke targets metadata
#   ./make.sh 'tar_make(names = raw_arrest)'   # arbitrary targets expr
set -euo pipefail

cd "$(dirname "$0")"

case "${1:-make}" in
  make)       expr='targets::tar_make()' ;;
  outdated)   expr='print(targets::tar_outdated())' ;;
  visnetwork) expr='targets::tar_visnetwork()' ;;
  destroy)    expr='targets::tar_destroy()' ;;
  *)          expr="targets::$1" ;;
esac

# Drop only the specific benign line from stdout. targets captures each
# target's subprocess stderr and replays it on the parent's stdout, so
# the filter needs to live on stdout (not stderr). Real errors still flow
# through untouched on stderr. --line-buffered keeps output streaming.
Rscript -e "$expr" > >(grep --line-buffered -v "Sortedness of columns cannot be checked")
