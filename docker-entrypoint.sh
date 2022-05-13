#! /bin/bash

# run jupyter in the background, and forward SIGTERM manually.
# "exec" seems like a much simpler solution for this,
# however, jupyter kernels die noisy deaths when using exec.

_term () {
  echo 'Caught SIGERM'
  kill -TERM "$child"
}
trap _term SIGTERM


# run jupyter notebook server
jupyter notebook \
    --ip='*' \
    --notebook-dir=/home/magpyuser/notebooks \
    --no-browser \
    --port=8000 &

child=$!
wait "$child"
