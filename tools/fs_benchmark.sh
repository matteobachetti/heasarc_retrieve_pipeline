#!/bin/bash
# Compare filesystems on the I/O profile HEASOFT actually produces.
#
# A reduced NuSTAR observation is 352 files, median 31 kB, 79% of them under 1 MB, and a
# single nupipeline run spawns at least 44 sub-tools, each of which reads and rewrites a
# parameter file. That is metadata-bound work: the number of operations matters far more
# than the number of bytes, which is the case a parallel filesystem handles worst and a
# local disk handles best. This script measures exactly that, so the choice of
# scratch_dir (see retrieve_and_process_data) can be made on numbers rather than on
# folklore.
#
# Usage, on a compute node of the cluster you are going to run on:
#
#     srun -n1 bash tools/fs_benchmark.sh /scratch/your/project /tmp
#
# Each argument is a directory to test; it is created, used, and removed again. Compare
# the per-file microsecond figures, not the MB/s ones: if the shared filesystem is within
# a factor of a few on "create" and "lookup", leaving the working directories where they
# are costs little. If it is ten times slower, and there is room, scratch_dir is worth
# setting.
#
# Environment overrides: NFILES (default 2000), NBIG (400), KB (32).
set -u
NFILES=${NFILES:-2000}
NBIG=${NBIG:-400}
KB=${KB:-32}          # median file size measured in a real reduced observation

bench () {
    local base=$1
    local dir="$base/fsbench.$$"
    mkdir -p "$dir" || { echo "  cannot write to $base"; return; }
    echo "--- $base"
    df -h "$base" | tail -1 | awk '{printf "    filesystem %s, %s free\n", $1, $4}'
    mount | grep -F " $(df -P "$base" | tail -1 | awk '{print $6}') " | head -1 | sed 's/^/    /'

    # Measured cost of forking a process on the machine this was written on: about a
    # millisecond, which is larger than most of the operations below and would make every
    # filesystem look the same. So the per-file loops use shell builtins only, and the
    # two steps with no portable builtin -- delete and read back -- fork once for the
    # whole batch instead of once per file. (bash 3.2, which is what macOS ships, has
    # neither mapfile nor read -N.)
    local blob
    printf -v blob "%${KB}024s" ""   # KB kibibytes of spaces, built once, no fork

    # 1. metadata: create many empty files
    local t0=$(date +%s.%N)
    for ((i = 1; i <= NFILES; i++)); do : > "$dir/e$i"; done
    local t1=$(date +%s.%N)
    # 2. metadata: look them all up
    for ((i = 1; i <= NFILES; i++)); do [ -f "$dir/e$i" ] || echo "missing e$i"; done
    local t2=$(date +%s.%N)
    # 3. metadata: delete them -- one rm, N unlink calls
    rm -f "$dir"/e*
    local t3=$(date +%s.%N)
    # 4. small-file writes, the size HEASOFT actually produces
    for ((i = 1; i <= NBIG; i++)); do printf "%s" "$blob" > "$dir/s$i"; done
    local t4=$(date +%s.%N)
    # 5. read them back -- one cat, N opens
    cat "$dir"/s* > /dev/null
    local t5=$(date +%s.%N)
    rm -rf "$dir"

    awk -v a=$t0 -v b=$t1 -v c=$t2 -v d=$t3 -v e=$t4 -v f=$t5 -v n=$NFILES -v m=$NBIG -v k=$KB 'BEGIN{
      printf "    create %5d files : %7.2f s  (%6.0f us/file)\n", n, b-a, 1e6*(b-a)/n
      printf "    lookup %5d files : %7.2f s  (%6.0f us/file)\n", n, c-b, 1e6*(c-b)/n
      printf "    delete %5d files : %7.2f s  (%6.0f us/file)\n", n, d-c, 1e6*(d-c)/n
      printf "    write  %5d x %d kB: %7.2f s  (%6.1f MB/s)\n", m, k, e-d, m*k/1024/(e-d)
      printf "    read   %5d x %d kB: %7.2f s  (%6.1f MB/s)\n", m, k, f-e, m*k/1024/(f-e)
    }'
}

if [ $# -eq 0 ]; then
    echo "usage: $0 <directory> [<directory> ...]" >&2
    exit 2
fi
for target in "$@"; do bench "$target"; done
