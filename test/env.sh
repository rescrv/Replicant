export REPLICANT_SRCDIR="$1"
export REPLICANT_BUILDDIR="$2"
export REPLICANT_VERSION="$3"

export REPLICANT_EXEC_PATH="${REPLICANT_BUILDDIR}"

export PATH=${REPLICANT_BUILDDIR}:${REPLICANT_SRCDIR}:${PATH}
