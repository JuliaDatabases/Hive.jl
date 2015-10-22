#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
OUTDIR=${DIR}/../src/
thrift --gen jl --out ${OUTDIR} HS2.thrift
