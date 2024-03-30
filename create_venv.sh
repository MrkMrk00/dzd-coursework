#!/usr/bin/env bash

[[ -z "$PY_EXE" ]] && PY_EXE="python3"

if [[ ! -d "venv" ]]; then
	$PY_EXE -m venv venv
fi

source ./venv/bin/activate

