#!/bin/bash

set -euo pipefail

CANOPY_DIR="$(cd "$(dirname "$0")" && pwd)"
UV_BIN=""

is_windows_shell() {
	case "${OSTYPE:-}" in
		msys*|cygwin*|win32*|mingw*) return 0 ;;
	esac
	local uname_s
	uname_s="$(uname -s 2>/dev/null | tr '[:upper:]' '[:lower:]')"
	case "$uname_s" in
		msys*|cygwin*|mingw*) return 0 ;;
	esac
	return 1
}

resolve_uv() {
	if command -v uv >/dev/null 2>&1; then
		UV_BIN="$(command -v uv)"
		return 0
	fi
	if command -v uv.exe >/dev/null 2>&1; then
		UV_BIN="$(command -v uv.exe)"
		return 0
	fi
	return 1
}

resolve_winget() {
	if command -v winget >/dev/null 2>&1; then
		echo "winget"
		return 0
	fi
	if command -v winget.exe >/dev/null 2>&1; then
		echo "winget.exe"
		return 0
	fi
	return 1
}

ensure_uv() {
	if resolve_uv; then
		echo "CANOPY : Using uv at $UV_BIN"
		return 0
	fi

	echo "CANOPY : uv not found, attempting install"

	if is_windows_shell; then
		local winget_cmd
		if winget_cmd="$(resolve_winget)"; then
			"$winget_cmd" install --id=astral-sh.uv -e --accept-package-agreements --accept-source-agreements || true
		else
			echo "CANOPY : winget not found, install uv manually"
			echo "CANOPY : https://docs.astral.sh/uv/getting-started/installation/"
			exit 1
		fi
	elif [[ "$(uname -s)" == "Darwin" ]]; then
		if command -v brew >/dev/null 2>&1; then
			brew install uv || true
		else
			echo "CANOPY : Homebrew not found, install uv manually"
			echo "CANOPY : https://docs.astral.sh/uv/getting-started/installation/"
			exit 1
		fi
	else
		if command -v curl >/dev/null 2>&1; then
			curl -LsSf https://astral.sh/uv/install.sh | sh
			export PATH="$HOME/.local/bin:$PATH"
		else
			echo "CANOPY : curl not found, install uv manually"
			echo "CANOPY : https://docs.astral.sh/uv/getting-started/installation/"
			exit 1
		fi
	fi

	export PATH="$HOME/.local/bin:$PATH"
	if ! resolve_uv; then
		echo "CANOPY : uv installation finished but uv is still not on PATH"
		echo "CANOPY : open a new terminal and rerun ./update.sh"
		exit 1
	fi

	echo "CANOPY : Installed uv at $UV_BIN"
}

echo "CANOPY : Starting standalone update in $CANOPY_DIR"
cd "$CANOPY_DIR"

ensure_uv

# Skip uv self-update on Windows/package-manager installs to avoid confusing warnings
if is_windows_shell; then
	echo "CANOPY : Skipping uv self-update on Windows"
# Try uv self-update on Unix-like shells, but continue when uv is package-managed
else
	"$UV_BIN" self update || true
fi

if "$UV_BIN" venv --python 3.13 --allow-existing .venv >/dev/null 2>&1; then
	echo "CANOPY : Created or reused .venv with Python 3.13"
else
	echo "CANOPY : Python 3.13 not available, creating .venv with default Python"
	"$UV_BIN" venv --allow-existing .venv
fi

"$UV_BIN" sync --upgrade

mkdir -p data/source data/temp data/processed data/releases data/geo data/apis

echo "CANOPY : Update complete"
echo "CANOPY : Run with: uv run python -m importer.canopy.run --process --fuse"
