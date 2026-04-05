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

run_with_optional_sudo() {
	if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
		"$@"
		return $?
	fi
	if command -v sudo >/dev/null 2>&1; then
		sudo "$@"
		return $?
	fi
	echo "CANOPY : sudo not found and current user is not root"
	return 1
}

install_with_platform_package_manager() {
	local pkg="$1"
	local winget_id="${2:-}"
	if is_windows_shell; then
		local winget_cmd=""
		if winget_cmd="$(resolve_winget)"; then
			if [[ -n "$winget_id" ]]; then
				"$winget_cmd" install --id="$winget_id" -e --accept-package-agreements --accept-source-agreements || true
			else
				"$winget_cmd" install --name "$pkg" --accept-package-agreements --accept-source-agreements || true
			fi
		fi
	elif [[ "$(uname -s)" == "Darwin" ]]; then
		if command -v brew >/dev/null 2>&1; then
			brew install "$pkg" || true
		fi
	else
		if command -v apt-get >/dev/null 2>&1; then
			run_with_optional_sudo apt-get update || true
			run_with_optional_sudo apt-get install -y "$pkg" || true
		elif command -v dnf >/dev/null 2>&1; then
			run_with_optional_sudo dnf install -y "$pkg" || true
		elif command -v yum >/dev/null 2>&1; then
			run_with_optional_sudo yum install -y "$pkg" || true
		elif command -v pacman >/dev/null 2>&1; then
			run_with_optional_sudo pacman -Sy --noconfirm "$pkg" || true
		elif command -v zypper >/dev/null 2>&1; then
			run_with_optional_sudo zypper --non-interactive install "$pkg" || true
		fi
	fi
}

ensure_command() {
	local cmd="$1"
	local pkg="$2"
	local winget_id="${3:-}"
	if command -v "$cmd" >/dev/null 2>&1; then
		echo "CANOPY : Using $cmd at $(command -v "$cmd")"
		return 0
	fi
	echo "CANOPY : $cmd not found, attempting install"
	install_with_platform_package_manager "$pkg" "$winget_id"
	if command -v "$cmd" >/dev/null 2>&1; then
		echo "CANOPY : Installed $cmd at $(command -v "$cmd")"
		return 0
	fi
	echo "CANOPY : unable to install $cmd automatically"
	echo "CANOPY : install it manually and rerun update.sh"
	exit 1
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
# Ensure aria2 is available for parallel dataset downloads
ensure_command "aria2c" "aria2" "aria2.aria2"
# Ensure ripgrep is available for large wikidata prefilter operations
ensure_command "rg" "ripgrep" "BurntSushi.ripgrep.MSVC"
# Ensure pigz is available for parallel gzip chunk processing
ensure_command "pigz" "pigz"

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
