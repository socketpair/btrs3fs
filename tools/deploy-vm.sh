#!/bin/bash
# Builds btrs3fs here and copies it with the credentials to the test VM, where
# all disk experiments run (CLAUDE.md): ~/btrs3fs/btrs3fs and ~/btrs3fs/creds.json.
#   tools/deploy-vm.sh [user@host]
set -euo pipefail
cd "$(dirname "$0")/.."
VM=${1:-runner-fedora@51.250.66.166}

cargo build --release
ssh "$VM" 'mkdir -p ~/btrs3fs && chmod 700 ~/btrs3fs'
# A new file renamed over the old one: a running daemon keeps its copy.
scp -q target/release/btrs3fs "$VM:btrs3fs/btrs3fs.new"
scp -q creds.json "$VM:btrs3fs/creds.json.new"
ssh "$VM" 'cd ~/btrs3fs && chmod 600 creds.json.new && mv -f creds.json.new creds.json &&
    mv -f btrs3fs.new btrs3fs && ./btrs3fs --version'
