#!/usr/bin/env bash
# ローリングアップグレードの試験（podman）。古い版（git の参照）と、いまの作業ツリーのイメージを作り、
# 3 ノードを古い版で起動してデータを書いてから、1 台ずつ新しい版に入れ替えて確かめる（UpgradeCheck）。
#   it/upgrade/run.sh [古い版の参照]      例：it/upgrade/run.sh v0.9.0   （既定は HEAD~1）
# 環境変数：KEEP=1 なら終わった後もコンテナを残す。SKIP_BUILD=1 なら作ってあるイメージを使う。
set -euo pipefail

OLD_REF=${1:-HEAD~1}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
OLD_IMAGE=localhost/scalas3:upgrade-old
NEW_IMAGE=localhost/scalas3:upgrade-new

build() { # $1 = ソースのディレクトリ, $2 = イメージ名
  (cd "$1" && sbt -batch server/assembly cli/assembly >/dev/null && podman build -q -t "$2" . >/dev/null)
}

if [ -z "${SKIP_BUILD:-}" ]; then
  WORK=$(mktemp -d)
  trap 'git -C "$ROOT" worktree remove --force "$WORK/old" 2>/dev/null || true; rm -rf "$WORK"' EXIT
  echo "[upgrade] building the old version ($OLD_REF = $(git -C "$ROOT" rev-parse --short "$OLD_REF"))"
  git -C "$ROOT" worktree add --detach "$WORK/old" "$OLD_REF" >/dev/null
  build "$WORK/old" "$OLD_IMAGE"
  echo "[upgrade] building the new version (working tree)"
  build "$ROOT" "$NEW_IMAGE"
fi

cd "$ROOT"
sbt -batch "server/Test/runMain scalas3.server.UpgradeCheck $OLD_IMAGE $NEW_IMAGE" 2>&1 | grep --line-buffered '\[upgrade\]\|Exception\|error'
