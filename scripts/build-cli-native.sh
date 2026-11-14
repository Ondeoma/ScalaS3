#!/usr/bin/env bash
# CLI をネイティブのバイナリにする（GraalVM の native-image が要る）。
#   GRAALVM_HOME=/path/to/graalvm scripts/build-cli-native.sh [出力先]
# 起動が速く（JVM の起動を待たない）、JDK のないマシンでも動く。cert create だけは keytool（JDK）を使う。
set -euo pipefail
cd "$(dirname "$0")/.."
: "${GRAALVM_HOME:?GRAALVM_HOME を GraalVM の Home に設定してください}"
out=${1:-cli/target/scalas3}
sbt -batch cli/assembly
"$GRAALVM_HOME/bin/native-image" --no-fallback -jar cli/target/scala-3.9.0/scalas3-cli.jar -o "$out"
echo "built: $out"
