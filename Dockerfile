# scalas3 のサーバーと CLI のイメージ。先に `sbt server/assembly cli/assembly` で jar を作っておく。
FROM eclipse-temurin:21-jre

# データディレクトリはイメージ内に作って scalas3 の所有にしておく。マウント先がイメージにないと、Docker は
# 名前付きボリュームや匿名ボリュームを root 所有で作るため、非 root のサーバーが書き込めずに起動直後に落ちる。
# 中身が空のボリュームは初回のマウント時に、このディレクトリの所有者と権限を引き継ぐ
RUN useradd --system --home /var/lib/scalas3 --create-home scalas3 \
 && install -d -o scalas3 -g scalas3 -m 0750 /var/lib/scalas3/data
COPY server/target/scala-3.9.0/scalas3-server.jar /opt/scalas3/scalas3-server.jar
COPY cli/target/scala-3.9.0/scalas3-cli.jar /opt/scalas3/scalas3-cli.jar
RUN printf '#!/bin/sh\nexec java -jar /opt/scalas3/scalas3-cli.jar "$@"\n' > /usr/local/bin/scalas3 && chmod +x /usr/local/bin/scalas3

USER scalas3
WORKDIR /var/lib/scalas3
# ヒープはコンテナのメモリの半分にする。ヒープの外（GC の管理領域、スレッドのスタック、コードなど）で約 300 MB 使う
# （docs/benchmark.md）。GC は G1 を使う：2 GB の VM での測定では、ZGC は G1 より小さな PUT が遅く（世代別 ZGC で
# 約 25% 減）、ヒープを確保しきって OOM で強制終了されやすかった
ENV SCALAS3_DATA=/var/lib/scalas3/data \
    SCALAS3_S3_LISTEN=0.0.0.0:3900 \
    SCALAS3_RPC_LISTEN=0.0.0.0:3901 \
    SCALAS3_ADMIN_LISTEN=0.0.0.0:3903 \
    JAVA_TOOL_OPTIONS="-XX:+UseG1GC -XX:MaxRAMPercentage=50"
VOLUME ["/var/lib/scalas3/data"]
EXPOSE 3900 3901 3902 3903
ENTRYPOINT ["java", "-jar", "/opt/scalas3/scalas3-server.jar"]
