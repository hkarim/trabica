#!/usr/bin/env sh
mod/node-template/target/graalvm-native-image/node-template \
bootstrap \
--id 00 \
--host 0.0.0.0 \
--port 6000 \
--data var/00 \
--peer 01@0.0.0.0:6001 \
--peer 02@0.0.0.0:6002