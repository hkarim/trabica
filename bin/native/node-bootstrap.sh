#!/usr/bin/env sh
mod/node-template/target/graalvm-native-image/node-template \
bootstrap \
--id 00 \
--host 0.0.0.0 \
--port 5555 \
--data var/00