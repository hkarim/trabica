#!/usr/bin/env sh
opt/node-template/target/graalvm-native-image/node-template \
startup \
--id 02 \
--host 0.0.0.0 \
--port 6002 \
--data var/02
