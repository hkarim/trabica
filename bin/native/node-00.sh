#!/usr/bin/env sh
opt/node-template/target/graalvm-native-image/node-template \
startup \
--id 00 \
--host 0.0.0.0 \
--port 6000 \
--data var/01
