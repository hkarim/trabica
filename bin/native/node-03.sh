#!/usr/bin/env sh
mod/node-template/target/graalvm-native-image/node-template \
startup \
--id 03 \
--host 0.0.0.0 \
--port 6003 \
--data var/03
