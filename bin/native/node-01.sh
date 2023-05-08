#!/usr/bin/env sh
mod/node-template/target/graalvm-native-image/node-template \
startup \
--id 01 \
--host 0.0.0.0 \
--port 6001 \
--data var/01
