#!/usr/bin/env sh
mod/node-template/target/graalvm-native-image/node-template \
startup \
--id 02 \
--host 0.0.0.0 \
--port 6002 \
--peer-address 0.0.0.0:6001 \
--data var/02
