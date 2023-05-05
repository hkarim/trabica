#!/usr/bin/env sh
mod/node-template/target/graalvm-native-image/node-template \
join \
--id 01 \
--host 0.0.0.0 \
--port 6001 \
--peer-address 0.0.0.0:5555 \
--data var/01
