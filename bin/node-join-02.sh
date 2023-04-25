#!/usr/bin/env sh
mod/node-template/target/universal/stage/bin/node-template -- \
join \
--ip 0.0.0.0 \
--port 6002 \
--peer-address 0.0.0.0:5555
