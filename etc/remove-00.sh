#!/usr/bin/env sh
grpcurl -plaintext \
-import-path mod/lib-trabica-proto/src/main/protobuf/ \
-import-path mod/lib-trabica-rpc/src/main/protobuf/ \
-proto rpc.proto \
-d '{"node": {"id": "00", "peer": {"host":"0.0.0.0", "port":6000}}}' \
0.0.0.0:6000 \
trabica.Trabica/RemoveServer
