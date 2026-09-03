#!/bin/bash
cd /root/pump-sniper-rust
nohup ./target/release/pump-sniper > logs_sniper.out 2>&1 &
echo $! > sniper.pid
