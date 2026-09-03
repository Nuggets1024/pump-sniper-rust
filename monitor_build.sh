#!/bin/bash
OUT=/root/pump-sniper-rust/build_status.txt
rm -f $OUT
while true; do
  if pgrep -f "cargo build --release" >/dev/null; then
    BIN=$(ls -lh /root/pump-sniper-rust/target/release/pump-sniper 2>/dev/null | awk '{print $5}')
    [ -z "$BIN" ] && BIN="none"
    SW=$(free -m | awk '/Swap/ {print $3"/"$2}')
    echo "$(date '+%H:%M:%S') RUNNING bin=$BIN swap=$SW" >> $OUT
    sleep 60
  else
    BIN=$(ls -lh /root/pump-sniper-rust/target/release/pump-sniper 2>/dev/null | awk '{print $5}')
    echo "$(date '+%H:%M:%S') DONE_OR_STOPPED" >> $OUT
    echo "FINAL_BIN=$BIN" >> $OUT
    break
  fi
done
