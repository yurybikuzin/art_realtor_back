#/usr/bin/env bash
dir=$(dirname "$0")
cd "$dir"
rm -rf nohup.out
nohup ./art_realtor_back for-analytics &disown # https://superuser.com/a/178591
sleep 1
tail -f nohup.out
