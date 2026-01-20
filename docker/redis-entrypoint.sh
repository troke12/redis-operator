#!/bin/sh
set -e

# Read password from secret env
REDIS_PASSWORD="${REDIS_PASSWORD:-}"
REDIS_PASSWORD=$(echo "${REDIS_PASSWORD}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

if [ -z "${REDIS_PASSWORD}" ]; then
  echo "ERROR: REDIS_PASSWORD environment variable is not set or is empty"
  exit 1
fi

# Ensure /data directory exists
mkdir -p /data

# Copy config to writable location (ConfigMap mounts are read-only)
cp /etc/redis/redis.conf /data/redis.conf

# Escape password for sed: backslashes, forward slashes, pipes, ampersands
ESCAPED_PWD=$(echo "${REDIS_PASSWORD}" | sed 's/\\/\\\\/g; s/\//\\\//g; s/|/\\|/g; s/&/\\&/g')

# Update password in redis.conf using sed with | delimiter
sed "s|^[[:space:]]*requirepass.*|requirepass ${ESCAPED_PWD}|" /data/redis.conf > /data/redis.conf.tmp
sed "s|^[[:space:]]*masterauth.*|masterauth ${ESCAPED_PWD}|" /data/redis.conf.tmp > /data/redis.conf
rm -f /data/redis.conf.tmp

# Set cluster announce IP (use Pod IP if available) so nodes advertise reachable addresses
if [ -n "${POD_IP:-}" ]; then
  printf "\ncluster-announce-ip %s\n" "${POD_IP}" >> /data/redis.conf
fi

# Execute the original command (redis-server /data/redis.conf)
exec "$@"