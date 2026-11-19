FROM redis/redis-stack-server:7.2.0-v11
COPY shepherd_broker/redis.conf /usr/local/etc/redis/redis.conf
COPY shepherd_broker/entrypoint.sh /usr/local/bin/shepherd-broker-entrypoint.sh
CMD [ "sh", "/usr/local/bin/shepherd-broker-entrypoint.sh", "/usr/local/etc/redis/redis.conf" ]