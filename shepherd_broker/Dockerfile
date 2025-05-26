FROM redis/redis-stack-server:7.2.0-v11
COPY shepherd_broker/redis.conf /usr/local/etc/redis/redis.conf
CMD [ "redis-server", "/usr/local/etc/redis/redis.conf" ]