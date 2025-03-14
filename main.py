from logging import INFO, basicConfig

import redis

from redisreentrantlock import ReentrantLock


logging_config = dict(
    level=INFO,
    # format='%(asctime)s %(message)s'
    format='[%(asctime)s - %(filename)s:%(lineno)d - %(funcName)s - %(levelname)s] %(message)s'
)

basicConfig(**logging_config)


r = redis.Redis(host='localhost', port=6379, db=0)

# r.lock('test', lock_class=ReentrantLock)
# r.hset('test', mapping={'a': 1})

# print(r.hget('test', 'tocken'))

# r.expire('test', 5)

from time import sleep

with r.lock('test', lock_class=ReentrantLock):
    print('level 1')
    sleep(10)
    with r.lock('test', lock_class=ReentrantLock):
        print('level 2')
        sleep(20)
    print('level 1')
    sleep(10)

# r.delete('test')
