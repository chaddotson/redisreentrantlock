import os
import socket

import redis
from redis.lock import Lock
from redis import StrictRedis

import threading
import time as mod_time
import uuid
from types import SimpleNamespace, TracebackType
from typing import Optional, Type

from redis.exceptions import LockError, LockNotOwnedError
from redis.typing import Number


class ReentrantLock(Lock):
    lua_acquire = None

    # KEYS[1] is the name of the hash
    # ARGV[1] is the value for the 'token' field
    # ARGV[2] is the timeout for KEYS[1].
    LUA_ACQUIRE_SCRIPT = """
        local token = redis.call('hget', KEYS[1], 'token')
        if not token then
            redis.call('hset', KEYS[1], 'token', ARGV[1])
            redis.call('hset', KEYS[1], 'count', 0)
        elseif token ~= ARGV[1] then
            return 0
        end

        redis.call('hincrby', KEYS[1], 'count', 1)

        local timeout = tonumber(ARGV[2])
        if not (timeout == nil) then
            redis.call('expire', KEYS[1],  timeout)
        end

        return 1
    """

    # KEYS[1] - lock name
    # ARGV[1] - token
    # return 1 if the lock was released, otherwise 0
    LUA_RELEASE_SCRIPT = """
        local token = redis.call('hget', KEYS[1], 'token')
        if not token or token ~= ARGV[1] then
            return 0
        end

        local count = redis.call('hget', KEYS[1], 'count')
        if not count then
            return 0
        end

        redis.call('hincrby', KEYS[1], 'count', -1)
        count = redis.call('hget', KEYS[1], 'count')
        if count == "0" then
            redis.call('del', KEYS[1])
        end
        return 1
    """

    # KEYS[1] - lock name
    # ARGV[1] - token
    # ARGV[2] - additional milliseconds
    # ARGV[3] - "0" if the additional time should be added to the lock's
    #           existing ttl or "1" if the existing ttl should be replaced
    # return 1 if the locks time was extended, otherwise 0
    LUA_EXTEND_SCRIPT = """
        local token = redis.call('hget', KEYS[1], 'token')
        if not token or token ~= ARGV[1] then
            return 0
        end
        local expiration = redis.call('pttl', KEYS[1])
        if not expiration then
            expiration = 0
        end
        if expiration < 0 then
            return 0
        end

        local newttl = ARGV[2]
        if ARGV[3] == "0" then
            newttl = ARGV[2] + expiration
        end
        redis.call('pexpire', KEYS[1], newttl)
        return 1
    """

    # KEYS[1] - lock name
    # ARGV[1] - token
    # ARGV[2] - milliseconds
    # return 1 if the locks time was reacquired, otherwise 0
    LUA_REACQUIRE_SCRIPT = """
        local token = redis.call('hget', KEYS[1], 'token')
        if not token or token ~= ARGV[1] then
            return 0
        end
        redis.call('pexpire', KEYS[1], ARGV[2])
        return 1
    """

    def register_scripts(self) -> None:
        cls = self.__class__
        client = self.redis

        if cls.lua_acquire is None:
            cls.lua_acquire = client.register_script(cls.LUA_ACQUIRE_SCRIPT)
        if cls.lua_release is None:
            cls.lua_release = client.register_script(cls.LUA_RELEASE_SCRIPT)
        if cls.lua_extend is None:
            cls.lua_extend = client.register_script(cls.LUA_EXTEND_SCRIPT)
        if cls.lua_reacquire is None:
            cls.lua_reacquire = client.register_script(cls.LUA_REACQUIRE_SCRIPT)

    # TODO: Acquire
    def acquire(
            self,
            sleep: Optional[Number] = None,
            blocking: Optional[bool] = None,
            blocking_timeout: Optional[Number] = None,
            token: Optional[str] = None,
    ):

        if sleep is None:
            sleep = self.sleep
        # if token is None:
        #     token = uuid.uuid1().hex.encode()
        # else:
        token = f'{socket.gethostname()}:{threading.get_native_id()}'
        encoder = self.redis.get_encoder()
        token = encoder.encode(token)

        if blocking is None:
            blocking = self.blocking
        if blocking_timeout is None:
            blocking_timeout = self.blocking_timeout
        stop_trying_at = None
        if blocking_timeout is not None:
            stop_trying_at = mod_time.monotonic() + blocking_timeout
        while True:
            if self.do_acquire(token):
                self.local.token = token
                return True
            if not blocking:
                return False
            next_try_at = mod_time.monotonic() + sleep
            if stop_trying_at is not None and next_try_at > stop_trying_at:
                return False
            mod_time.sleep(sleep)

    def do_acquire(self, token: str) -> bool:
        args = [token]
        if self.timeout:
            # convert to milliseconds
            timeout = int(self.timeout * 1000)
            args.append(timeout)
        else:
            timeout = None
        # if self.redis.set(self.name, token, nx=True, px=timeout):
        print(self.name, token, timeout)


        if bool(
                self.lua_acquire(keys=[self.name], args=args, client=self.redis)
        ):
            return True
        return False

    def owned(self) -> bool:
        """
        Returns True if this key is locked by this lock, otherwise False.
        """
        stored_token = self.redis.hget(self.name, 'token')
        # need to always compare bytes to bytes
        # TODO: this can be simplified when the context manager is finished
        if stored_token and not isinstance(stored_token, bytes):
            encoder = self.redis.get_encoder()
            stored_token = encoder.encode(stored_token)
        return self.local.token is not None and stored_token == self.local.token

    def do_release(self, expected_token: str) -> None:
        print('-->', self.name, expected_token)
        if not bool(
                self.lua_release(keys=[self.name], args=[expected_token], client=self.redis)
        ):
            raise LockNotOwnedError("Cannot release a lock that's no longer owned")




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