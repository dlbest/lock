#!/usr/bin/env python

"""
redis+python实现分布式锁
"""

import redis
from redis import Redis
import uuid
import time


redis_cli = Redis.from_url('redis://localhost:6379/0')


class DistributedLock(object):
    def __init__(self, lock_name):
        self.lock_name = 'lock_name: ' + self.lock_name
        self.identifier = None

    def acquire(self, acquire_time=10, time_out=10):
        if self.identifier:   # 如果不为空，说明此时存在锁，不可以二次获取
            return False
        self.identifier = str(uuid.uuid4())   # 获取唯一的值，不可能存在锁冲突
        end = time.time() + acquire_time   # 设置一个获取时间，如果在规定时间内没有拿到，则失败
        while time.time() < end:
            if redis_cli.setnx(self.lock_name, self.identifier):  # 创建是否成功
                redis_cli.expire(self.lock_name, time_out)   # 设置锁的超时时间
                return self.identifier
            elif redis_cli.ttl(self.lock_name) == -1:   # 不成功，且发现没有设置超时时间，则为这个锁设置一个超时时间
                redis_cli.expire(self.lock_name, time_out)
            time.sleep(0.001)
        self.identifier = None   # 如果没有成功，需要清空值
        return False

    def release(self):
        pipe = redis_cli.pipeline(True)
        res = False
        # redis的事务管道，实际上是在本地先写一个批处理，如果写批处理的器件锁被别人给改了，说明锁被别人给拿去了，则事务失效
        while True:
            try:
                # watch只要是在execute时查看键的值与初始值是否一样，一样就执行，不一样就不执行，
                # 实际上在redis中只是批处理，不支持事务的，如果execute中出现问题，是不支持回滚的
                # 只是在前端支持事务，伪事务
                pipe.watch(self.lock_name)
                if pipe.get(self.lock_name) == self.identifier:
                    pipe.multi()
                    pipe.delete(self.lock_name)  # 支持多个操作，现在只设置一个操作
                    pipe.execute()     # 真正执行
                    res = True

                pipe.unwatch()
                self.identifier = None  # 释放锁之后将值清空
                break
            except redis.exceptions.WatchError:
                pass

        return res

    def keep(self, timeout=10):
        """
        保持锁，重设时间
        :param timeout: 锁的超时时间
        :return:
        """
        if redis_cli.ttl(self.lock_name) == -1:  
            redis_cli.expire(self.lock_name, timeout)
        while True:
            if not self.identifier:  # 循环必须在锁激活状态下执行
                break
            if redis_cli.ttl(self.lock_name) < 3:  # 当少于三秒时，开始重设
                redis_cli.expire(self.lock_name, timeout)
            time.sleep(0.01)






