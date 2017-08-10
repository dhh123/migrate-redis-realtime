#!/usr/bin/python
#coding=utf-8
"""
Copies all keys from the source Redis host to the destination Redis host.
Useful to migrate Redis instances where commands like SLAVEOF and MIGRATE are
restricted (e.g. on Amazon ElastiCache).
The script scans through the keyspace of the given database number and uses
a pipeline of DUMP and RESTORE commands to migrate the keys.
Requires Redis 2.8.0 or higher.
Python requirements:
click==4.0
progressbar==2.3
redis==2.10.3
"""

import click, multiprocessing
from multiprocessing import Queue

from progressbar import ProgressBar
from progressbar.widgets import Percentage, Bar, ETA
import redis, re

class Monitor():
    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
        self.connection = None

    def __del__(self):
        try:
            self.reset()
        except:
            pass

    def reset(self):
        if self.connection:
            self.connection_pool.release(self.connection)
            self.connection = None

    def monitor(self):
        if self.connection is None:
            self.connection = self.connection_pool.get_connection(
                'monitor', None)
        self.connection.send_command("monitor")
        return self.listen()

    def parse_response(self):
        return self.connection.read_response()

    def listen(self):
        while True:
            yield self.parse_response()

def monit_playback(q, sr, sp, dr, dp):
    spool = redis.ConnectionPool(host=sr, port=sp)
    dredis = redis.Redis(host=dr, port=dp)
    monitor = Monitor(spool)
    commands = monitor.monitor()
    g=re.compile(r".*\[(\d+).*\] (.*)")
    for c in commands :
        command_str = re.match(g,c)
        if not command_str:
            continue
        command = command_str.group(2).replace("\"","")
        if not command or str.lower(command).startswith(('ping', 'info', 'replconf', 'mget', 'get', 'dump', 'pttl', 'ttl', 'scan', 'hscan', 'select')):
            continue
        try:
            dredis.execute_command('select ' + command_str.group(1))
            dredis.execute_command(command)
        except:
            q.put(['ERR:',command])

@click.command()
@click.argument('srchost')
@click.argument('dsthost')
#@click.option('--db', default=0, help='Redis db number, default 0')
@click.option('--monitor', default="", help='Redis execute monitor command, Do not choice the "srchost" db(for performance)')
@click.option('--flush', default=False, is_flag=True, help='Delete all keys from destination before migrating')

def migrate(srchost, dsthost, flush, monitor):
    if srchost == dsthost:
        print 'Source and destination must be different.'
        return
    print "Start monitor Process"
    q = Queue()
    monit_host = monitor.split(":")
    shost = srchost.split(":")
    dhost = dsthost.split(":")
    d = multiprocessing.Process(name='monitor', target=monit_playback, args=(q, shost[0], int(shost[1]), dhost[0], int(dhost[1]) ))
    d.daemon = True
    d.start()
    source = redis.Redis(host=shost[0], port=int(shost[1]))
    sinfo = source.info()
    for i in sinfo.keys():
        if re.match("^db(\d)+", i):
            dbnum = int(re.compile(r'(\d+)').search(i).group(0))
            db = i
            print 'start scan db:',db
            source = redis.Redis(host=shost[0], port=int(shost[1]), db=dbnum)
            dest = redis.Redis(host=dhost[0], port=int(dhost[1]), db=dbnum)
            if flush:
                dest.flushdb()

            size = source.dbsize()

            if size == 0:
                print 'No keys found.'
                return

            progress_widgets = ['%s, %d keys: ' % (db, size), Percentage(), ' ', Bar(), ' ', ETA()]
            pbar = ProgressBar(widgets=progress_widgets, maxval=size).start()

            COUNT = 2000 # scan size

            cnt = 0
            non_existing = 0
            already_existing = 0
            cursor = 0

            while True:
                cursor, keys = source.scan(cursor, count=COUNT)
                pipeline = source.pipeline()
                for key in keys:
                    pipeline.pttl(key)
                    pipeline.dump(key)
                result = pipeline.execute()

                pipeline = dest.pipeline()

                for key, ttl, data in zip(keys, result[::2], result[1::2]):
                    if ttl is None:
                        ttl = 0
                    if data != None:
                        pipeline.restore(key, ttl, data)
                    else:
                        non_existing += 1

                results = pipeline.execute(False)
                for key, result in zip(keys, results):
                    if result != 'OK':
                        e = result
                        if hasattr(e, 'message') and (e.message == 'BUSYKEY Target key name already exists.' or e.message == 'Target key name is busy.'):
                            already_existing += 1
                        else:
                            print 'Key failed:', key, `data`, `result`
                            raise e

                if cursor == 0:
                    break

                cnt += len(keys)
                pbar.update(min(size, cnt))

            pbar.finish()
            print 'Keys disappeared on source during scan:', non_existing
            print 'Keys already existing on destination:', already_existing
    while True:
        if not q.empty():
            print q.get()
        else:
            break
    d.join()

if  __name__ == '__main__':
    migrate()
