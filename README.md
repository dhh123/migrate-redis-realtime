### 使用前必读
> 此脚本是用来迁移redis数据从旧的集群，到新集群,支持多db迁移,并且是实时的（通过monitor回放命令)。使用前请做好测试工作，避免任何可能的损失，本人不负责
特别适合限制了一些关键命令(slaveof)的场景下数据的实时迁移(例如：aws elasticache redis),方便应用切换。

### 脚本使用需求
* Requires Redis 2.8.0 or higher.
* Python requirements:
* click==4.0
* progressbar==2.3
* python-redis==2.10.3

### 使用介绍
./migrate_redis_realtime.py --monitor="`monitor_host:monitor_port`" "`source_host:source_port`" "`dest_host:dest_port`"
可选参数 `--flush` ，表示导入之前先将 dest_host中的数据清空
demo:
> ./migrate_redis_realtime.py --monitor="127.0.0.1:6379" "127.0.0.1:6380" "127.0.0.1:6381"
   解释：实时回放(monitor): 127.0.0.1:6379 新进来的数据，扫描127.0.0.1:6380里面所有db里面的数据,并且导入到127.0.0.1:6381数据库中

### 建议
  monitor redis和源redis使用不同的实例，因为使用同一个实例monitor会消耗源redis一半的性能

### 参考
1. https://gist.github.com/thomasst/afeda8fe80534a832607
2. https://github.com/Qihoo360/pika/tree/master/tools/pika_monitor 
