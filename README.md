# SSTable Tools

一个用于解析、查看 Cassandra 3.x SSTables文件的工具。同时以第三方工具补丁的形式提供cassandra冷数据迁移的功能。目前该项目还在开发测试过程中，冷数据迁移功能还在大规模数据测试中。

预编译的cassandra冷数据迁移补丁:

* [cassandra-patch.tar.gz](https://github.com/xiangt920/sstable-tools/releases/download/v3.7.0-migrate/cassandra-patch.tar.gz) -  当前支持Cassandra 3.7

示例:

    java -jar sstable-tools.jar describe -f ma-2-big-Data.db
    java -jar sstable-tools.jar timestamp -f ma-2-big-Data.db
    sstable-tools sstable -k keyspace -t table
    sstable-tools migrate -k keyspace -t table -c "0 0 2 * * ?" -e 1000 -m 10 /data00 /data01

**注意:** describe 与 timestamp无需进行环境配置即可运行，sstable 与 migrate命令需要以补丁的形式放入cassandra安装目录下执行，或者自己设置相关环境变量（详见[stable-tools脚本文件](https://github.com/xiangt920/sstable-tools/tree/master/bin/sstable-tools)）也可。

**特性:**

* [describe](#describe) - 查看sstable中的数据与元数据；
* [timestamp](#timestamp) - 查看sstable中的数据的时间戳范围；
* [sstable](#sstable) - 查看指定的表在本节点的所有sstable文件；
* [migrate](#migrate) - 执行冷数据迁移。

## 构建

本项目使用 [Apache Maven](https://maven.apache.org/) 来构建一个
可执行jar包。  构建该jar包可执行以下命令:

```shell
mvn package
```

可执行jar包将在target目录下生成。

## describe

打印与指定sstable相关的数据与元数据信息。 

示例输出:

```
/Users/clohfink/git/sstable-tools/ma-119-big-Data.db
====================================================
Partitions: 32162                                                               
Rows: 32162
Tombstones: 0
Cells: 353782
Widest Partitions:
   [7339364] 1
   [7153250] 1
   [7216142] 1
   [7043886] 1
   [7687007] 1
Largest Partitions:
   [7445112] 3418 (3.4 kB)
   [7015610] 3278 (3.3 kB)
   [7290631] 3109 (3.1 kB)
   [7043285] 2808 (2.8 kB)
   [7728519] 2788 (2.8 kB)
Tombstone Leaders:
Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
Bloom Filter FP chance: 0.010000
Size: 30920990 (30.9 MB) 
Compressor: org.apache.cassandra.io.compress.LZ4Compressor
  Compression ratio: 0.6600443582175085
Minimum timestamp: 1474892678232006 (2016-09-26 20:24:38)
Maximum timestamp: 1474892693221025 (2016-09-26 20:24:53)
SSTable min local deletion time: 2147483647 (2038-01-19 11:14:07)
SSTable max local deletion time: 2147483647 (2038-01-19 11:14:07)
TTL min: 0 (0 milliseconds)
TTL max: 0 (0 milliseconds)
minClustringValues: []
maxClustringValues: []
Estimated droppable tombstones: 0.0
SSTable Level: 0
Repaired at: 0 (1970-01-01 08:00:00)
  Lower bound: ReplayPosition(segmentId=1474890699224, position=4007)
  Upper bound: ReplayPosition(segmentId=1474890699229, position=29647053)
totalColumnsSet: 353782
totalRows: 32162
Estimated tombstone drop times:
  Value                            | Count    %   Histogram 
  2147483647 (2038-01-19 11:14:07) | 385944 (100) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 
Estimated partition size:
  Value | Count   %   Histogram 
  179   |     1 (  0)  
  215   |     6 (  0)  
  258   |    66 (  0) ▎ 
  310   |   260 (  0) ▉▎ 
  372   |   713 (  2) ▉▉▉▋ 
  446   |  1562 (  4) ▉▉▉▉▉▉▉▉ 
  535   |  2246 (  6) ▉▉▉▉▉▉▉▉▉▉▉▍ 
  642   |  2902 (  9) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▊ 
  770   |  3470 ( 10) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▊ 
  924   |  4100 ( 12) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 
  1109  |  4929 ( 15) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▏ 
  1331  |  5861 ( 18) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 
  1597  |  4971 ( 15) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▍ 
  1916  |  1021 (  3) ▉▉▉▉▉▏ 
  2299  |    44 (  0) ▏ 
  2759  |     7 (  0)  
  3311  |     3 (  0)  
Estimated column count:
  Value | Count   %   Histogram 
  12    | 32162 (100) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 
Estimated cardinality: 32002
EncodingStats minTTL: 0 (0 milliseconds)
EncodingStats minLocalDeletionTime: 1442880000 (1970-01-18 00:48:00)
EncodingStats minTimestamp: 1474892678228003 (2016-09-26 20:24:38)
KeyType: org.apache.cassandra.db.marshal.LongType
ClusteringTypes: []
StaticColumns: {}
RegularColumns: {
    f6:org.apache.cassandra.db.marshal.UTF8Type, 
    f7:org.apache.cassandra.db.marshal.UTF8Type, 
    f8:org.apache.cassandra.db.marshal.UTF8Type, 
    f9:org.apache.cassandra.db.marshal.UTF8Type, 
    f10:org.apache.cassandra.db.marshal.UTF8Type, 
    f11:org.apache.cassandra.db.marshal.UTF8Type, 
    f1:org.apache.cassandra.db.marshal.UTF8Type, 
    f2:org.apache.cassandra.db.marshal.UTF8Type, 
    f3:org.apache.cassandra.db.marshal.UTF8Type, 
    f4:org.apache.cassandra.db.marshal.UTF8Type, 
    f5:org.apache.cassandra.db.marshal.UTF8Type
}
```

### 用法

```
java -jar sstable-tools.jar describe -f /path/to/file.db
```

## timestamp

输出指定sstable中包含数据的时间戳范围

示例输出

```
/data06/cassandra/data/test/resume-59c4b610816611e68f4ef144bf2e9d9f/mb-60-big-Data.db
=====================================================================================
Minimum timestamp: 1474892678232006 (2016-09-26 20:24:38)
Maximum timestamp: 1474892693221025 (2016-09-26 20:24:53)
```

### 用法

```
java -jar sstable-tools.jar timestamp -f /path/to/file.db
```

## sstable

输出指定表在本节点的所有sstable文件路径信息

示例输出

```
是否包含软连接：true
 SSTables for keyspace: test, table: resume
===========================================
/data01/cassandra/data/test/resume-59c4b610816611e68f4ef144bf2e9d9f/mb-1-big-Data.db isSymbolicLink:true
/data01/cassandra/data/test/resume-59c4b610816611e68f4ef144bf2e9d9f/mb-13-big-Data.db isSymbolicLink:true
/data01/cassandra/data/test/resume-59c4b610816611e68f4ef144bf2e9d9f/mb-19-big-Data.db isSymbolicLink:true
/data01/cassandra/data/test/resume-59c4b610816611e68f4ef144bf2e9d9f/mb-37-big-Data.db isSymbolicLink:true
/data01/cassandra/data/test/resume-59c4b610816611e68f4ef144bf2e9d9f/mb-43-big-Data.db isSymbolicLink:true
/data01/cassandra/data/test/resume-59c4b610816611e68f4ef144bf2e9d9f/mb-49-big-Data.db isSymbolicLink:true
/data01/cassandra/data/test/resume-59c4b610816611e68f4ef144bf2e9d9f/mb-55-big-Data.db isSymbolicLink:true
/data01/cassandra/data/test/resume-59c4b610816611e68f4ef144bf2e9d9f/mb-7-big-Data.db isSymbolicLink:true
--finished--
```

### 用法

```
sstable-tools sstable -i -k test -t resume
```

## migrate

冷数据（与热数据相对应，即基本不再访问或极少访问的数据）迁移工具。

### 目的

开发该工具的主要目的是在保证cassandra写入速度的前提下尽量增大节点数据容量并降低硬件成本。

### 工作方式

指定数据冷化时间，当数据冷化后，将数据从ssd磁盘迁移至hdd磁盘，即从高速磁盘迁移至低速磁盘。


### 用法

```
名称
        sstable-tools migrate - 冷数据迁移

简介
        sstable-tools migrate [(-c <定时表达式> | --cron_expression <定时表达式>)]
                [(-e <冷化时间> | --expired_second <冷化时间>)] [(-k <ks名> | --keyspace <ks名>)]
                [(-m <最大尝试次数> | --maxAttempt <最大尝试次数>)] [(-t <表名> | --table <表名>)] [--]
                <目录1> <目录2> ...

选项
        -c <定时表达式>, --cron_expression <定时表达式>
            quartz定时表达式

        -e <冷化时间>, --expired_second <冷化时间>
            以“秒”为单位的数据过期时间，该时间过后的数据为冷数据

        -k <ks名>, --keyspace <ks名>
            keyspace名称

        -m <最大尝试次数>, --maxAttempt <最大尝试次数>
            最大迁移尝试次数，默认为10

        -t <表名>, --table <表名>
            table名称

        --
            该选项用户分隔选项列表

        <目录1> <目录2> ...
            迁移目标目录列表，可设置多个
```

### quartz定时表达式

该工具使用quartz管理定时任务，因此使用[cron定时表达式](http://www.quartz-scheduler.org/documentation/quartz-2.2.x/tutorials/tutorial-lesson-06.html)来启动定时任务