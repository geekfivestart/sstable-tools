# SSTable Tools

SSTable Tools是用于对mpp-engine系统中数据、索引进行管理的工具，目前项目处于不断的开发中，当前仅在cassandra 3.8 环境中进行测试验证，在其它版本下运行可能出现不可预知情况。SSTable Tools具有如下功能：

* [过期数据分离 (move)](### 1.1)
* [过期索引分离 (moveindex)](#-1.2-过期索引分离)
* [冷数据迁移 (migrate)](### 1.3-冷数据迁移)
* [冷索引迁移 (migrateindex)](#1.4)
* [无效冷数据删除 (cleanup)](#1.5)
* [显示sstable文件元数据 (describe)](#1.6)
* [显示sstable中数据的时间戳范围 (timestamp)](#1.7)
* [显示表在当前节点上的所有sstable文件(sstable)](#1.8)
* [从C* 本地节点读取数据，需要指定十六进制形式的主键 (geth)](#1.9)
* [从C* 本地节点读取数据，需要指定分区列的值，以clustering的列的值 (get)](#1.10)
* [合并索引文件 (mergeindexes)](#1.11)
* [查看索引文件所包含记录的主键范围 (tkrange)](#1.12)
* [查看索引文件信息 (indexinfo)](#1.13)
* [检测索引索引文件token分布 (indextkverify)](#1.14)
* [检查索引文件状态 (checkindex)](#1.15)

## 一、使用方法

    sstable-tools [ move | moveindex | migrate | migrateindex | cleanup |
    describe -f file | timestamp -f file | sstable [-i] -k ksname -t table |
    geth ks tb dk| get ks tb pk [ck1...] | mergeindexes baseindex index1...
    tkrange [-n num] [-c|-s] index1... | indexinfo index1... |
    indexinfo index1...| indextkverify ks table| checkindex [-exorcise] [-crossCheckTermVectors] [-segment X] [-segment Y] [-dir-impl X] indexpath ]

### 1.1 <span id="1.1">过期数据分离</span>
    sstable-tools move
用于将原表中的过期数据移动到新表中，此命令仅移动 cassandra 中数据，并不移动索引文件。
执行此命令前，<font color=red>需要先创建新表，并务必停止运行 mpp-engine 服务，即 nodetool drain && pkill -9 impalad。</font>
需要在配置文件中配置以下参数，参数名与参数间使用冒号分隔，下同。

| 参数名|含义|
|:----------:|:-------:|
|keyspace|待进行数据分离表的 keyspace|
|table|待进行数据分离表名|
|move_since|以秒为单位的时间戳，即对包含数据的最大<br>时间戳小于move_since的sstable进行分离|
|newTable|新表的名称|

### 1.2 过期索引分离
    sstable-tools moveindex
用于将原表中的索引文件移动到新表中，并从元数据中将原表被分离出的索引文件对应的元数据信息删除，将这些索引文件与新表关联起来的元数据写入到元数据服务中。
执行此命令前，<font color=red>需要先创建新表，并务必停止运行 mpp-engine 服务，即 nodetool drain && pkill -9 impalad。</font>
此功能需要在配置文件中配置如下参数。

| 参数名|含义|
|:----------:|:-------:|
|keyspace|待进行数据分离表的 keyspace|
|table|待进行数据分离表名|
|move_since|以秒为单位的时间戳，即对包含数据的最大<br>时间戳小于move_since的sstable进行分离|
|newTable|新表的名称|

注：此命令将修改mpp-engine的元数据服务，元数据服务的ip及端口由 sstable-tools 脚本文件中的 ip 及 port 变量配置。
首次执行此命令前需要进行确认。

### 1.3 冷数据迁移
    sstable-tools migrate
用于将冷数据(一定时间段之前的数据)从原始目录移动到新目录，并在原目录中建立符号链接指向新目录下的文件，这样便可实现将新数据放置在高速磁盘上(如SSD)，冷数据放置到低速磁盘(如机械硬盘)上的目的。
执行此命令前，<font color=red>务必停止运行 mpp-engine 服务，即 nodetool drain && pkill -9 impalad。</font>
此功能需要在配置文件中配置如下参数。

| 参数名|含义|
|:----------:|:-------:|
|keyspace|待进行数据迁移表的 keyspace|
|table|待进行数据迁移表名|
|move_since|以秒为单位的时间戳，即对包含数据的最大<br>时间戳小于move_since的sstable进行迁移|
|migrate_dirs|放置冷数据的目录，每行一个目录，可配置多个|

### <span id="1.4">1.4 冷索引迁移</span>
    sstable-tools migrateindex

用于将冷索引(一定时间段之前的索引)从原始目录移动到新目录，并建立符号链接指向新目录，这样便可实现将新索引放置在高速磁盘上(如SSD)，冷索引放置到低速磁盘(如机械硬盘)上的目的。
执行此命令前，<font color=red>务必停止运行 mpp-engine 服务，即 nodetool drain && pkill -9 impalad。</font>
此功能需要在配置文件中配置如下参数。

| 参数名|含义|
|:----------:|:-------:|
|keyspace|待进行数据迁移表的 keyspace|
|table|待进行数据迁移表名|
|move_since|以秒为单位的时间戳，即对包含数据的最大<br>时间戳小于move_since的索引文件进行迁移|
|migrate_index_dirs|放置冷数据的目录，每行一个目录，可配置多个|

### <span id="1.5">1.5 无效冷数据删除</span>
    sstable-tools cleanup
  cassandra运行过程中，在进行compaction进会删除原有的数据文件，生成新的数据文件。若数据文件已经被
  迁移到冷数据目录后，在compaction时间仅会删除指向冷数据目录的符号链接，并不会删除冷数据，进而产生无用的
  数据文件。因此需要使用此命令删除无效数据文件。
  执行此命令可不用停止服务。
  此功能需要在配置文件中配置如下参数。

| 参数名|含义|
|:----------:|:-------:|
|keyspace|表的 keyspace|
|table|表名|
|migrate_dirs|放置冷数据的目录，每行一个目录，可配置多个|

### <span id="1.6">1.6 显示 sstable 文件的元数据</span>
    sstable-tools describe -f file
其中 file 为要显示的 sstable 文件
示例输出
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
### <span id="1.7">1.7 显示 sstable 中数据的时间戳范围</span>
    sstable-tools timestamp -f file
其中 file 为 sstable 文件
  示例输出
  ```
/data06/cassandra/data/test/resume-59c4b610816611e68f4ef144bf2e9d9f/mb-60-big-Data.db
=====================================================================================
Minimum timestamp: 1474892678232006 (2016-09-26 20:24:38)
Maximum timestamp: 1474892693221025 (2016-09-26 20:24:53)
  ```

### <span id="1.8">1.8 显示表在当前节点上的所有 sstable 文件</span>
    sstable-tools sstable [-i] -k ksname -t table
其中，-i 表示是否显示由符号链接指示的sstable, ksname 为待显示表的 keyspace, table 为待显示表名

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

### <span id="1.9">1.9 从C* 本地节点读取数据（geth）</span>
    sstable-tools geth ks tb dk
从本地数据结点读取数据，ks、tb分别代表 keyspace 及表名，dk为Decorated key的十六进制形式的字条串。此值由索引文件中_key字段得到。

### <span id="1.10">1.10 从C* 本地节点读取数据 (get)</span>
    sstable-tools get ks tb pk [ck1...]
从本地数据结点读取数据，ks、tb分别代表 keyspace 及表名，pk为分区列的值，ck1...等为clustering 列的值。pk及ck均为可读的字符串形式。

### <span id="1.11">1.11 合并索引文件</span>
    sstable-tools mergeindexes baseindex index1 ...
将索引文件index1...等进行合并到baseindex中，若baseindex不存在，则新创建一个索引文件并命名为baseindex。可以指定多个待合并的索引文件，每个索引文件以空间分隔。合并完成后，index1...等将被删除。

### <span id="1.12">1.12 查看索引文件所包含记录的主键范围</span>
    sstable-tools tkrange [-n num] [-c|-s] index1...
用于查看指定文件中所包含主键的token范围。在输出token范围时也可输出索引文件中前若干条记录的token值，此记录数由num指定，num默认为0，即不输出记录的具体token值。 -c及-s用于指示主键是单列组成的简单主键还是复合主键，-c表示复合主键，-s表示简单主键，默认为简单主键。index1...代表索引文件路径，多个文件间以空格分隔。

### <span id="1.13">1.13 查看索引文件信息</span>
    sstable-tools indexinfo index1...
查询索引文件信息，如索引文件记录数，字段数等。 index1...代表索引路径，多个索引间以空格分隔。

### <span id="1.14">1.14 检测索引索引文件token分布</span>
    sstable-tools indextkverify ks table

检测给定ks及table在当前节点上索引文件的token分布情况，对于1）不应落在本节点上的数据及2）token范围计算错误等两种情况，给出告警，并在日志文件中给出处理命令。

### <span id="1.15">1.15 检查索引文件状态</span>
    sstable-tools checkindex [-exorcise] [-crossCheckTermVectors] [-segment X] [-segment Y] [-dir-impl X] indexpath
检测索引文件的状态，对于包含的异常情况进行告警，并可进行挽救性修复。在不使用-exorcise参数运行时，仅打开索引文件，输出版本信息及索引文件包含的异常信息，以及如果使用-exorcise运行时将采取的修复异常动作。

  -exorcise: 移除有问题的段(segment), 并重写segments_N文件，此命令将使异常段内的数据丢失。在使用此参数运行程序时，务必确保没有其它进程在写当前的索引文件。

  -fast:     仅进行checksum校验，忽略逻辑完整性检查。

  -crossCheckTermVectors: 验证词向量(term vector)与posting是否匹配，执行此项检查将严重降低程序运行速度

  -codec X:  在移动无效段后，指定重写segments_N文件所使用的编码器

  -verbose:  在执行过程中输出详细的运行信息

  -segment X:检查指定的段，可使用此参数同时指定多个段，如 '-segment 2 -segment a'。此参数不能与-exorcise同时使用。

  -dir-impl X:使用指定的FSDirectory实现，如果不指定此项参数，默认使用 org.apache.lucene.store


## 二、项目开发及构建
本项目在jdk1.8 + IntelliJ环境下开发，使用[Apache Maven](https://maven.apache.org/)进行构建管理。
首先从github 上 clone 项目到本地

    git clone https://github.com/geekfivestart/sstable-tools

使用如下命令进行构建

```shell
mvn package
```

通过编译后，在 target 目录下 original-sstable-tools-xxx.jar 即为生成的可执行工具，配合下述的 shell 脚本便形成完整的管理工具。

## 三、安装部署
下载并解压本工具的安装包[cassandra-path.tar.gz](https://), 可看到共有 4 个文件，需要将这几个文件分别放置到相应位置上，即可完成工具安装。

|文件|放置位置|
|:----:|:-----:|
|sstable-tools |$IMPALA_HOME/bin|
|original-sstable-tools-xxx.jar | $IMPALA_HOME:/lib|
| migrate.properties :| $IMPALA_HOME:/conf|
| logback-sstable.xml | $IMPALA_HOME:/conf|

注：$IMPALA_HOME为mpp-engine服务的根目录。sstable-tools 为 shell 脚本，用于启动original-sstable-tools-xxx.jar并向其传递参数, original-sstable-tools-xxx.jar为实际进行相应操作的可执行程序，
migrate.properties为包含程序执行所需参数的配置文件， logback-sstable.xml 为工具的日志输出配置文件。

## 四、其它
以下的配置文件供参考


    keyspace: new_wb
    table:t1
    move_since:1505347199
    newTable:t2
    migrate_dirs: /mpp-data/cold/data00
    migrate_dirs: /mpp-data/cold/data01
    migrate_dirs: /mpp-data/cold/data02
    migrate_dirs: /mpp-data/cold/data03

此工具的执行过程中的日志输出到 $IMPALA_HOME/log/目录下的sstable-tools.log中。
