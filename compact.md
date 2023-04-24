# compact 
将cassandra 产生的sstable文件进行合并，合并完成后在输出目录产生一个新的sstable文件，原始的sstable文件会被删除。**勿直接在线上cassandra的数据目内合并sstable,为安全考虑需要先待合并的sstable复制到其它的目录，再执行合并操作。**

---
### 使用方法
~~~
sstable-tools compact -k ks -t tb  [-i sst1,sst2...,sst3] [-f sstablefile] [-o outputdir] [-b batchsize] [-c threadnum] 
~~~


---
### 参数说明

**-k ks**: 
ks 表示待合并数据文件所对应的keyspace 名. *必填参数*

**-t tb**:
tb 表示待合并数据文件所对应的表名. *必填参数*

**-i sst1,sst2,...**: 
待合并的sstable 文件（完整的绝对路径），即Data.db文件,多个文件使用逗号分隔

**-f sstablefile**
待合并的sstable 文件写在配置文件中，每行一个，例如
~~~
/tmp/ksname/tablename/md-256-big-Data.db
/tmp/ksname/tablename/md-259-big-Data.db
/tmp/ksname/tablename/md-263-big-Data.db
~~~
需要注意的是，sstable所在目录的上两级、一级名称，必须为ks名、表名，否则执行会报错

**-o outputfile**: 
outputfile指示合并产生文件放置的目录，生成的ssable会放置在outputfile目录下的ksname/tablename目录内 .*可选参数*

**-b batchsize**:
batch的大小，即多少个文件合并生成一个文件，默认值为1000. *可选参数*

**-c threadnum**: 
执行合并的线程数量， 默认为1个线程，每个线程单次执行一个batch的合并操作. *可选参数*

---
### 示例
~~~
sstable-tools compact -k medical_db -t doctorinfo_data_amazon_nocompact -f /tmp/cmptfiles -o /tmp/compactout/ -b 4 -c 2

~~~
---
### 注意事项
1、compact操作实际由名为UserDefinedCompact的java进程执行，合并操作执行完成后，该进程并不会退出。因此，不能通过UserDefinedCompact进程是否还存在来判断合并操作是否完成。
2、判断合并操作是否完成需要看待合并的数据文件是否都已被删除，全被删除说明合并操作完成。
3、该合并操作会写compaction_history、sstable_activity 两个表，为防止与线上运行系统冲突，在sstable-tools脚本中，通过-Dcassandra.config=file:////tmp/conf/cassandra.yaml 来指定需要指定另一个配置文件，在该配置文件中，data_file_directory需要指定不同的目录，并将线上系统的system  system_auth  system_distributed  system_schema  system_traces 这几个ks复制到/tmp/conf/cassandra.yaml指定数据目录内.