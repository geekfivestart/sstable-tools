# sstable dump 
将sstable中数据以json形式导出，每条记录以一行json展示

---
### 使用方法
~~~
sstable-tools sstabledump <[-i sstable1,sstable2,..|-f filecontainssstable]> <-k keyname1[,keyname2,...]> [-c ck1[,ck2,...]] 
[-o outputfile] [-s size] [-t tkfile]  [-a] 
~~~

---
### 参数说明
**-i sstable1,sstable2,...**: 
待dump 的sstable 文件，即Data.db文件,多个文件使用逗号分隔

**-f filecontainssstale**
待dump的sstable 文件写在配置文件中，每行一个

**-k keyname1[,keyname2,...]**: 
partition key（分区键） 列名，多个列名以逗号分隔，需按顺序列出. *必选参数*

**-c ck1[,ck2,...]**: 
clustering key列名，多个列名以逗号分隔，需按顺序列出. *如待dump的sstable中包含clustering key,则必需包含此参数*

**-o outputfile**: 
outputfile指示输出文件的前缀，默认为当前目录下dumping. 文件名为outputfile-000000形式，即以6位数值为后缀，输出文件超出设定文件大小后，
以后继数值命名文件继续 .*可选参数*

**-s size**:
size为输出文件的大小，默认值为10MB，size 仅支持m/M/g/G单位，由数值及单位组成，如10m/1g. 当文件
*可选参数*

**-t tkfile**:
tkfile为包含token范围的文件，用以指示导出数据的token范围，即只导出满足指定范围的数据. *可选参数*
~~~
tkfile中，每行指定一个token范围，左右边界值以逗号分隔，如
-7300669385236757835,-7112612670907726572
-6202527875276568255,-6142981862256944334
-162336417000690103,550174580698252731
4717862664701053315,4841313150637601990                         
5719687356401122880,6390118760357509297                         
6691645017097393259,7149342093162380894
~~~

**-a**:
是否将输出内容追加到已有文件中. *可选参数*

**-l**:
选择哪些普通列用于输出，pk,ck默认输出. *可选参数*
---
### 示例
~~~
./sstable-tools sstabledump -i  /data/t_ks/t1-442d7820c3de11edb96743548817ccb3/md-1-big-Data.db 
-o /tmp/t1 -s 1g -k key1,key2 -c key3 -a -t /tmp/tkfile.txt -l col
产生输出文件为 t1-000000
文件内容为
{"key1":"k2","key2":3,"key3":-1,"col":"sec row"}
{"key1":"k1","key2":1,"key3":-1,"col":"firt row"}
~~~