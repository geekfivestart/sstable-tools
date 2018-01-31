package cn.ac.iie.index;

import cn.ac.iie.migrate.MigrateDirectories;
import cn.ac.iie.migrate.MigrateDirectory;
import cn.ac.iie.move.MoveUtils;
import cn.ac.iie.utils.CassandraClient;
import cn.ac.iie.utils.FileUtils;
import com.datastax.driver.core.Row;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static cn.ac.iie.utils.FileUtils.createDirIfNotExists;

/**
 * Created by zhangjc on 2018-1-29.
 */
public class IndexFileHandler {
    private static Logger LOG= LoggerFactory.getLogger(MoveUtils.class);

    public  static final String COL_HOSTNAME                     = "hn";
    public  static final String COL_KEYSPACE                     = "ks";
    public  static final String COL_TABLE                        = "tb";
    public  static final String COL_UUID                         = "uuid";
    public  static final String COL_PATH                         = "path";
    public  static final String COL_TOKEN                        = "tk";
    public  static final String COL_SHARD_ID                     = "sid";
    public  static final String COL_FULL_PATH                    = "fpath";
    public  static final String COL_PART_MIN_RANGE               = "min_pr";
    public  static final String COL_PART_MAX_RANGE               = "max_pr";
    public  static final String COL_MIN_RANGE                    = "min_r";
    public  static final String COL_MAX_RANGE                    = "max_r";
    public  static final String COL_MIN_ADD_RANGE                = "min_ar";
    public  static final String COL_MAX_ADD_RANGE                = "max_ar";
    public  static final String COL_IS_PRIMARY                   = "pm";
    public static void moveIndex(String ip,int port,String host,String ks, String oriTable, String newTable, long moveSince){
        String query = "select * from mpp_schema.mpp_index " + "where ks='" + ks
                + "' and tb='" + oriTable + "' and hn='" + host + "' allow filtering ;";
        LOG.info("QueryOrder:{}", query);
        CassandraClient cassandra = new CassandraClient(ip, port, "cassandra", "cassandra");
        cassandra.ConnectCassandra();
        Iterator<Row> rows = cassandra.queryResult(query);
        // connect cassandra database
        int i = 0;
        LOG.info("moveSince:{}", moveSince);
        String insertCql= String.format("insert into (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ",
                COL_HOSTNAME,COL_KEYSPACE,COL_TABLE,COL_UUID,COL_PATH,
                COL_MIN_RANGE,COL_MAX_RANGE,COL_TOKEN,COL_FULL_PATH,COL_SHARD_ID,
                COL_PART_MIN_RANGE,COL_PART_MAX_RANGE,COL_IS_PRIMARY,COL_MIN_ADD_RANGE,COL_MAX_ADD_RANGE);

        Field[] fields=IndexMeta.class.getDeclaredFields();
        ObjectMapper om=new ObjectMapper();
        try {
            while (true) {
                String fpath;
                long maxPartTime;
                if (rows.hasNext() == false) {
                    break;
                } else {
                    Row row = rows.next();
                    fpath = row.getString("fpath");
                    maxPartTime = row.getLong("max_pr");
                    if (maxPartTime > 0 && maxPartTime < Long.MAX_VALUE - 10) {
                        maxPartTime = maxPartTime / 1000;
                        if(maxPartTime>System.currentTimeMillis()/1000){
                            //   throw new RuntimeException(String.format("Invalid maxParTime %s, which is larger than " +
                            //          "current timestamp %s",maxPartTime,System.currentTimeMillis()/1000));
                        }
                    }else{
                        continue;
                    }

                    if (maxPartTime <= moveSince) {
                        LOG.info("{} {}", maxPartTime, fpath);
                        IndexMeta meta = new IndexMeta();

                        for (Field f : fields) {
                            if (f.getType() == String.class) {
                                f.set(meta, row.getString(f.getName()));
                            } else if (f.getType() == long.class) {
                                if(row.isNull(f.getName())==false)
                                    f.set(meta,row.getLong(f.getName()));
                            } else if (f.getType() == boolean.class) {
                                f.set(meta, row.getBool(f.getName()));
                            } else{
                                throw new Exception(String.format("Invalid data type {} for field:{}",
                                        f.getType().toString(),f.getName()));
                            }
                        }

                        String newFpath=meta.getFpath().replace(meta.getKs()+"/"+meta.getTb(),
                                meta.getKs()+"/"+newTable);
                        String newPath=meta.getPath().replace(meta.getKs()+"/"+meta.getTb(),
                                meta.getKs()+"/"+newTable);


                        meta.setTb(newTable);
                        meta.setFpath(newFpath);
                        meta.setPath(newPath);
                        String insertS="insert into mpp_schema.mpp_index json '"+om.writeValueAsString(meta)+"'";
                        LOG.info("{}",insertS);
                        cassandra.execute(insertS);
                        String deleteSql=String.format("delete from mpp_schema.mpp_index where ks='%s'" +
                                " and tb='%s' and hn='%s' and uuid='%s'",meta.getKs(),oriTable,meta.getHn(),meta.getUuid());
                        LOG.info("{}",deleteSql);
                        cassandra.execute(deleteSql);
                        LOG.info("mkdir -p {}",newFpath);
                        FileUtils.createDirIfNotExists(new File(newFpath));
                        LOG.info("mv {} {}",fpath,newFpath);
                        FileUtils.moveFile(new File(fpath).toPath(),new File(newFpath).toPath());
                    }
                }
            }
        }catch (Exception ex){
            LOG.error(ex.getMessage(),ex);
        }
        cassandra.close();
    }

    public static class IndexMeta{
        public  String hn;
        public  String ks;
        public  String tb;
        public  String uuid;
        public  String path;
        public  long tk;
        public  long sid;
        public  String fpath;
        public  long min_pr;

        public String getHn() {
            return hn;
        }

        public void setHn(String hn) {
            this.hn = hn;
        }

        public String getKs() {
            return ks;
        }

        public void setKs(String ks) {
            this.ks = ks;
        }

        public String getTb() {
            return tb;
        }

        public void setTb(String tb) {
            this.tb = tb;
        }

        public String getUuid() {
            return uuid;
        }

        public void setUuid(String uuid) {
            this.uuid = uuid;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public long getTk() {
            return tk;
        }

        public void setTk(long tk) {
            this.tk = tk;
        }

        public long getSid() {
            return sid;
        }

        public void setSid(long sid) {
            this.sid = sid;
        }

        public String getFpath() {
            return fpath;
        }

        public void setFpath(String fpath) {
            this.fpath = fpath;
        }

        public long getMin_pr() {
            return min_pr;
        }

        public void setMin_pr(long min_pr) {
            this.min_pr = min_pr;
        }

        public long getMax_pr() {
            return max_pr;
        }

        public void setMax_pr(long max_pr) {
            this.max_pr = max_pr;
        }

        public long getMin_r() {
            return min_r;
        }

        public void setMin_r(long min_r) {
            this.min_r = min_r;
        }

        public long getMax_r() {
            return max_r;
        }

        public void setMax_r(long max_r) {
            this.max_r = max_r;
        }

        public long getMin_ar() {
            return min_ar;
        }

        public void setMin_ar(long min_ar) {
            this.min_ar = min_ar;
        }

        public long getMax_ar() {
            return max_ar;
        }

        public void setMax_ar(long max_ar) {
            this.max_ar = max_ar;
        }

        public boolean isPm() {
            return pm;
        }

        public void setPm(boolean pm) {
            this.pm = pm;
        }

        public  long max_pr;
        public  long min_r;
        public  long max_r;
        public  long min_ar;
        public  long max_ar;
        public  boolean pm;
    }


    public static void migrateIndex(String ip,int port,String host,String ks,String table, long moveSince,List<String> destPath){
        if(ip==null||ip.equals("")){
            System.out.println("ip of Catalog is null or empty");
            return;
        }
        if(host==null||host.equals("")){
            System.out.println("host is null or empty");
            return;
        }
        if(ks==null||ks.equals("")){
            System.out.println("keyspace is null or empty");
            return;
        }
        if(table==null||table.equals("")){
            System.out.println("table is null or empty");
            return;
        }
        if(moveSince<0){
            System.out.println("moveSince should be positive number");
            return;
        }
        if(destPath.size()<=0){
            System.out.println("destPath should not be empty");
            return;
        }

        String query = "select * from mpp_schema.mpp_index " + "where ks='" + ks
                + "' and tb='" + table + "' and hn='" + host + "' allow filtering ;";
        LOG.info("QueryOrder:{}", query);
        CassandraClient cassandra = new CassandraClient(ip, port, "cassandra", "cassandra");
        cassandra.ConnectCassandra();
        Iterator<Row> rows = cassandra.queryResult(query);
        // connect cassandra database
        LOG.info("moveSince:{}", moveSince);

        try{
            List<String> list= Lists.newArrayList();
            Map<String,String> map= Maps.newHashMap();
            while(rows.hasNext()){
                Row row=rows.next();
                String fpath=row.getString("fpath");
                long maxPartTime=row.getLong("max_pr");
                if(maxPartTime>0 && maxPartTime<Long.MAX_VALUE-10){
                    maxPartTime=maxPartTime/1000;
                }else{
                    continue;
                }

                if(maxPartTime<moveSince){
                    if(!Files.isSymbolicLink((new File(fpath).toPath()))){
                        map.put(fpath,"");
                    }
                }
            }
            cassandra.close();
            list.addAll(map.keySet());
            LOG.info("{} index file(s) to be migrated!",list.size());
            long st=System.currentTimeMillis();
            final byte [] lock=new byte[0];
            MigrateDirectories migrateDirectories=new MigrateDirectories();
            migrateDirectories.addAllString(destPath);

            Thread [] thread =new Thread[destPath.size()];
            for(int i=0;i<thread.length;++i){
                thread[i]=new Thread(){
                    @Override
                    public void run(){
                        while(true){
                            String file=null;
                            synchronized (lock){
                                if(list.size()>0){
                                    file=list.remove(0);
                                }else{
                                    break;
                                }
                            }
                            long sst=System.currentTimeMillis();
                            doMigrateIndex(ks,table,file,migrateDirectories);
                            LOG.info("{} has been migrated successfully, time consumer:{}ms",
                                    file,System.currentTimeMillis()-sst);
                        }
                    }
                };
                thread[i].start();
            }

            for(Thread t:thread)
                t.join();
            LOG.info("index migrate complete, time consume: {}ms",(System.currentTimeMillis()-st));
        }catch (Exception ex){
            LOG.error(ex.getMessage(),ex);
        }
    }

    /**
     * move lucene index in directory of fileString to one of migrateDirectories in following steps:
     * 1. select one directory from migrateDirectories as the dest directory.
     * 2. copy the entire directory of lucene index to the dest directory.
     * 3. delete the directory of lucene index
     * 4. create symbol link in order to make the original directory of lucene index pointing to the dest directory
     * @param fileString directory of an instance of lucene index
     * @param migrateDirectories
     */
    private static void doMigrateIndex(String ks,String table,String fileString, MigrateDirectories migrateDirectories){
        MigrateDirectory directory=null;
        try {
            // 从待选迁移目标目录中选择一个剩余空间最大的目录作为迁移目标目录
            directory = migrateDirectories.poll(0, 10);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }

        File file=new File(fileString);
        if(directory == null || !FileUtils.noOthersUsing(file)){
            LOG.error("directory is null or file is being used");
        } else {
            String ksTablePattern=ks+"/"+table;
            String tableDir = file.getParentFile().getName();
            String ksDir = file.getParentFile().getParentFile().getName();
            String targetDir = directory.getAbsolutePath() + File.separator +
                    ksDir + File.separator + tableDir;
            int pos=fileString.indexOf(ksTablePattern);
            if(pos<0){
                throw new RuntimeException(fileString+" does not contain pattern "+ksTablePattern);
            }
            targetDir=directory.getAbsolutePath()+File.separator+fileString.substring(pos);
            if(createDirIfNotExists(new File(targetDir))) {
                File [] files=file.listFiles();
                for(File f: files){
                    FileUtils.copyFileLessInfo(f.toPath(),new File(targetDir+File.separator+f.getName()).toPath());
                }
                LOG.info("{} has been copied to {}",fileString,targetDir);
                for(File f:files){
                    FileUtils.deleteFile(f);
                }
                file.delete();
                FileUtils.createSymbolicLink(file.toPath(),new File(targetDir).toPath());
            } else{
                LOG.error("move index failed");
            }
            // 重新将迁移目录放回,
            // 先取出再放回的原因是迁移过后数据目录磁盘空间会发生变化，
            // 因此该迁移目录的排序位置可能发生变化
            migrateDirectories.add(directory);
        }
        return;
    }

    /**
     * merge index file <>indexes</> into base, if base not exists, a new index file named base will be created.
     * delete all <>indexes</> when merge operation completed.
     * @param base
     * @param indexes
     */
    public static void mergIndexes(String base,String ... indexes) throws IOException {
        Directory dirs[]=new Directory[indexes.length];
        int i=0;
        for(String s:indexes){
            File f=new File(s);
            if(!f.exists()){
                throw new RuntimeException(s+" not exist");
            }
            dirs[i++]=FSDirectory.open(new File(s).toPath());
        }
        IndexWriterConfig iwc=new IndexWriterConfig();
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        IndexWriter baseWriter=new IndexWriter(FSDirectory.open(new File(base).toPath()),iwc);
        System.out.println("maxDoc before merge:"+baseWriter.maxDoc());
        baseWriter.addIndexes(dirs);
        baseWriter.commit();
        System.out.println("maxDoc after merge:"+baseWriter.maxDoc());
        baseWriter.close();
        for(String s:indexes)
            FileUtils.deleteDirecotry(new File(s));
    }

    public static void indexSummary(String file){
        try {
            DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(file))) ;
            System.out.println("=======================");
            System.out.println(file);
            System.out.println("Number of document:"+reader.numDocs());
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void tkRange(String file,boolean isCompositeType,int n){
        int count=n;
        long lowbound=Long.MAX_VALUE;
        long upperbound=Long.MIN_VALUE;
        try {
            System.out.println("===============================");
            System.out.println(file);
            DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(file))) ;
            for (int i = 0; i < reader.leaves().size(); i++) {
                LeafReader leafReader = reader.leaves().get(i).reader();
                Bits bits = leafReader.getLiveDocs();
                for (int j = 0; j < leafReader.maxDoc(); j++) {
                    if (bits != null && bits.get(j) == false) {
                        continue;
                    }
                    Document doc=leafReader.document(j);
                    BytesRef br=doc.getBinaryValue("_key");
                    ByteBuffer bbr=ByteBuffer.wrap(br.bytes);
                    ByteBuffer hex=bbr.duplicate();
                    DecoratedKey pk=null;
                    if(isCompositeType){
                        ByteBuffer bb=bbr.duplicate();
                        ByteBuffer tmp= ByteBufferUtil.readBytesWithShortLength(bb);
                        pk=new BufferDecoratedKey(Murmur3Partitioner.instance.getToken(tmp),tmp);
                    }else{
                        pk=new BufferDecoratedKey(Murmur3Partitioner.instance.getToken(bbr),bbr);
                    }
                    long tk=(Long)pk.getToken().getTokenValue();
                    if(count-->0){
                        System.out.println("token:"+tk+" hex:"+ByteBufferUtil.bytesToHex(hex));
                    }

                    if(tk<lowbound){
                        lowbound=tk;
                    }
                    if(tk>upperbound){
                        upperbound=tk;
                    }
                }
            }
            System.out.println("min tk:"+lowbound+"   max tk:"+upperbound);
            reader.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } finally {

        }
    }
}
