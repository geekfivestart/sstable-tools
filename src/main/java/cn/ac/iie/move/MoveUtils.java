package cn.ac.iie.move;

import cn.ac.iie.cassandra.CassandraUtils;
import cn.ac.iie.drive.Options;
import cn.ac.iie.sstable.SSTableUtils;
import cn.ac.iie.utils.CassandraClient;
import cn.ac.iie.utils.FileUtils;
import com.datastax.driver.core.Row;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by zhangjc on 2017/12/13.
 */
public class MoveUtils {
    private static Logger LOG= LoggerFactory.getLogger(MoveUtils.class);
    private static List<String> postFix=new ArrayList<String>(){
        {
            add("Index.db");
            add("Filter.db");
            add("Summary.db");
            add("Digest.crc32");
            add("CompressionInfo.db");
            add("Statistics.db");
            add("TOC.txt");
        }
    };

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

    public static void startDoMigrateTask(String ksName,String tbName,long expiredSecond,
                                          String newTable){
        try {
            List<File> files = CassandraUtils.getSstableFromTime(
                    ksName,
                    tbName,
                    expiredSecond);
            if(files.size()<=0){
               return;
            }
            files.forEach(file->{
                try {
                    LOG.info("{} MaxTimeInSeconds:{}",file, SSTableUtils.maxTimestamp(file.getAbsolutePath()));
                } catch (IOException e) {
                    LOG.error(e.getMessage());
                }
            });
            int end=files.get(0).getAbsolutePath().lastIndexOf("/");
            int st=files.get(0).getAbsolutePath().lastIndexOf("/",end-1);
            String oriTableStr= files.get(0).getAbsolutePath().substring(st+1,end);
            final String[] newTableStr=new String[1];
            for (File file : new File(files.get(0).getAbsolutePath().substring(0, st)).listFiles()) {
                if(file.isDirectory() && file.getAbsolutePath().substring(file.getAbsolutePath().
                        lastIndexOf("/")+1).startsWith(newTable+"-")){
                    newTableStr[0]=file.getAbsolutePath().substring(file.getAbsolutePath().
                            lastIndexOf("/")+1);
                    break;
                }
            }
            files.forEach(file ->{
                String path=file.getAbsolutePath();
                String newDatafile=path.replace(oriTableStr,newTableStr[0]);
                LOG.error("mv {} {}",path, newDatafile);
                FileUtils.createDirIfNotExists(new File(newDatafile).getParentFile());
                FileUtils.moveFile(new File(path).toPath(),new File(newDatafile).toPath());
                postFix.forEach(postFix->{
                    LOG.error("mv {} {}",path.replace("Data.db",postFix),newDatafile.replace("Data.db",postFix));
                    FileUtils.moveFile(new File(path.replace("Data.db",postFix)).toPath(),
                            new File(newDatafile.replace("Data.db",postFix)).toPath());
                });
            });
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);

            System.exit(-1);
        }
    }

    public static void moveIndex(String ip,int port,String host,String ks, String oriTable, String newTable, long expireTimeInSeconds){
        String query = "select * from mpp_schema.mpp_index " + "where ks='" + ks
                + "' and tb='" + oriTable + "' and hn='" + host + "' allow filtering ;";
        LOG.info("QueryOrder:{}", query);
        CassandraClient cassandra = new CassandraClient(ip, port, "cassandra", "cassandra");
        cassandra.ConnectCassandra();
        Iterator<Row> rows = cassandra.queryResult(query);
        // connect cassandra database
        int i = 0;
        LOG.info("expireTimeInSeconds:{}",expireTimeInSeconds);
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

                    if (maxPartTime <= expireTimeInSeconds) {
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
}
