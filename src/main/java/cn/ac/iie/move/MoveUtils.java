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

    /*
     */

    /**
     * move c* data of tbName to newTable
     * @param ksName
     * @param tbName
     * @param expiredSecond
     * @param newTable
     */
    public static void moveCassandraData(String ksName,String tbName,long expiredSecond,
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


}
