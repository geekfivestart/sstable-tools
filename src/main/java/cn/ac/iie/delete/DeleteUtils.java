package cn.ac.iie.delete;

import cn.ac.iie.cassandra.CassandraUtils;
import cn.ac.iie.move.MoveUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Created by zhangjc on 2018-4-6.
 */
public class DeleteUtils {
    private static Logger LOG= LoggerFactory.getLogger(DeleteUtils.class);
    public static void deleteData(String ks,String table,long expire){
        try {
            List<File> list=CassandraUtils.getSstableFromTime(ks,table,expire);
            list.forEach(file -> {
                LOG.info("rm "+file.getAbsolutePath().replace("Data.db","*"));
            });
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage(),e);
        }
    }
}
