package cn.ac.iie.drive;

import cn.ac.iie.migrate.MigrateDirectories;

import java.util.Collection;

/**
 * 操作选项类
 *
 * @author Xiang
 * @date 2016-09-21 09:07
 */
public class Options {
    public final String ksName;
    public final String tbName;

    public final Long expiredSecond;
    public final String ssTablePath;
    public String cronExpression;
    public final MigrateDirectories migrateDirectories = new MigrateDirectories();
    public static Options instance;


    public Options(String ksName, String tbName, Long expiredSecond, String ssTablePath) {
        this.ksName = ksName;
        this.tbName = tbName;
        this.expiredSecond = expiredSecond;
        this.ssTablePath = ssTablePath;
    }

    public static void init(String ksName, String tbName, Long expiredSecond, String ssTablePath){
        instance = new Options(ksName, tbName, expiredSecond, ssTablePath);
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public void setMigratePaths(Collection<String> paths){
        if(paths == null)
            return;
        migrateDirectories.addAllString(paths);
    }
}
