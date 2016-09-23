package cn.ac.iie.migrate;

import cn.ac.iie.cassandra.CassandraUtils;
import cn.ac.iie.drive.Options;
import com.google.common.collect.Lists;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * 文件迁移工具类
 *
 * @author Xiang
 * @date 2016-09-20 16:58
 */
public class MigrateUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MigrateUtils.class);
    private static final String tmpSuffix = ".tmp";

    /**
     * 设置冷数据迁移任务，并开始执行
     */
    public static void startMigrate(){
        LOG.info("设置定时冷数据迁移任务，执行时间：{}", Options.instance.cronExpression);
        SchedulerFactory factory = new StdSchedulerFactory();
        try {
            Scheduler scheduler = factory.getScheduler();

            JobDetail job =  newJob(MigrateTask.class)
                    .withIdentity("cold-data.migrate.job", "migrate.job")
                    .withDescription("冷数据迁移任务")
                    .build();
            CronTrigger trigger = newTrigger()
                    .withIdentity("cold-data.migrate.trigger", "migrate.trigger")
                    .withSchedule(CronScheduleBuilder.cronSchedule(Options.instance.cronExpression))
                    .build();
            scheduler.scheduleJob(job, trigger);
            scheduler.start();
            while (true){
                if(scheduler.getTriggerState(
                        new TriggerKey("cold-data.migrate.trigger",
                                "migrate.trigger"))
                        == Trigger.TriggerState.COMPLETE){
                    break;
                }
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            LOG.error("冷数据迁移异常退出");
            System.exit(-1);
        }
    }

    /**
     * 执行冷数据迁移<br>
     * 首先获取相关表的所有待迁移sstable文件<br>
     * 然后对各个sstable文件进行迁移，迁移过程如下：<br>
     * 从可迁移磁盘目录中选择一个剩余空间最大的目录作为目标目录D<br>
     * 首先将原sstable SS文件重命名为SS.tmp，若重命名失败则迁移失败；<br>
     * 然后在原sstable SS文件创建软连接LS 指向目录D下的目标文件DS，若创建失败则还原SS.tmp，迁移失败；<br>
     * 最后将临时文件SS.tmp移动至目标目录D，移动后的路径为DS，若移动失败则删除软连接并还原SS.tmp，迁移失败。
     */
    public static void doMigrate(){

        try {
            List<File> files = CassandraUtils.expiredSSTableFromName(Options.instance.ksName,
                    Options.instance.tbName,
                    Options.instance.expiredSecond);
            doMigrate(files, 0, 10);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);

            LOG.error("冷数据迁移异常退出");
            System.exit(-1);
        }
    }

    /**
     * 对sstable文件进行迁移
     * @param files sstable文件列表
     */
    private static void doMigrate(List<File> files, final int attempt, final int maxAttempt){
        List<File> failedFiles = Lists.newArrayList();
        files.forEach(file -> {
            String fileName = file.getName();
//                String parentPath = file.getParentFile().getAbsolutePath();
            MigrateDirectory directory = null;
            try {
                // 从待选迁移目标目录中选择一个剩余空间最大的目录作为迁移目标目录
                directory = Options.instance.migrateDirectories.poll(0, 10);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
                LOG.error("冷数据迁移异常退出");
                System.exit(-1);
            }
            if(directory == null){
                failedFiles.add(file);
            } else {
                File targetFile = new File(directory.getAbsolutePath() + File.separator + fileName);
                boolean migrated = doMigrate(file, targetFile);
                // 若迁移失败，则需要将sstable文件放入失败列表中并尝试重新执行迁移
                if(!migrated){
                    failedFiles.add(file);
                }
                // 重新将迁移目录放回,
                // 先取出再放回的原因是迁移过后数据目录磁盘空间会发生变化，
                // 因此该迁移目录的排序位置可能发生变化
                Options.instance.migrateDirectories.add(directory);
            }
        });
        files.clear();
        // 若有文件迁移失败，则重新对文件进行迁移，
        // 若尝试次数超过最大尝试次数，则不再进行重试
        if(!failedFiles.isEmpty()){
            if(attempt >= maxAttempt){
                LOG.error("迁移尝试次数超过最大尝试次数: {}， 请检查磁盘并尝试重新开始迁移", maxAttempt);
                LOG.error("请检查以下未迁移成功sstable： {}", failedFiles.toString());
            } else {
                doMigrate(failedFiles, attempt + 1, maxAttempt);
            }
        }
    }

    /**
     * 迁移文件并在源文件位置创建软连接<br>
     * 迁移过程如下：<br>
     * 首先将原sstable SS文件重命名为SS.tmp，若重命名失败则迁移失败；<br>
     * 然后在原sstable SS文件创建软连接LS 指向目录D下的目标文件DS，若创建失败则还原SS.tmp，迁移失败；<br>
     * 最后将临时文件SS.tmp移动至目标目录D，移动后的路径为DS，若移动失败则删除软连接并还原SS.tmp，迁移失败。
     *
     * @param sourceFile 原sstable文件
     * @param targetFile 目标sstable文件
     */
    public static boolean doMigrate(File sourceFile, File targetFile){
        String sourceAbsolutePath = sourceFile.getAbsolutePath();
        File tmpFile = new File(sourceFile.getParentFile(), sourceFile.getName()+ tmpSuffix);
        File linkFile = new File(sourceAbsolutePath);
        Path linkPath = linkFile.toPath();
        Path targetPath = targetFile.toPath();
        boolean migrated;
        // FIXME: 2016/09/23 目前的方法还不能保证一定不会造成数据丢失，无法保证数据一致性
        try {
            migrated = sourceFile.renameTo(tmpFile);
        } catch (Exception e){
            LOG.error(e.getMessage(), e);
            migrated = false;
        }

        if (migrated) {
            migrated = createSymbolicLink(linkPath, targetPath);
            if(migrated){
                migrated = doMigrate(tmpFile.toPath(), targetPath);
            }
            // 迁移失败需要还原sstable文件，否则会造成数据丢失
            // FIXME: 2016/09/23 目前还没有一个有效的方法能保证迁移失败后sstable文件一定能够还原成功
            if(!migrated){
                if(linkFile.exists()){
                    //noinspection ResultOfMethodCallIgnored
                    linkFile.delete();
                }
                //noinspection ResultOfMethodCallIgnored
                tmpFile.renameTo(new File(sourceAbsolutePath));
            }
        }
        return migrated;
    }

    /**
     * 迁移文件
     * @param sourcePath 源文件路径
     * @param targetPath 目标文件路径
     */
    public static boolean doMigrate(Path sourcePath, Path targetPath){
        boolean migrated = false;
        try {
            // 将源文件移动至目标文件，同时需要拷贝文件属性
            Files.move(sourcePath, targetPath, REPLACE_EXISTING);
            LOG.info("成功将文件{} 移动至{}", sourcePath.toString(), targetPath.toString());
            migrated = true;
        } catch (IOException e) {
            LOG.error("文件移动出现IO异常，源文件：{}，目标文件：{}", sourcePath.toString(), targetPath.toString());
            LOG.error(e.getMessage(), e);
        } catch (Exception e) {
            LOG.error("文件移动出现其它异常，源文件：{}，目标文件：{}", sourcePath.toString(), targetPath.toString());
            LOG.error(e.getMessage(), e);
        }
        return migrated;
    }

    /**
     * 创建软连接
     * @param linkPath 连接文件路径
     * @param targetPath 目标文件路径
     * @return 返回是否创建成功
     * 当出现IO异常或其它异常时将会创建失败
     */
    public static boolean createSymbolicLink(Path linkPath, Path targetPath){
        boolean created = true;
        try {
            Files.createSymbolicLink(
                    linkPath,
                    targetPath);
            LOG.info("成功创建软连接{}，连接目标：{}", linkPath.toString(), targetPath.toString());
        } catch (IOException e) {
            LOG.error("创建软连接{} 出现IO异常，连接目标：{}", linkPath.toString(), targetPath.toString());
            LOG.error(e.getMessage(), e);
            created = false;
        } catch (Exception e){
            LOG.error("创建软连接{} 出现其它异常，连接目标：{}", linkPath.toString(), targetPath.toString());
            LOG.error(e.getMessage(), e);
            created = false;

        }
        return created;
    }
}
