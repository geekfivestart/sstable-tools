package cn.ac.iie.migrate;

import cn.ac.iie.cassandra.CassandraUtils;
import cn.ac.iie.cassandra.NoSuchKeyspaceException;
import cn.ac.iie.cassandra.NoSuchTableException;
import cn.ac.iie.drive.Options;
import cn.ac.iie.sstable.SSTableUtils;
import cn.ac.iie.task.CleanupTask;
import cn.ac.iie.task.DoMigrateTask;
import cn.ac.iie.task.MigrateTask;
import cn.ac.iie.utils.FileUtils;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static cn.ac.iie.utils.FileUtils.*;
import org.quartz.JobBuilder;
import org.quartz.TriggerBuilder;
import sun.reflect.annotation.ExceptionProxy;

/**
 * 文件迁移工具类
 *
 * @author Xiang
 * @date 2016-09-20 16:58
 */
public class MigrateUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MigrateUtils.class);
    private static final String TMP_SUFFIX = ".tmp";
    private static final String DATE_FORMAT_STR = "yyyy-MM-dd HH:mm:ss";
    private static final String DEFAULT_CRON_EXP = "0 0 1 * * ?";
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT_STR);
    private static final String DO_CLEANUP_JOB = "do.clean.job";
    private static final String DO_MIGRATE_JOB = "do.migrate.job";
    private static final String MAIN_MIGRATE_JOB = "main.migrate.job";
    private static final String MIGRATE_JOB = "migrate.job";
    private static final String CLEANUP_JOB = "cleanup.job";
    private static final String MIGRATE_JOB_SUFFIX = ".migrate.job";
    private static final String CLEANUP_TRIGGER = "cleanup.trigger";
    private static final String MIGRATE_TRIGGER = "migrate.trigger";
    private static final String MIGRATE_TRIGGER_SUFFIX = ".migrate.trigger";
    private static final String DO_CLEANUP_TRIGGER = "do.cleanup.trigger";
    private static final String DO_MIGRATE_TRIGGER = "do.migrate.trigger";
    private static final String MAIN_MIGRATE_TRIGGER = "main.migrate.trigger";
    private static Scheduler scheduler;

    /**
     * 设置冷数据迁移主任务，并开始执行<br/>
     * 主任务的作用主要是获取需要迁移的所有ssTable<br/>
     * 并开启针对单个ssTable的迁移子任务
     */
    public static void startMigrate(){
        LOG.info("Cold migration is arranged，cront exp：{}", Options.instance.cronExpression);
        int diskCount = Options.instance.migrateDirectories.dirCount();
        try {
            Properties p = new Properties();
            // 设置线程池中暑为目标目录数
            // 原因是使并行任务不会超过目标目录个数
            // 事实上，由于目前目录获取机制，当并行任务数大于目标目录个数时
            // 有可能造成某些任务（极有可能是超过目标目录数之后的任务）执行失败，
            // 从而造成不必要的重试次数的增加
            p.setProperty("org.quartz.threadPool.threadCount", diskCount+"");
            p.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
            SchedulerFactory factory = new StdSchedulerFactory(p);
            scheduler = factory.getScheduler();

            // 创建主任务，该任务主要负责创建ssTable迁移子任务
            // 所有迁移操作实际上均在迁移子任务中执行
            JobDetail job = JobBuilder.newJob(MigrateTask.class)
                    .withIdentity(MAIN_MIGRATE_JOB, MIGRATE_JOB)
                    .withDescription("冷数据迁移任务")
                    .build();
            CronTrigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity(MAIN_MIGRATE_TRIGGER, MIGRATE_TRIGGER)
                    .withSchedule(CronScheduleBuilder.cronSchedule(Options.instance.cronExpression))
                    .build();
            Date startTime = scheduler.scheduleJob(job, trigger);
            LOG.info("Migration task will be started on {}", DATE_FORMAT.format(startTime));
            scheduler.start();
            startCleanupTask();
//            while (true){
//                if(scheduler.getTriggerState(
//                        new TriggerKey(MAIN_MIGRATE_TRIGGER,
//                                MIGRATE_TRIGGER))
//                        == Trigger.TriggerState.COMPLETE){
//                    break;
//                }
//                Thread.sleep(5000);
//            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            LOG.error("Data Migration Exception");
            System.exit(-1);
        }
    }

    /**
     * 执行冷数据迁移<br/>
     * 首先获取相关表的所有待迁移sstable文件<br/>
     * 然后对各个sstable文件创建迁移任务并开始执行
     */
    public static void startDoMigrateTask(){
        try {
            int existsTaskCount = scheduler.getJobKeys(GroupMatcher.groupContains(DO_MIGRATE_JOB)).size();
            if(existsTaskCount > 0){
                LOG.warn("{} task(s) running， try next time", existsTaskCount);
                return;
            }
            List<File> files = CassandraUtils.expiredSSTableFromName(
                    Options.instance.ksName,
                    Options.instance.tbName,
                    Options.instance.expiredSecond);

            files.forEach(file-> {
                try {
                    long maxtime = CassandraUtils.maxTimestampOfSSTable(file.getAbsolutePath());
                    LOG.info("{} {}({})", file.getAbsolutePath(),maxtime, SSTableUtils.toDateString(maxtime,
                            TimeUnit.MICROSECONDS, false));
                }catch(Exception ex){
                }
            });
            LOG.info("{} file(s) to be migrated", files.size());
            // 对每个文件创建迁移任务，并在5秒钟后开始执行
            files.forEach(file -> startDoMigrateTask(file, 0, 5));

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);

            System.exit(-1);
        }
    }

    /**
     * 对一个ssTable文件创建迁移任务
     * @param ssTable ssTable文件
     * @param attempt 标识该任务为第几次执行
     * @param delaySecond 延迟秒数，标识该任务将延迟多少秒后执行
     */
    private static void startDoMigrateTask(File ssTable, final int attempt, int delaySecond){
        Date startTime;
        // 调用DateBuilder获取延迟后的时间；
        // 默认延迟为5秒
        // 此处设置延迟主要是为了确保任务能够按预定延迟时间创建并执行，
        // 若任务时间设置为当前执行，则任务可能无法执行，因此默认延迟设置为5秒
        startTime = DateBuilder.futureDate(
                delaySecond > 5? delaySecond: 5,
                DateBuilder.IntervalUnit.SECOND);
        String jobName = ssTable.getName()+".NO."+attempt;
        JobDetail job = JobBuilder.newJob(DoMigrateTask.class)
                .withIdentity(jobName + MIGRATE_JOB_SUFFIX, DO_MIGRATE_JOB)
                .withDescription("冷数据迁移任务")
                .build();
        // 执行任务过程中需要知道ssTable文件以确定ssTable位置；
        // 知道尝试次数以便任务失败后判断是否重新执行
        job.getJobDataMap().put("sstable", ssTable);
        job.getJobDataMap().put("attempt", attempt);
        // 由于该任务不是定时任务，因此只需要设置一个
        // 简单触发器在到达延迟时间时刻执行一次即可
        SimpleTrigger trigger = (SimpleTrigger) TriggerBuilder.newTrigger()
                .withIdentity(jobName + MIGRATE_TRIGGER_SUFFIX, DO_MIGRATE_TRIGGER)
                .startAt(startTime)
                .build();

        try {
            startTime = scheduler.scheduleJob(job, trigger);
            LOG.info("Migration of sstable（{}）will be started on {} for the {}nd/th time",

                    ssTable.getName(),
                    DATE_FORMAT.format(startTime),
                    attempt+1);
        } catch (SchedulerException e) {
            LOG.error(e.getMessage(), e);
            // 若任务执行出现异常，则有可能系统出现问题，
            // 因此，此处暂时选择退出程序
            System.exit(-1);
        }
    }

    /**
     * 对sstable文件进行迁移
     * @param file sstable文件
     * @param attempt 尝试次数
     */
    public static boolean doMigrate(File file, final int attempt){
        List<File> failedFiles = Lists.newArrayList();
        String fileName = file.getName();
//                String parentPath = file.getParentFile().getAbsolutePath();
        MigrateDirectory directory = null;
        boolean migrated;
        try {
            // 从待选迁移目标目录中选择一个剩余空间最大的目录作为迁移目标目录
            directory = Options.instance.migrateDirectories.poll(0, 10);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
//                System.exit(-1);
        }
        // 若未能从列表中取出目标路径或者ssTable被cassandra占用，
        // 则直接设置迁移失败，等待下一次重新执行迁移任务
        // 未能取出目标路径说明其它任务正在使用全部路径，暂时无可用目标目录可用，
        // 等待其它任一任务执行完毕即可
        // ssTable文件被其它进程占用，则说明cassandra进程极有可能正在对该ssTable进行读或写操作，
        // 因此需要等待cassandra完成操作后再执行任务
        if(directory == null || !FileUtils.noOthersUsing(file)){
            migrated = false;
        } else {
            String tableDir = file.getParentFile().getName();
            String ksDir = file.getParentFile().getParentFile().getName();
            String targetDir = directory.getAbsolutePath() + File.separator +
                    ksDir + File.separator + tableDir;
            if(createDirIfNotExists(new File(targetDir))) {
                File targetFile = new File(targetDir + File.separator + fileName);

                migrated = doMigrate(file, targetFile);
            } else{
                migrated = false;
            }
            // 重新将迁移目录放回,
            // 先取出再放回的原因是迁移过后数据目录磁盘空间会发生变化，
            // 因此该迁移目录的排序位置可能发生变化
            Options.instance.migrateDirectories.add(directory);
        }
        // 若有文件迁移失败，则重新对文件进行迁移，
        // 新的任务将在5分钟以后执行
        // 若尝试次数超过最大尝试次数，则不再进行重试
        if(!migrated){
            if(attempt >= Options.instance.maxMigrateAttemptTimes){
                LOG.error("Migration task run {} times, still not success",
                        Options.instance.maxMigrateAttemptTimes);
                LOG.error("Migration task run %s times, still not success", failedFiles.toString());
            } else {
                startDoMigrateTask(file, attempt + 1, 300);
            }
        }
        return migrated;
    }

    /**
     * 迁移文件并在源文件位置创建软连接<br/>
     * 迁移过程如下：<br/>
     * 首先将原sstable SS文件复制到目标目录，路径为DS，若复制失败则迁移失败<br/>
     * 然后将原sstable SS文件重命名为SS.tmp，若重命名失败则删除DS，迁移失败；<br/>
     * 然后在原sstable SS文件创建软连接LS 指向目录D下的目标文件DS，若创建失败则还原SS.tmp并删除DS，迁移失败。
     *
     * @param sourceFile 原sstable文件
     * @param targetFile 目标sstable文件
     */
    public static boolean doMigrate(File sourceFile, File targetFile){
        String sourceAbsolutePath = sourceFile.getAbsolutePath();
        File tmpFile = new File(sourceFile.getParentFile(), sourceFile.getName()+ TMP_SUFFIX);
        File linkFile = new File(sourceAbsolutePath);
        Path linkPath = linkFile.toPath();
        Path targetPath = targetFile.toPath();
        Path sourcePath = sourceFile.toPath();
        // FIXME: 2016/09/23 目前的方法还不能保证一定不会造成数据丢失，无法保证数据一致性
        boolean migrated = copyFile(sourcePath, targetPath);


        if(migrated) {
            // 仅当文件不被占用且目标文件与原文件相同时才进行下一步操作
            // 由于ssTable 的性质，若cassandra对ssTable进行的操作只能是追加内容或者删除文件，
            // 因此，若原文件与目标文件大小不一致则说明原文件发生了变化；
            // 反之，则说明原文件未发生任何变化
            if(FileUtils.noOthersUsing(sourceFile)
                    && targetFile.length() > 0
                    && FileUtils.isSameLength(sourceFile, targetFile)) {
                // 重命名原ssTable文件
                try {
                    migrated = sourceFile.renameTo(tmpFile);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                    migrated = false;
                }

                if (!migrated) {
                    if (!deleteFile(targetFile)) {
                        LOG.warn("Migration failed，dest file cannot be deleted：{}", targetFile.getAbsolutePath());
                    }
                    return false;
                }


                migrated = createSymbolicLink(linkPath, targetPath);
            } else{
                migrated = false;
            }
            if (migrated) {
                if(!deleteFile(tmpFile)){
                    LOG.warn("Migration success，tmp file deleted failed：{}", tmpFile.getAbsolutePath());
                }
            }
            else{
                // 迁移失败需要还原sstable文件，否则会造成数据丢失
                // FIXME: 2016/09/23 目前还没有一个有效的方法能保证迁移失败后sstable文件一定能够还原成功
                //noinspection ResultOfMethodCallIgnored
                tmpFile.renameTo(new File(sourceAbsolutePath));
                if(!deleteFile(targetFile)){
                    LOG.warn("Migration failed，dst file created but delete failed：{}", targetFile.getAbsolutePath());
                }
            }
        }
        return migrated;
    }


    /**
     * 将迁移文件还原至原来的位置
     * @param ssTablePath ssTable路径，该路径应为软连接路径而非软连接指向的真实文件路径
     */
    public static void restore(String ssTablePath){
        File ssTable = new File(ssTablePath);
        String absolutePath = ssTable.getAbsolutePath();
        if(!ssTable.exists()){
            LOG.error("file {} not exist", absolutePath);
            System.err.printf("file %s not exist", absolutePath);
            System.exit(1);
        }
        if(!Files.isSymbolicLink(ssTable.toPath())){
            LOG.error("File {} is not symbol link，not need to restore", absolutePath);
            System.err.printf("File %s is not symbol link，not need to restore", absolutePath);
            System.exit(1);
        }
        Path targetPath;
        try {
            targetPath = Files.readSymbolicLink(ssTable.toPath());
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
            return;
        }
        String oriDir;
        try {
            oriDir = ssTable.getParentFile().getCanonicalPath();
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            System.exit(1);
            return;
        }
        String ssTableName = ssTable.getName();
        doRestore(targetPath, ssTableName, oriDir);
    }

    /**
     * 执行文件还原，执行过程与迁移过程相反：<br/>
     * 1、将ssTable文件复制到cassandra表空间目录下的临时文件T<br/>
     * 2、将软连接文件删除<br/>
     * 3、将临时文件T重命名为软连接文件<br/>
     * 4、将迁移文件删除
     * @param linkedPath 迁移文件路径
     * @param ssTableName ssTable名称
     * @param oriDir ssTable目录，即表空间目录
     */
    public static void doRestore(Path linkedPath, String ssTableName, String oriDir){
        File tmp = new File(oriDir + File.separator + ssTableName + TMP_SUFFIX);
        File ssTable = new File(oriDir + File.separator + ssTableName);
        boolean copied = copyFile(linkedPath, tmp.toPath());
        if(copied){
            boolean deleted = deleteFile(ssTable);
            if (deleted){
                if(tmp.renameTo(ssTable)){
                    if(deleteFile(linkedPath.toFile())){
                        LOG.info("File {} has been restored to {}",
                                linkedPath.toString(), ssTable.getAbsolutePath());
                        System.out.printf("File %s has been restored to %s",
                                linkedPath.toString(), ssTable.getAbsolutePath());
                    } else{
                        LOG.warn("File {} has been restored to {},but src file deleted failed",
                                linkedPath.toString(), ssTable.getAbsolutePath());
                        System.out.printf("File %s has been restored to %s,but src file deleted failed",
                                linkedPath.toString(), ssTable.getAbsolutePath());
                    }
                } else{
                    LOG.error("restore file failed，since rename tmp file {} to {} failed",
                            tmp.getAbsolutePath(), ssTable.getAbsolutePath());
                    System.err.printf("restore file failed，since rename tmp file %s to %s failed",
                            tmp.getAbsolutePath(), ssTable.getAbsolutePath());
                    if(!createSymbolicLink(ssTable.toPath(), linkedPath)){
                        LOG.warn("restore symbol link failed，please execute shell command manually：ln -s {} {}",
                                ssTable.getAbsolutePath(), linkedPath.toString());
                        System.err.printf("restore symbol link failed，please execute shell command manually：ln -s %s %s",
                                ssTable.getAbsolutePath(), linkedPath.toString());
                    }
                    if(!deleteFile(tmp)){
                        LOG.warn("delete tmp file {} failed，please execute shell command manually：rm -rf {}",
                                tmp.getAbsolutePath(), tmp.getAbsolutePath());
                        System.err.printf("delete tmp file %s failed，please execute shell command manually：rm -rf %s",
                                tmp.getAbsolutePath(), tmp.getAbsolutePath());
                    }
                }
            } else {
                System.err.printf("restore failed，since file {} cannot be deleted", ssTable.getAbsolutePath());
                LOG.error("restore failed，since file %s cannot be deleted", ssTable.getAbsolutePath());
                if(!deleteFile(tmp)){
                    System.err.printf("restore failed，tmp file {} has been created, but deleted failed", tmp.getAbsolutePath());
                    LOG.warn("restore failed，tmp file %s has been created, but deleted failed", tmp.getAbsolutePath());
                }
            }
        } else{
            System.err.printf("Copying %s to %s failed", linkedPath.toString(), tmp.getAbsolutePath());
        }
    }

    private static String join(String... s){
        return StringUtils.join(s, File.separator);
    }

    private static void startCleanupTask(){
        JobDetail job = JobBuilder.newJob(CleanupTask.class)
                .withIdentity(DO_CLEANUP_JOB, CLEANUP_JOB)
                .withDescription("废弃ssTable清理任务")
                .build();
        CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(DO_CLEANUP_TRIGGER, CLEANUP_TRIGGER)
                .withSchedule(CronScheduleBuilder.cronSchedule(DEFAULT_CRON_EXP))
                .build();
        Date startTime;
        try {
            startTime = scheduler.scheduleJob(job, trigger);
            LOG.info("The task of cleanning discard ssTable will be started at {}",
                    DATE_FORMAT.format(startTime));
        } catch (SchedulerException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * 清理迁移目录，删除无用ssTable文件<br/>
     * 由于cassandra执行compact、删除或数据过期等原因，
     * 可能会导致cassandra将ssTable文件删除，若此前该文件执行过
     * 迁移操作则cassandra仅将软连接文件删除而不会删除真正的ssTable数据文件，
     * 从而产生废弃ssTable文件。<br/>
     * 因此，需要执行清理操作以删除废弃ssTable文件
     */
    public static void cleanUpMigrateDirs(){
        final String tblId;
        List<File> ssTableList;
        try {
            //获取某个表的uuid用于构造相应的目录路径
            tblId = CassandraUtils.getTableUid(Options.instance.ksName, Options.instance.tbName);
            //获取当前所有的ssTable文件用于检测迁移目录下的文件是否已被删除
            ssTableList = CassandraUtils.ssTableFromName(Options.instance.ksName, Options.instance.tbName, true);
        } catch (NoSuchKeyspaceException | NoSuchTableException e) {
            LOG.error(e.getMessage(), e);
            return;
        }
        Set<String> migrateDirs = new HashSet<>(Options.instance.migrateDirectories.getAllDir());
        Map<String, File> migratedSsTableFileMap = new HashMap<>();
        /*
        获取迁移目录中相关表目录下的所有ssTable数据文件
        由于cassandra本身的ssTable文件命名机制，统一集群中的不同ssTable（
        无论是在不同节点上、在同一节点上的不同数据目录下还是在统一节点上的同一目录下）
        拥有不同的编号，因此迁移目录下的ssTable名称必然不会重复，
        故而可以使用ssTable名称作为HashMap的key。
         */
        migrateDirs.forEach(dir -> {
            String migratePath = join(dir, Options.instance.ksName, tblId);
            File migrateDir = new File(migratePath);
            if(migrateDir.exists() && migrateDir.isDirectory()){
                try {
                    File[] files = migrateDir.listFiles();
                    if(files != null) {
                        for (File file : files) {
                            migratedSsTableFileMap.put(file.getName(), file);
                        }
                    }
                } catch (Exception e){
                    LOG.error(e.getMessage(), e);
                }
            }
        });

        doCleanup(migratedSsTableFileMap, ssTableList);
    }

    /**
     * 执行清理操作，删除已废弃的ssTable数据文件。<br/>
     * 清理过程如下：<br/>
     * 1、获取迁移目录下的ssTable名称集合<code>A</code><br/>
     * 2、在<code>A</code>中删除所有在线的ssTable名称，则<code>A</code>中将只剩下离线的ssTable<br/>
     * 3、遍历A中的ssTable名称，从<code>migratedSsTableFileMap</code>获取相应的离线ssTable数据文件并删除<br/>
     * 由于cassandra本身的ssTable文件命名机制，统一集群中的不同ssTable（
     * 无论是在不同节点上、在同一节点上的不同数据目录下还是在统一节点上的同一目录下）
     * 拥有不同的编号，因此迁移目录下的ssTable名称必然不会重复。
     * @param migratedSsTableFileMap 迁移目录下所有ssTable名称与ssTable文件映射
     * @param ssTableList 在线的所有ssTable文件列表
     */
    public static void doCleanup(Map<String, File> migratedSsTableFileMap, List<File> ssTableList){
        Set<String> ssTableSet = migratedSsTableFileMap.keySet();
        ssTableList.forEach(onlineSsTable -> ssTableSet.remove(onlineSsTable.getName()));
        ssTableSet.forEach(offlineSsTable -> deleteFile(migratedSsTableFileMap.get(offlineSsTable)));
    }

}
