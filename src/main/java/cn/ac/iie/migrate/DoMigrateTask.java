package cn.ac.iie.migrate;

import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * 执行迁移任务类
 *
 * @author Xiang
 * @date 2016-09-23 22:23
 */
public class DoMigrateTask implements Job {
    private static final Logger LOG = LoggerFactory.getLogger(DoMigrateTask.class);
    /**
     * <p>
     * Called by the <code>{@link Scheduler}</code> when a <code>{@link Trigger}</code>
     * fires that is associated with the <code>Job</code>.
     * </p>
     * <p>
     * <p>
     * The implementation may wish to set a
     * {@link JobExecutionContext#setResult(Object) result} object on the
     * {@link JobExecutionContext} before this method exits.  The result itself
     * is meaningless to Quartz, but may be informative to
     * <code>{@link JobListener}s</code> or
     * <code>{@link TriggerListener}s</code> that are watching the job's
     * execution.
     * </p>
     *
     * @param context 上下文
     * @throws JobExecutionException if there is an exception while executing the job.
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String jobName = context.getJobDetail().getKey().getName();

        File ssTable = (File) context.getJobDetail().getJobDataMap().get("sstable");
        int attempt = (Integer) context.getJobDetail().getJobDataMap().get("attempt");
        LOG.info("开始第{}次尝试对{}执行迁移任务：{}", attempt,ssTable.getName(), jobName);
        boolean success = MigrateUtils.doMigrate(ssTable, attempt);
        LOG.info("任务{}，执行结果：{}", jobName, success?"成功":"失败");
    }
}
