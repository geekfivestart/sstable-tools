package cn.ac.iie.task;

import cn.ac.iie.migrate.MigrateUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.JobListener;
import org.quartz.TriggerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * 废弃ssTable文件清理任务类
 *
 * @author Xiang
 * @date 2016-10-17 19:28
 */
public class CleanupTask implements Job {
    private static final Logger LOG = LoggerFactory.getLogger(CleanupTask.class);
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
     * @param context 任务上下文
     * @throws JobExecutionException if there is an exception while executing the job.
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String jobName = context.getJobDetail().getKey().getName();
        String jobId = UUID.randomUUID().toString();
        LOG.info("开始执行任务:{}-{}", jobName, jobId);
        MigrateUtils.cleanUpMigrateDirs();
        LOG.info("任务执行完毕:{}-{}", jobName, jobId);
    }
}
