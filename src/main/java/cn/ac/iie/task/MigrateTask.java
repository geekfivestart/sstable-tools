package cn.ac.iie.task;

import cn.ac.iie.migrate.MigrateUtils;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * 冷数据迁移任务执行类
 *
 * @author Xiang
 * @date 2016-09-22 14:58
 */
public class MigrateTask implements Job {
    private static final Logger LOG = LoggerFactory.getLogger(MigrateTask.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobDetail job = jobExecutionContext.getJobDetail();
        String jobName = job.getKey().getName();
        LOG.info("Starting migration task[{}]", jobName);
        MigrateUtils.startDoMigrateTask();
        Date nextTime = jobExecutionContext.getNextFireTime();
        LOG.info("Task [{}] completed，and will be executed again on {}", jobName, MigrateUtils.DATE_FORMAT.format(nextTime));
    }
}
