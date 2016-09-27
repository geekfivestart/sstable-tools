package cn.ac.iie.migrate;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        LOG.info("开始执行任务{}", jobExecutionContext.getJobDetail().getKey().getName());
        MigrateUtils.startDoMigrateTask();
    }
}
