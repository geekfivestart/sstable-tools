package cn.ac.iie.migrate;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * 冷数据迁移任务执行类
 *
 * @author Xiang
 * @date 2016-09-22 14:58
 */
public class MigrateTask implements Job {

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        MigrateUtils.startDoMigrateTask();
    }
}
