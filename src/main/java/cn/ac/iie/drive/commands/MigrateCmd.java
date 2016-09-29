package cn.ac.iie.drive.commands;

import cn.ac.iie.drive.Options;
import cn.ac.iie.drive.commands.base.ClusterTableCmd;
import cn.ac.iie.migrate.MigrateUtils;
import com.google.common.collect.Lists;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.io.File;
import java.util.List;

/**
 * 冷数据迁移命令：migrate
 *
 * @author Xiang
 * @date 2016-09-22 14:11
 */
@Command(name = "migrate", description = "冷数据迁移")
public class MigrateCmd extends ClusterTableCmd {

    @Option(name = {"-e", "--expired_second"},
            title = "冷化时间",
            description = "以“秒”为单位的数据过期时间，该时间过后的数据为冷数据")
    private Long expiredSecond = 0L;

    @Option(name = {"-c", "--cron_expression"},
            title = "定时表达式",
            description = "quartz定时表达式，详见http://www.quartz-scheduler.org/documentation/quartz-2.2.x/tutorials/tutorial-lesson-06.html"
    )
    private String cronExpression = "";

    @Option(name = {"-m", "--maxAttempt"},
            title = "最大尝试次数", description = "最大迁移尝试次数，默认为10")
    private Integer maxMigrateAttemptTimes = 10;

    @Arguments(usage = "<目录1> <目录2> ...",
            title = "迁移目录", description = "迁移目标目录列表，可设置多个")
    private List<String> migratePath = Lists.newArrayList();

    @Override
    protected boolean validate() {
        if(migratePath == null) {
            System.out.println("迁移目录为空");
            return false;
        }
        for (String path : migratePath) {
            File file = new File(path);
            if(!(file.exists() && file.isDirectory() && file.canWrite())){
                System.out.println(String.format("文件夹%s 不存在或无写入权限", path));
                return false;
            }
        }

        return super.validate() && expiredSecond > 0
                && !"".equals(cronExpression) && migratePath.size() > 0
                && maxMigrateAttemptTimes > 0;
    }

    @Override
    protected void execute() {
        Options.init(ksName, tbName, expiredSecond, null);
        Options.instance.setCronExpression(cronExpression);
        Options.instance.setMigratePaths(migratePath);
        Options.instance.maxMigrateAttemptTimes = maxMigrateAttemptTimes;
        MigrateUtils.startMigrate();
    }
}
