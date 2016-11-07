package cn.ac.iie.drive.commands;

import cn.ac.iie.drive.Options;
import cn.ac.iie.drive.commands.base.ClusterTableCmd;
import cn.ac.iie.migrate.MigrateUtils;
import io.airlift.command.Command;

/**
 * 迁移文件夹清理命令：cleanup
 *
 * @author Xiang
 * @date 2016-10-17 16:42
 */
@Command(name = "cleanup", description = "迁移文件夹清理，删除无用文件")
public class CleanupCmd extends ClusterTableCmd{


    @Override
    protected void execute() {
        Options.init(ksName, tbName, 0L, "");
        MigrateUtils.cleanUpMigrateDirs();
    }
}
