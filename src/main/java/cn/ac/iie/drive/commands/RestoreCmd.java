package cn.ac.iie.drive.commands;

import cn.ac.iie.drive.commands.base.SSTableFileCmd;
import cn.ac.iie.migrate.MigrateUtils;
import io.airlift.command.Command;

/**
 * 冷数据迁移还原命令：restore
 *
 * @author Xiang
 * @date 2016-10-17 09:49
 */
@Command(name = "restore", description = "冷数据还原")
public class RestoreCmd extends SSTableFileCmd{

    @Override
    protected boolean validate() {
        if(ssTablePath == null || ssTablePath.trim().equals("")){
            System.out.println("sstable文件路径为空");
            return false;
        }
        return true;
    }

    @Override
    protected void execute() {
        MigrateUtils.restore(ssTablePath);
    }
}
