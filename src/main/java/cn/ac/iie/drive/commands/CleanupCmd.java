package cn.ac.iie.drive.commands;

import cn.ac.iie.drive.Options;
import cn.ac.iie.drive.commands.base.ClusterTableCmd;
import cn.ac.iie.migrate.MigrateUtils;
import com.google.common.collect.Lists;
import io.airlift.command.Arguments;
import io.airlift.command.Command;

import java.io.File;
import java.util.List;

/**
 * 迁移文件夹清理命令：cleanup
 *
 * @author Xiang
 * @date 2016-10-17 16:42
 */
@Command(name = "cleanup", description = "迁移文件夹清理，删除无用文件")
public class CleanupCmd extends ClusterTableCmd{

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
        if(super.validate()==false){
            System.err.print("super validate failed");
        }else if(migratePath.size()<=0){
            System.err.print("migratePath validate failed");
        }
        return super.validate() && migratePath.size() > 0 ;
    }

    @Override
    protected void execute() {
        Options.init(ksName, tbName, 0L, "");
        Options.instance.setMigratePaths(migratePath);
        MigrateUtils.cleanUpMigrateDirs();
    }
}
