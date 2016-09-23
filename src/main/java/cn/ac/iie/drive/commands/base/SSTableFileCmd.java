package cn.ac.iie.drive.commands.base;

import io.airlift.command.Option;

/**
 * 操作sstable文件相关的命令
 *
 * @author Xiang
 * @date 2016-09-22 14:15
 */
public class SSTableFileCmd extends SSTableToolCmd {

    @Option(name = {"-f", "--sstable-file"},
            title = "文件路径",
            description = "sstable文件路径")
    protected String ssTablePath = "";

    @Override
    protected boolean validate() {
        return super.validate();
    }

    @Override
    protected void execute() {
        super.execute();
    }
}
