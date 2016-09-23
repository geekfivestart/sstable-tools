package cn.ac.iie.drive.commands.base;

import io.airlift.command.Option;

/**
 * 操作集群table相关的命令
 *
 * @author Xiang
 * @date 2016-09-22 14:19
 */
public class ClusterTableCmd extends SSTableToolCmd {

    @Option(name = {"-k", "--keyspace"},
            title = "ks名",
            description = "keyspace名称")
    protected String ksName = "";

    @Option(name = {"-t", "--table"},
            title = "表名",
            description = "table名称")
    protected String tbName = "";
    @Override
    protected boolean validate() {
        return !"".equals(ksName) && !"".equals(tbName);
    }

    @Override
    protected void execute() {
        super.execute();
    }
}
