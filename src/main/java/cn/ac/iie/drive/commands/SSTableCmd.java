package cn.ac.iie.drive.commands;

import cn.ac.iie.cassandra.CassandraUtils;
import cn.ac.iie.drive.Options;
import cn.ac.iie.drive.commands.base.ClusterTableCmd;
import cn.ac.iie.sstable.SSTableUtils;
import cn.ac.iie.sstable.TableTransformer;
import com.google.common.base.Strings;
import io.airlift.command.Command;
import io.airlift.command.Option;
import org.apache.cassandra.tools.NodeProbe;

/**
 * 获取某个表的所有sstable文件命令
 *
 * @author Xiang
 * @date 2016-09-22 13:15
 */
@Command(name = "sstable", description = "获取本节点上某个表的所有sstable")
public class SSTableCmd extends ClusterTableCmd {


    @Option(name = {"-i", "--include-symbol"},
            title = "软连接标识",
            description = "结果中是否包含软连接")
    private Boolean includeSymbolicLink = false;

    @Override
    protected boolean validate() {
        System.out.printf("是否包含软连接：%s%n", includeSymbolicLink);
        return !"".equals(ksName) && !"".equals(tbName);
    }

    @Override
    protected void execute() {
        Options.init(ksName, tbName, null, null);
        try {
            String tip = String.format("SSTables for keyspace: %s, table: %s",
            Options.instance.ksName, Options.instance.tbName);
            System.out.printf("\u001B[1;34m %s%n", tip);
            System.out.println(TableTransformer.ANSI_CYAN + Strings.repeat("=", tip.length()+1));
            System.out.print(TableTransformer.ANSI_RESET);
            SSTableUtils.printSSTables(Options.instance.ksName, Options.instance.tbName, includeSymbolicLink);
            System.out.printf("%s--finished--%s%n", TableTransformer.ANSI_RED, TableTransformer.ANSI_RESET);
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
