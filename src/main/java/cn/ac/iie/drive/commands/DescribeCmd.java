package cn.ac.iie.drive.commands;

import cn.ac.iie.drive.Options;
import cn.ac.iie.drive.commands.base.SSTableFileCmd;
import cn.ac.iie.sstable.SSTableUtils;
import cn.ac.iie.sstable.TableTransformer;
import com.google.common.base.Strings;
import io.airlift.command.Command;

import java.io.File;

/**
 * 查看sstable统计信息命令：describe
 *
 * @author Xiang
 * @date 2016-09-22 11:22
 */
@Command(name="describe", description = "查看sstable统计信息")
public class DescribeCmd extends SSTableFileCmd {

    @Override
    protected boolean validate() {
        File sstable = new File(ssTablePath);
        return sstable.exists();
    }

    @Override
    protected void execute() {
        Options.init(null, null, null, ssTablePath);

        String path = new File(Options.instance.ssTablePath).getAbsolutePath();
        try {
            System.out.println("\u001B[1;34m" + path);
            System.out.println(TableTransformer.ANSI_CYAN + Strings.repeat("=", path.length()));
            System.out.print(TableTransformer.ANSI_RESET);
            SSTableUtils.printStats(path, System.out);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
