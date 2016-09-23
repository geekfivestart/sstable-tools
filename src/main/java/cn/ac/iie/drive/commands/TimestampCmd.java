package cn.ac.iie.drive.commands;

import cn.ac.iie.drive.Options;
import cn.ac.iie.drive.commands.base.SSTableFileCmd;
import cn.ac.iie.sstable.SSTableUtils;
import cn.ac.iie.sstable.TableTransformer;
import com.google.common.base.Strings;
import io.airlift.command.Command;

import java.io.File;

/**
 * 获取sstable事件范围命令：timestamp
 *
 * @author Xiang
 * @date 2016-09-22 13:11
 */
@Command(name = "timestamp", description = "获取sstable的时间范围")
public final class TimestampCmd extends SSTableFileCmd {

    @Override
    protected boolean validate() {
        File sstable = new File(ssTablePath);
        return sstable.exists();
    }

    @Override
    protected void execute() {
        Options.init(null, null, null, ssTablePath);
        try {
            System.out.println("\u001B[1;34m" + Options.instance.ssTablePath);
            System.out.println(TableTransformer.ANSI_CYAN + Strings.repeat("=", Options.instance.ssTablePath.length()));
            System.out.print(TableTransformer.ANSI_RESET);
            SSTableUtils.printTimestampRange(Options.instance.ssTablePath, System.out, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
