package cn.ac.iie.drive;

import cn.ac.iie.drive.commands.DescribeCmd;
import cn.ac.iie.drive.commands.MigrateCmd;
import cn.ac.iie.drive.commands.SSTableCmd;
import cn.ac.iie.drive.commands.TimestampCmd;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.airlift.command.*;

import java.util.List;

import static com.google.common.base.Throwables.getStackTraceAsString;

public class Driver {

    public static void main(String ... args) {
        @SuppressWarnings("unchecked")
        List<Class<? extends Runnable>> commands = Lists.newArrayList(
                Help.class,
                SSTableCmd.class,
                DescribeCmd.class,
                TimestampCmd.class,
                MigrateCmd.class
        );
        Cli.CliBuilder<Runnable> builder = Cli.builder("sstable-tools");

        builder.withDescription("sstable操作工具")
                .withDefaultCommand(Help.class)
                .withCommands(commands);

        Cli<Runnable> parser = builder.build();
        int status = 0;
        try
        {
            Runnable parse = parser.parse(args);
            parse.run();
        } catch (IllegalArgumentException |
                IllegalStateException |
                ParseArgumentsMissingException |
                ParseArgumentsUnexpectedException |
                ParseOptionConversionException |
                ParseOptionMissingException |
                ParseOptionMissingValueException |
                ParseCommandMissingException e)
        {
            badUse(e);
            status = 1;
        } catch (Throwable throwable)
        {
            err(Throwables.getRootCause(throwable));
            status = 2;
        }

        System.exit(status);
    }

    private static void badUse(Exception e)
    {
        System.out.println("sstable-tools: " + e.getMessage());
        System.out.println("See 'sstable-tools help' or 'sstable-tools help <command>'.");
    }

    private static void err(Throwable e)
    {
        System.err.println("error: " + e.getMessage());
        System.err.println("-- StackTrace --");
        System.err.println(getStackTraceAsString(e));
    }

}
