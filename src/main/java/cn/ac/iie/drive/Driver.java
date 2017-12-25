package cn.ac.iie.drive;

import cn.ac.iie.drive.commands.*;
import cn.ac.iie.migrate.MigrateUtils;
import cn.ac.iie.move.MoveUtils;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.airlift.command.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Throwables.getStackTraceAsString;

public class Driver {
    private static Logger LOG= LoggerFactory.getLogger(Driver.class);
    public static void main(String ... args) {
        for (String arg : args) {
            LOG.info(arg);
        }
        if(args.length>0 && args[0].equals("move")){
            MoveUtils.startDoMigrateTask(args[1],args[2],Long.parseLong(args[3]),args[4]);
            return;
        }else if(args.length>0 && args[0].equals("moveindex")){
            InetAddress localAddress = null;
            try {
                localAddress = InetAddress.getLocalHost();
            } catch (UnknownHostException e) {
                LOG.error(e.getMessage(),e);
                return;
            }
            String hostip= localAddress.getHostAddress();
            MoveUtils.moveIndex(args[1],Integer.parseInt(args[2]),hostip,args[3],
                    args[4],args[5],Long.parseLong(args[6]));
            return;
        }else if(args.length>0 && args[0].equals("migrate")){
            if(args.length<5){
                System.out.println("Missing parameters!");
                return;
            }
            String ks=args[1];
            String table=args[2];
            long moveSince=Long.parseLong(args[3]);
            List<String> list=new ArrayList<>();
            for(int i=4;i<args.length;++i){
                list.add(args[i]);
            }
            try {
                MigrateUtils.starMigrateMission(ks,table,moveSince,list);
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error(e.getMessage(),e);
            }
            return;
        }else if(args.length>0 && args[0].equals("cleanup")){
            if(args.length<4){
                System.out.println("Missing parameters!");
                return;
            }
            String ks=args[1];
            String table=args[2];
            List<String> list=new ArrayList<>();
            for(int i=3;i<args.length;++i){
                list.add(args[i]);
            }
            MigrateUtils.cleanUpMigrateDirs(ks,table,list);
            return;
        }
        @SuppressWarnings("unchecked")
        List<Class<? extends Runnable>> commands = Lists.newArrayList(
                Help.class,
                SSTableCmd.class,
                DescribeCmd.class,
                TimestampCmd.class,
                RestoreCmd.class
        );
        Cli.CliBuilder<Runnable> builder = Cli.builder("sstable-tools");

        builder.withDescription("sstable操作工具")
                .withDefaultCommand(Help.class)
                .withCommands(commands);

        Cli<Runnable> parser = builder.build();
        int status;
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
            System.exit(status);
        } catch (Throwable throwable)
        {
            err(Throwables.getRootCause(throwable));
            status = 2;
            System.exit(status);
        }

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
