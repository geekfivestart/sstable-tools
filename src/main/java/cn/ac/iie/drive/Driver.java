package cn.ac.iie.drive;

import cn.ac.iie.drive.commands.*;
import cn.ac.iie.migrate.MigrateUtils;
import cn.ac.iie.move.MoveUtils;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.airlift.command.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Throwables.getStackTraceAsString;

public class Driver {
    private static Logger LOG= LoggerFactory.getLogger(Driver.class);
    private static Map<String,String> cmdMap=new HashMap<String,String>(){
        {
            put("help","显示此帮助信息");
            put("move","  用于将原表中的过期数据移动到新表中，此命令仅移动 cassandra 中数据，并不移动索引文件。 执行此命令前，需要先创建\n"+
                    Strings.repeat(" ","move".length()+2)+
                    "新表，并务必停止运行 mpp-engine 服务，即 nodetool drain && pkill -9 impalad。 需要在配置文件中配置以下\n"+
                    Strings.repeat(" ","move".length()+2)+
                    "参数，参数名与参数间使用冒号分隔。\n"+
                    Strings.repeat(" ","move".length()+2)+
                    "keyspace:\t待进行数据分离表的 keyspace\n"+
                    Strings.repeat(" ","move".length()+2)+
                    "table:\t待进行数据分离表名\n"+
                    Strings.repeat(" ","move".length()+2)+
                    "move_since:\t以秒为单位的时间戳，即对包含数据的最大时间戳小于move_since的sstable进行分离\n"+
                    Strings.repeat(" ","move".length()+2)+
                    "newTable:\t新表的名称\n");
            put("moveindex","  用于将原表中的索引文件移动到新表中，并从元数据中将原表被分离出的索引文件对应的元数据信息删除，将这些索引文件\n"+
                    Strings.repeat(" ","moveindex".length()+2)+
                    "与新表关联起来的元数据写入到元数据服务中。 执行此命令前，需要先创建新表，并务必停止运行 mpp-engine 服务，即\n"+
                    Strings.repeat(" ","moveindex".length()+2)+
                    "nodetool drain && pkill -9 impalad。 此功能需要在配置文件中配置如下参数\n"+
                    Strings.repeat(" ","moveindex".length()+2)+
                    "keyspace:\t待进行数据分离表的 keyspace\n"+
                    Strings.repeat(" ","moveindex".length()+2)+
                    "table:\t待进行数据分离表名\n"+
                    Strings.repeat(" ","moveindex".length()+2)+
                    "move_since:\t以秒为单位的时间戳，即对包含数据的最大时间戳小于move_since的sstable进行分离\n"+
                    Strings.repeat(" ","moveindex".length()+2)+
                    "newTable:\t新表的名称\n");
            put("migrate","  用于将冷数据(一定时间段之前的数据)从原始目录移动到新目录，并在原目录中建立符号链接指向新目录下的文件，这样便\n"+
                    Strings.repeat(" ","migrate".length()+2)+
                    "可实现将新数据放置在高速磁盘上(如SSD)，冷数据放置到低速磁盘(如机械硬盘)上的目的。 执行此命令前，务必停止运行\n"+
                    Strings.repeat(" ","migrate".length()+2)+
                    " mpp-engine 服务，即 nodetool drain && pkill -9 impalad。 此功能需要在配置文件中配置如下参数。\n"+
                    Strings.repeat(" ","migrate".length()+2)+
                    "keyspace:\t待进行数据迁移表的 keyspace\n"+
                    Strings.repeat(" ","migrate".length()+2)+
                    "table:\t待进行数据迁移表名\n"+
                    Strings.repeat(" ","migrate".length()+2)+
                    "move_since:\t以秒为单位的时间戳，即对包含数据的最大时间戳小于move_since的sstable进行分离\n"+
                    Strings.repeat(" ","migrate".length()+2)+
                    "migrate_dirs:\t放置冷数据的目录，每行一个目录，可配置多个\n");
            put("migrateindex","  用于将冷索引(一定时间段之前的索引)从原始目录移动到新目录，并在建立符号链接指向新目录，这样便\n"+
                    Strings.repeat(" ","migrate".length()+2)+
                    "可实现将新索引放置在高速磁盘上(如SSD)，冷索引放置到低速磁盘(如机械硬盘)上的目的。 执行此命令前，务必停止运行\n"+
                    Strings.repeat(" ","migrate".length()+2)+
                    " mpp-engine 服务，即 nodetool drain && pkill -9 impalad。 此功能需要在配置文件中配置如下参数。\n"+
                    Strings.repeat(" ","migrate".length()+2)+
                    "keyspace:\t待进行数据迁移表的 keyspace\n"+
                    Strings.repeat(" ","migrate".length()+2)+
                    "table:\t待进行数据迁移表名\n"+
                    Strings.repeat(" ","migrate".length()+2)+
                    "move_since:\t以秒为单位的时间戳，即对包含数据的最大时间戳小于move_since的sstable进行分离\n"+
                    Strings.repeat(" ","migrate".length()+2)+
                    "migrate_index_dirs:\t放置冷索引的目录，每行一个目录，可配置多个\n");
            put("cleanup","  cassandra运行过程中，在进行compaction进会删除原有的数据文件，生成新的数据文件。若数据文件已经被 迁移到\n"+
                    Strings.repeat(" ","cleanup".length()+2)+
                    "冷数据目录后，在compaction时间仅会删除指向冷数据目录的符号链接，并不会删除冷数据，进而产生无用的 数据文件。因此需\n"+
                    Strings.repeat(" ","cleanup".length()+2)+
                    "要使用此命令删除无效数据文件。 执行此命令可不用停止服务。 此功能需要在配置文件中配置如下参数。\n"+
                    Strings.repeat(" ","cleanup".length()+2)+
                    "keyspace:\t待进行数据分离表的 keyspace\n"+
                    Strings.repeat(" ","cleanup".length()+2)+
                    "table:\t待进行数据分离表名\n"+
                    Strings.repeat(" ","cleanup".length()+2)+
                    "migrate_dirs:\t放置冷数据的目录，每行一个目录，可配置多个\n");
            put("describe","  describe -f file, 显示 sstable 文件的元数据, file为待显示的sstable文件\n");
            put("timestamp","  timestamp -f file, 显示 sstable 数据的时间戳范围, file为待显示的sstable文件\n");
            put("sstable","  sstable [-i] -k ksname -t table, 显示表在当前节点上的所有 sstable 文件信息\n"+
                    Strings.repeat(" ","sstable".length()+2)+
                    "其中，-i 表示是否显示由符号链接指示的sstable, ksname 为待显示表的 keyspace, table 为待显示表名\n");
        }
    };
    public static void printHelpInfo(){
        StringBuilder sb=new StringBuilder();
        sb.append("sstable-tools [ move | moveindex | migrate | migrateindex | cleanup | " +
                "describe -f file | timestamp -f file | sstable [-i] -k ksname -t table ]\n");
        sb.append("\t");
        sb.append("move       过期数据分离\n");
        sb.append("\t");
        sb.append("moveindex  过期索引分离\n");
        sb.append("\t");
        sb.append("migrate    冷数据迁移\n");
        sb.append("\t");
        sb.append("cleanup    无效数据删除\n");
        sb.append("\t");
        sb.append("describe   显示sstable文件的元数据\n");
        sb.append("\t");
        sb.append("timestamp  显示sstable中数据的时间戳范围\n");
        sb.append("\t");
        sb.append("sstable    显示当前节点上某个表的所有sstable文件\n");
        sb.append("\t");
        sb.append("help <command> 显示每个命令的详情\n");

        System.out.print(sb.toString());
    }
    public static void main(String ... args) {
        for (String arg : args) {
            LOG.info(arg);
        }
        if(args.length>0 && !cmdMap.containsKey(args[0])){
            printHelpInfo();
            return;
        }
        if(args.length==0 || args[0].equals("help")){
            if(args.length>1){
                if(cmdMap.containsKey(args[1])){
                    System.out.print(args[1]+cmdMap.get(args[1]));
                }else{
                   printHelpInfo();
                }
            }else{
               printHelpInfo();
            }
            return;
        }else if(args.length>0 && args[0].equals("move")){
            MoveUtils.moveCassandraData(args[1],args[2],Long.parseLong(args[3]),args[4]);
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
        }else if(args.length>0 && args[0].equals("migrateindex")){
            if(args.length<7){
                System.out.println("Missing parameters!, at least 7 parameters are expected!");
                return;
            }
            String ip=args[1];
            int port=Integer.parseInt(args[2]);
            String ks=args[3];
            String table=args[4];
            long moveSince=Long.parseLong(args[5]);
            List<String> list=Lists.newArrayList();
            for(int i=6;i<args.length;++i){
                list.add(args[i]);
            }

            InetAddress localAddress = null;
            try {
                localAddress = InetAddress.getLocalHost();
            } catch (UnknownHostException e) {
                LOG.error(e.getMessage(),e);
                return;
            }
            String hostip= localAddress.getHostAddress();
            MigrateUtils.migrateIndex(ip,port,hostip,ks,table,moveSince,list);
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
