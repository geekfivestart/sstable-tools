package cn.ac.iie.sstable;

import cn.ac.iie.cassandra.CassandraUtils;
import cn.ac.iie.drive.Driver;
import cn.ac.iie.index.IndexFileHandler;
import cn.ac.iie.utils.KillSignalHandler;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.commons.cli.*;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class UserDefinedCompact {
    private static Logger LOG = LoggerFactory.getLogger(UserDefinedCompact.class);
    private static final String KS="k";
    private static final String CF="t";
    private static final String INPUT="f";
    private static final String STIN="i";
    private static final String OUTPUT="o";
    private static final String BATCH_SIZE="b";
    private static final String CONCURR_THREADS="c";
    private static final Options options = new Options();
    static
    {
        Option ks=new Option(KS,true,"keyspace name");
        ks.setArgs(1);
        options.addOption(ks);

        Option tb=new Option(CF,true,"table name");
        tb.setArgs(1);
        options.addOption(tb);

        Option input=new Option(INPUT,true,"input file containing Data.db");
        input.setArgs(1);
        options.addOption(input);

        Option stin=new Option(STIN,true,"Data.db from stdin");
        stin.setArgs(10);
        options.addOption(stin);

        Option out=new Option(OUTPUT,true,"directory for compaction output");
        out.setArgs(1);
        options.addOption(out);

        Option batch=new Option(BATCH_SIZE,true,"batch size for on compaction task");
        out.setArgs(1);
        options.addOption(batch);

        Option numt=new Option(CONCURR_THREADS,true,"number of thread for compaction task");
        out.setArgs(1);
        options.addOption(numt);
    }

    private static boolean keepOriginals=true;
    public static String ks;
    public static String cfname;
    private static ColumnFamilyStore cfs;
    private static List<String> fileList=new ArrayList<>();
    private static String output;
    private static int threadsNum=1;
    private static int batchSize=1000;
    private static BlockingQueue<List<String>> sstQueue=new ArrayBlockingQueue(1000);

    private static void NoOP(){
        while(Driver.debug.get()==true){
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static CommandLine cmd;

    private static void parsingArgs(String [] args){
        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            System.exit(1);
        }


        if (cmd.getOptionValue(KS) == null) {
            System.err.println("missing -k parameter");
            System.exit(1);
        }
        ks= cmd.getOptionValues(KS)[0];

        if (cmd.getOptionValue(CF) == null) {
            System.err.println("missing -t parameter");
            System.exit(1);
        }
        cfname= cmd.getOptionValues(CF)[0];

        if(cmd.getOptionValue(OUTPUT)==null){
            System.err.println("missing -o parameter");
            printUsage();
            System.exit(-1);
        }
        output=cmd.getOptionValue(OUTPUT);

        if(cmd.getOptionValue(STIN)!=null){
            for (String st:cmd.getOptionValues(STIN)){
                if(!Strings.isNullOrEmpty(st.trim())){
                    if(!fileList.contains(st.trim())){
                        fileList.add(st.trim());
                    }
                }
            }
        }

        if(cmd.getOptionValue(INPUT)!=null){
            String file=cmd.getOptionValue(INPUT);
            if(!new File(file).exists()){
                System.err.println("Cannot find file " + file);
                System.exit(1);
            }else{
                try {
                    BufferedReader br=new BufferedReader(new FileReader(new File(file)));
                    String line=null;
                    while((line=br.readLine())!=null){
                        if(!fileList.contains(line.trim())){
                            fileList.add(line.trim());
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        if(fileList.size()==0){
            System.err.println("missing compact files");
            printUsage();
            System.exit(-1);
        }

        if(cmd.getOptionValue(BATCH_SIZE)!=null){
            batchSize=Integer.parseInt(cmd.getOptionValue(BATCH_SIZE).trim());
        }

        int counter=0;
        List<String> tmp=new ArrayList<>();
        for(String st:fileList){
            if(counter>= batchSize){
                sstQueue.add(tmp);
                tmp=new ArrayList<>();
                counter=0;
            }
            tmp.add(st);
            counter++;
        }

        if (tmp.size()!=0) {
            sstQueue.add(tmp);
        }

        if(cmd.getOptionValue(CONCURR_THREADS)!=null){
            threadsNum=Integer.parseInt(cmd.getOptionValue(CONCURR_THREADS).trim());
        }
    }

    private static void printUsage(){

    }

    public static void main(String []args) throws IOException, InterruptedException {
        if(System.getProperty("debugmode")!=null){
            try{
                Driver.debug.set(Boolean.parseBoolean(System.getProperty("debugmode")));
                System.out.println("debugmode:"+Driver.debug);
            }catch (Exception ex){
                System.err.println(ex.getMessage());
            }
        }
        Signal.handle(new Signal("TERM"), new KillSignalHandler());
        NoOP();

        if(args.length<1){
            System.err.println("too less args:"+args);
            System.exit(-1);
        }

        if(args[0].equals("compact")){
            String [] tmp=new String[args.length-1];
            for(int i=1;i<args.length;++i){
                tmp[i-1]=args[i];
            }
            args=tmp;
        }

        parsingArgs(args);


        DatabaseDescriptor.daemonInitialization();
        Schema.instance.loadFromDisk(false);
        Keyspace.setInitialized();

        //LifecycleTransaction transaction = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        //cfs = Keyspace.open(ks).getColumnFamilyStore(cfname);
        cfs = Keyspace.openWithoutSSTables(ks).getColumnFamilyStore(cfname);
        //run(fileList);

        for(int i=0;i<threadsNum;++i){
            CompactionTask task= new CompactionTask();
            task.setName("CompactionTask-"+i);
            task.start();
        }
    }

    private static int getLevel(){
        return 0;
    }

    public static CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                          Directories directories,
                                                          LifecycleTransaction transaction,
                                                          Set<SSTableReader> nonExpiredSSTables)
    {
        return new DefaultCompactionWriter(cfs, directories, transaction, nonExpiredSSTables, keepOriginals, getLevel());
    }

    static class CompactionTask extends Thread{

        Logger LOG = LoggerFactory.getLogger(CompactionTask.class);
        public CompactionTask(){

        }

        @Override
        public void run() {
            List<String> file=sstQueue.poll();
            while(file!=null){
                try {
                    long st=System.currentTimeMillis();
                    doaction(file);
                    long end=System.currentTimeMillis();
                    LOG.info("compaction batch finished, time comsume {}ms",end-st);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOG.error(e.getMessage(),e.getStackTrace());
                }
                file=sstQueue.poll();
            }

        }

        private void doaction(List<String> fileList) throws IOException, InterruptedException {
            CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
            Set<SSTableReader> actuallyCompact=new HashSet<>();
            int nowInSec = FBUtilities.nowInSeconds();
            long totalKeysWritten = 0;
            Collection<SSTableReader> newSStables;

            for(String st:fileList){
                LOG.debug("comacting "+st);
                try {
                    actuallyCompact.add(SSTableReader.open(Descriptor.fromFilename(st.trim())));
                }catch (Exception e){
                    LOG.error("Exception occured while opening file {}",st);
                    LOG.error(e.getMessage(),e.getStackTrace());
                }
                //actuallyCompact.add(SSTableReader.openNoValidation(Descriptor.fromFilename(st.trim()),cfs.metadata));
            }
            LOG.info("ready for compaction of {} sstables.",actuallyCompact.size());
            cfs.getTracker().addInitialSSTables(actuallyCompact);

            LifecycleTransaction transaction = cfs.getTracker().tryModify(actuallyCompact, OperationType.COMPACTION);
            CompactionController controller= new CompactionController(cfs, actuallyCompact, nowInSec);
            try (Refs<SSTableReader> refs = Refs.ref(actuallyCompact);
                 AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(actuallyCompact);
                 CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, scanners.scanners, controller, nowInSec, UUID.randomUUID()))
            {
                Directories.DataDirectory [] dirArray=new Directories.DataDirectory[1];
                dirArray[0]=new Directories.DataDirectory(new File(output));
                Directories directories=new Directories(cfs.metadata,dirArray);

                try (CompactionAwareWriter writer = getCompactionAwareWriter(cfs, directories, transaction, actuallyCompact))
                {
                    while (ci.hasNext())
                    {
                        if (ci.isStopRequested())
                            throw new CompactionInterruptedException(ci.getCompactionInfo());

                        if (writer.append(ci.next()))
                            totalKeysWritten++;
                    }

                    newSStables = writer.finish();
                    writer.close();
                }
                finally
                {

                }
                LOG.info("compact finished,{} records written",totalKeysWritten);
            }

            Refs.release(Refs.selfRefs(newSStables));
        }

    }

    private static void run(List<String> fileList) throws IOException, InterruptedException {
        CompactionStrategyManager strategy = cfs.getCompactionStrategyManager();
        Set<SSTableReader> actuallyCompact=new HashSet<>();
        int nowInSec = FBUtilities.nowInSeconds();
        long start = System.nanoTime();
        long startTime = System.currentTimeMillis();
        long totalKeysWritten = 0;
        long estimatedKeys = 0;
        long inputSizeBytes;
        Collection<SSTableReader> newSStables;

        for(String st:fileList){
            LOG.info("comacting "+st);
            actuallyCompact.add(SSTableReader.open(Descriptor.fromFilename(st.trim())));
        }

        cfs.getTracker().addInitialSSTables(actuallyCompact);

        LifecycleTransaction transaction = cfs.getTracker().tryModify(actuallyCompact, OperationType.COMPACTION);
        CompactionController controller= new CompactionController(cfs, actuallyCompact, nowInSec);
        try (Refs<SSTableReader> refs = Refs.ref(actuallyCompact);
             AbstractCompactionStrategy.ScannerList scanners = strategy.getScanners(actuallyCompact);
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, scanners.scanners, controller, nowInSec, UUID.randomUUID()))
        {
            Directories.DataDirectory [] dirArray=new Directories.DataDirectory[1];
            dirArray[0]=new Directories.DataDirectory(new File(output));
            Directories directories=new Directories(cfs.metadata,dirArray);

            try (CompactionAwareWriter writer = getCompactionAwareWriter(cfs, directories, transaction, actuallyCompact))
            {
                while (ci.hasNext())
                {
                    if (ci.isStopRequested())
                        throw new CompactionInterruptedException(ci.getCompactionInfo());

                    if (writer.append(ci.next()))
                        totalKeysWritten++;
                }

                newSStables = writer.finish();
                writer.close();
            }
            finally
            {

            }
            LOG.info("compact finished,{} records written",totalKeysWritten);
        }

        Refs.release(Refs.selfRefs(newSStables));
//            ScheduledExecutors.shutdownAndWait();
//            SSTableReader.shutdownBlocking();
//            CassandraUtils.shutdownBackgroundTasks();
            //System.exit(0);
    }
}
