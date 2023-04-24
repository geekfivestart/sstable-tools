package cn.ac.iie.sstable;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import cn.ac.iie.drive.Driver;
import cn.ac.iie.utils.KillSignalHandler;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.commons.cli.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

/**
 * Export SSTables to JSON format.
 */

public class SSTableExport {
    private static Logger LOG = LoggerFactory.getLogger(SSTableExport.class);
    private static final String KEY_OPTION = "k";
    private static final String DEBUG_OUTPUT_OPTION = "d";
    private static final String EXCLUDE_KEY_OPTION = "x";
    private static final String ENUMERATE_KEYS_OPTION = "e";
    private static final String RAW_TIMESTAMPS = "t";

    private static final Options options = new Options();
    private static CommandLine cmd;

    //column name of partition key, -k pk1
    //or -k pk1,pk2,pk3
    private static final String PK_NAME="k";

    //column name of clustering key, -c ck1
    //or -c ck1,ck2,ck3
    private static final String CK_NAME="c";

    // file that contains token range, -t file,
    private static final String TOKEN_RANGE="t";

    //output file
    private static final String OUT_PUT="o";

    //size of output size, supporting g/G for GB,m/M for MB
    private static final String SIZE="s";

    public static String[] pkName;

    public static String[] ckName;

    private static String outfilePrefix="./dumping";
    public static List<Range<Token>> tkRange=new ArrayList<>();
    //file size in bytes, default 10MB
    public static long fileSize=100*1024*1024;

    public static String APPEND="a";
    public static boolean append_flag=false;

    public static String COLUMN_NAME="l";
    public static Map<String,Byte[]> selectColumn=new HashMap<>();

    private static final String CMD_INPUT="i";
    private static final String INPUT_FILE="f";
    private static List<String> ssfiles=new ArrayList<>();

    static
    {
        DatabaseDescriptor.toolInitialization();

        Option pkName =new Option(PK_NAME,true,"partition column name");
        pkName.setArgs(10);
        options.addOption(pkName);

        Option ckName =new Option(CK_NAME,true,"clustering column name");
        ckName.setArgs(10);
        options.addOption(ckName);

        Option tkRange=new Option(TOKEN_RANGE,true,"token range for dumping");
        tkRange.setArgs(10);
        options.addOption(tkRange);


        Option dumpingFile=new Option(OUT_PUT,true,"ouput file for dumping");
        dumpingFile.setArgs(10);
        options.addOption(dumpingFile);


        Option fileSize=new Option(SIZE,true,"dumping file size");
        fileSize.setArgs(10);
        options.addOption(fileSize);

        Option append=new Option(APPEND,false,"whether appending output to existing files");
        options.addOption(append);

        Option columnName=new Option(COLUMN_NAME,true,"selection of output column name");
        columnName.setArgs(10);
        options.addOption(columnName);

        Option cmdinput=new Option(CMD_INPUT,true,"sstable file from cmd argment");
        cmdinput.setArgs(1);
        options.addOption(cmdinput);

        Option inputfile=new Option(INPUT_FILE,true,"sstable files indicated by file");
        inputfile.setArgs(1);
        options.addOption(inputfile);
    }

     /**
     * Construct table schema from info stored in SSTable's Stats.db
     *
     * @param desc SSTable's descriptor
     * @return Restored CFMetaData
     * @throws IOException when Stats.db cannot be read
     */

    public static CFMetaData metadataFromSSTable(Descriptor desc) throws IOException
    {
        if (!desc.version.storeRows())
            throw new IOException("pre-3.0 SSTable is not supported.");

        EnumSet<MetadataType> types = EnumSet.of(MetadataType.STATS, MetadataType.HEADER);
        Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);
        IPartitioner partitioner = FBUtilities.newPartitioner(desc);

        CFMetaData.Builder builder = CFMetaData.Builder.create("keyspace", "table").withPartitioner(partitioner);
        header.getStaticColumns().entrySet().stream()
                .forEach(entry -> {
                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                    builder.addStaticColumn(ident, entry.getValue());
                });
        header.getRegularColumns().entrySet().stream()
                .forEach(entry -> {
                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                    builder.addRegularColumn(ident, entry.getValue());
                });
        builder.addPartitionKey("PartitionKey", header.getKeyType());
        for (int i = 0; i < header.getClusteringTypes().size(); i++)
        {
            builder.addClusteringColumn("clustering" + (i > 0 ? i : ""), header.getClusteringTypes().get(i));
        }
        return builder.build();
    }

    private static <T> Stream<T> iterToStream(Iterator<T> iter)
    {
        Spliterator<T> splititer = Spliterators.spliteratorUnknownSize(iter, Spliterator.IMMUTABLE);
        return StreamSupport.stream(splititer, false);
    }
    private static void NoOP(){
        while(Driver.debug.get()==true){
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void parsingArgs(String [] args){
        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            printUsage();
            System.exit(1);
        }

//        if (cmd.getArgs().length != 1)
//        {
//            System.err.println("You must supply exactly one sstable");
//            printUsage();
//            System.exit(1);
//        }

        if (cmd.getOptionValue(PK_NAME) == null) {
            System.err.println("missing -k parameter");
            System.exit(1);
        }
        pkName= cmd.getOptionValues(PK_NAME)[0].split(",");

        if (cmd.getOptionValue(CK_NAME) != null) {
            ckName= cmd.getOptionValues(CK_NAME)[0].split(",");
        }

        if (cmd.getOptionValue(TOKEN_RANGE) != null) {
           String file=cmd.getOptionValue(TOKEN_RANGE);
           if(!new File(file).exists()){
               System.err.println("Cannot find token file " + file);
               System.exit(1);
           }else{
               try {
                   BufferedReader br=new BufferedReader(new FileReader(new File(file)));
                   String line=null;
                   while((line=br.readLine())!=null){
                       String [] tkarray=line.trim().replaceAll(" ","").split(",");
                       if(tkarray.length!=2){
                           throw new RuntimeException("token format error:"+line);
                       }
                       long left=Long.parseLong(tkarray[0]);
                       long right=Long.parseLong(tkarray[1]);

                       tkRange.add(new Range<Token>(new LongToken(left), new LongToken(right)));
                   }
               } catch (IOException e) {
                   e.printStackTrace();
               }
           }
        }

        if (cmd.getOptionValue(SIZE) != null) {
            String size=cmd.getOptionValue(SIZE).trim().toLowerCase();
            if(!(size.endsWith("m")||size.endsWith("g"))){
                throw new RuntimeException("size format error:"+size);
            }
            fileSize = 1024 * 1024 * Long.parseLong(size.substring(0,size.length()-1));
            if(size.endsWith("g")){
               fileSize=fileSize*1024;
            }
        }
        if (cmd.getOptionValue(OUT_PUT) != null) {
            outfilePrefix=cmd.getOptionValue(OUT_PUT).trim();
        }

        append_flag=cmd.hasOption(APPEND);

        if(cmd.hasOption(COLUMN_NAME)){
            String [] columns= cmd.getOptionValue(COLUMN_NAME).trim().split(",");
            for(String st:columns){
                selectColumn.put(st,new Byte[0]);
            }
        }

        if(cmd.getOptionValue(CMD_INPUT)!=null){
            String [] files=cmd.getOptionValue(CMD_INPUT).trim().split(",");
            for(String st:files)
                ssfiles.add(st);
        }

        if(cmd.getOptionValue(INPUT_FILE)!=null){
            String file=cmd.getOptionValue(INPUT_FILE);
            if(!new File(file).exists()){
                System.err.println("Cannot find input file " + file);
                System.exit(1);
            }else{
                try {
                    BufferedReader br=new BufferedReader(new FileReader(new File(file)));
                    String line=null;
                    while((line=br.readLine())!=null){
                        try{
                            if(new File(line.trim()).exists()){
                                ssfiles.add(line.trim());
                            }else {
                                LOG.warn("sst file not exist. {}",line.trim());
                            }
                        }
                        catch (Exception e){
                            LOG.error(e.getMessage(),e.getStackTrace());
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        if(ssfiles.size()==0){
            System.err.println("No sstable files, use -i or/and -f to indicate sstable files");
            LOG.error("No sstable files, use -i or/and -f to indicate sstable files");
            System.exit(-1);
        }
    }



    public static void main(String [] args) throws IOException {
        if(args.length>0 && args[0].equals("sstabledump")){
            String [] tmp=new String[args.length-1];
            for(int i=1;i<args.length;++i){
                tmp[i-1]=args[i];
            }
            args=tmp;
        }

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
        parsingArgs(args);

        for(String ssTableFileName:ssfiles) {
            try {
                if (Descriptor.isLegacyFile(new File(ssTableFileName))) {
                    System.err.println("Unsupported legacy sstable");
                    continue;
                }
                if (!new File(ssTableFileName).exists()) {
                    System.err.println("Cannot find file " + ssTableFileName);
                    continue;
                }
                Descriptor desc = Descriptor.fromFilename(ssTableFileName);

                CFMetaData metadata = metadataFromSSTable(desc);

                SSTableReader sstable = SSTableReader.openNoValidation(desc, metadata);
                //IPartitioner partitioner = sstable.getPartitioner();
                final ISSTableScanner currentScanner = sstable.getScanner();

                Stream<UnfilteredRowIterator> partitions = iterToStream(currentScanner);
                JsonTransformer.toJson(currentScanner, partitions, false, metadata, System.out, outfilePrefix);
            }catch (Exception e){
                LOG.error(e.getMessage(),e.getStackTrace());
            }
        }
    }

    /**
     * Given arguments specifying an SSTable, and optionally an output file, export the contents of the SSTable to JSON.
     *
     * @param args
     *            command lines arguments
     * @throws ConfigurationException
     *             on configuration failure (wrong params given)
     */

    public static void main1(String[] args) throws ConfigurationException
    {
        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            printUsage();
            System.exit(1);
        }

        if (cmd.getArgs().length != 1)
        {
            System.err.println("You must supply exactly one sstable");
            printUsage();
            System.exit(1);
        }

        String[] keys = cmd.getOptionValues(KEY_OPTION);
        HashSet<String> excludes = new HashSet<>(Arrays.asList(
                cmd.getOptionValues(EXCLUDE_KEY_OPTION) == null
                        ? new String[0]
                        : cmd.getOptionValues(EXCLUDE_KEY_OPTION)));
        String ssTableFileName = new File(cmd.getArgs()[0]).getAbsolutePath();

        if (Descriptor.isLegacyFile(new File(ssTableFileName)))
        {
            System.err.println("Unsupported legacy sstable");
            System.exit(1);
        }
        if (!new File(ssTableFileName).exists())
        {
            System.err.println("Cannot find file " + ssTableFileName);
            System.exit(1);
        }
        Descriptor desc = Descriptor.fromFilename(ssTableFileName);
        try
        {
            CFMetaData metadata = metadataFromSSTable(desc);
            if (cmd.hasOption(ENUMERATE_KEYS_OPTION))
            {
                try (KeyIterator iter = new KeyIterator(desc, metadata))
                {
                    JsonTransformer.keysToJson(null, iterToStream(iter),
                            cmd.hasOption(RAW_TIMESTAMPS),
                            metadata,
                            System.out);
                }
            }
            else
            {
                SSTableReader sstable = SSTableReader.openNoValidation(desc, metadata);
                IPartitioner partitioner = sstable.getPartitioner();
                final ISSTableScanner currentScanner;
                if ((keys != null) && (keys.length > 0))
                {
                    List<AbstractBounds<PartitionPosition>> bounds = Arrays.stream(keys)
                            .filter(key -> !excludes.contains(key))
                            .map(metadata.getKeyValidator()::fromString)
                            .map(partitioner::decorateKey)
                            .sorted()
                            .map(DecoratedKey::getToken)
                            .map(token -> new Bounds<>(token.minKeyBound(), token.maxKeyBound())).collect(Collectors.toList());
                    currentScanner = sstable.getScanner(bounds.iterator());
                }
                else
                {
                    currentScanner = sstable.getScanner();
                }
                Stream<UnfilteredRowIterator> partitions = iterToStream(currentScanner).filter(i ->
                        excludes.isEmpty() || !excludes.contains(metadata.getKeyValidator().getString(i.partitionKey().getKey()))
                );
                if (cmd.hasOption(DEBUG_OUTPUT_OPTION))
                {
                    AtomicLong position = new AtomicLong();
                    partitions.forEach(partition ->
                    {
                        position.set(currentScanner.getCurrentPosition());

                        if (!partition.partitionLevelDeletion().isLive())
                        {
                            System.out.println("[" + metadata.getKeyValidator().getString(partition.partitionKey().getKey()) + "]@" +
                                    position.get() + " " + partition.partitionLevelDeletion());
                        }
                        if (!partition.staticRow().isEmpty())
                        {
                            System.out.println("[" + metadata.getKeyValidator().getString(partition.partitionKey().getKey()) + "]@" +
                                    position.get() + " " + partition.staticRow().toString(metadata, true));
                        }
                        partition.forEachRemaining(row ->
                        {
                            System.out.println(
                                    "[" + metadata.getKeyValidator().getString(partition.partitionKey().getKey()) + "]@"
                                            + position.get() + " " + row.toString(metadata, false, true));
                            position.set(currentScanner.getCurrentPosition());
                        });
                    });
                }
                else
                {
                    JsonTransformer.toJson(currentScanner, partitions, cmd.hasOption(RAW_TIMESTAMPS), metadata, System.out,outfilePrefix);
                }
            }
        }
        catch (IOException e)
        {
            // throwing exception outside main with broken pipe causes windows cmd to hang
            e.printStackTrace(System.err);
        }

        System.exit(0);
    }

    private static void printUsage()
    {
        String usage = String.format("sstable-tools sstabledump <sstable file path> <options>%n");
        //String usage = String.format("sstabledump <sstable file path> <options>%n");
        String header = "Dump contents of given SSTable to standard output in JSON format.";
        new HelpFormatter().printHelp(usage, header, options, "");
    }
}

