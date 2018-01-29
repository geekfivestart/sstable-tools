package cn.ac.iie.cassandra;

import cn.ac.iie.drive.Driver;
import cn.ac.iie.sstable.SSTableUtils;
import cn.ac.iie.utils.InvokeUtils;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.DateTieredCompactionStrategy;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Cassandra操作工具类
 *
 * @author Xiang
 * @date 2016-09-20 17:00
 */
public class CassandraUtils {

    private static final Logger logger = LoggerFactory.getLogger(CassandraUtils.class);
    private static final Map<String, UserType> knownTypes = Maps.newHashMap();
    private static final Map<String, KeyspaceMetadata> keyspaceMetadataMap = Maps.newConcurrentMap();
    private static final Map<String, CFMetaData> cfMetaDataMap = Maps.newConcurrentMap();
    private static final Map<String, Long> maxWindowSizeSecondsMap = Maps.newConcurrentMap();
    private static final Long million = 1000000L;
    private static boolean loaded = false;

    /**
     * 关闭不必要的后台任务
     * 该操作，忽略所有异常
     */
    private static void shutdownBackgroundTasks() {
        try {
            if(!Config.isClientMode()) {
                logger.info("{} task(s) in total，{} task(s) is running，{} task(s) completed",
                        ScheduledExecutors.optionalTasks.getTaskCount(),
                        ScheduledExecutors.optionalTasks.getActiveCount(),
                        ScheduledExecutors.optionalTasks.getCompletedTaskCount());
                ScheduledExecutors.optionalTasks.getQueue().forEach(runnable -> {
                    if (runnable instanceof RunnableScheduledFuture) {
                        RunnableScheduledFuture task = (RunnableScheduledFuture) runnable;
                        if (task.cancel(true)) {
                            logger.info("The task {} has been stopped", task.getClass().getName());
                        }
                    }
                });

                ScheduledExecutors.optionalTasks.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
                ScheduledExecutors.scheduledTasks.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
                ScheduledExecutors.scheduledFastTasks.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

                ScheduledExecutors.optionalTasks.shutdown();
                logger.info("{} task(s) in total，{} task(s) is running，{} task(s) completed",
                        ScheduledExecutors.optionalTasks.getTaskCount(),
                        ScheduledExecutors.optionalTasks.getActiveCount(),
                        ScheduledExecutors.optionalTasks.getCompletedTaskCount());
                try {
                    CommitLog.instance.shutdownBlocking();
                    ColumnFamilyStore.shutdownPostFlushExecutor();
                    MessagingService.instance().shutdown();
                    ScheduledExecutors.scheduledTasks.shutdown();
                    ScheduledExecutors.nonPeriodicTasks.shutdown();
                    ScheduledExecutors.scheduledFastTasks.shutdown();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        } catch (Exception ignored){
            logger.warn(ignored.getMessage(), ignored);
        }
    }

    /**
     * 获取keyspace元数据，优先从缓存中获取，若未缓存则从集群中获取
     * 若集群中不存在该keyspace则抛出异常
     * @param keyspaceName keyspace名称
     * @return 返回keyspace元数据
     */
    private static KeyspaceMetadata keyspaceFromName(String keyspaceName) throws NoSuchKeyspaceException {
        // 由于没有对缓存进行初始化，因此需要在第一次获取某个keyspace时从集群中获取
        // 如此，可尽量减少不必要的缓存
        if(keyspaceMetadataMap.containsKey(keyspaceName)){
            return keyspaceMetadataMap.get(keyspaceName);
        } else{
            if(!loaded) {
                Schema.instance.loadFromDisk(false);
                loaded = true;
            }
            KeyspaceMetadata metadata = Schema.instance.getKSMetaData(keyspaceName);
            // 若集群中不存在相应的keyspace，则抛出运行时异常
            if(metadata == null ){
                logger.error("keyspace not exist: {}", keyspaceName);
                throw new NoSuchKeyspaceException(String.format("keyspace not exist: %s", keyspaceName));
            }
            logger.info("Get meta data of keyspace({}) {} from cluster", keyspaceName, metadata.toString());
            shutdownBackgroundTasks();
            keyspaceMetadataMap.put(keyspaceName, metadata);
            return metadata;
        }
    }

    /**
     * 根据keyspace及table名称获取表元数据，优先从缓存中获取元数据
     * 若缓存中不存在该元数据，则从keyspace获取
     * @param keyspaceName keyspace名称
     * @param tableName table名称
     * @return 返回表元数据
     */
    private static CFMetaData tableFromKeyspace(String keyspaceName, String tableName)
            throws NoSuchKeyspaceException, NoSuchTableException {
        // 由于缓存中key的格式为keyspace.table，因此需要将table名称装换成真正的table名
        String ksTableName = String.format("%s.%s", keyspaceName, tableName);
        if(cfMetaDataMap.containsKey(ksTableName)){
            return cfMetaDataMap.get(ksTableName);
        }
        KeyspaceMetadata ksMetaData = keyspaceFromName(keyspaceName);
        CFMetaData tableMetaData = ksMetaData.getTableOrViewNullable(tableName);
        if(tableMetaData == null){
            logger.error("table not exist:{}", tableName);
            throw new NoSuchTableException(String.format("表不存在: %s", tableName));
        }
        logger.info("Get metadata from keyspace（{}） for table（{}）:{}",
                keyspaceName, tableName, tableMetaData.toString());
        cfMetaDataMap.put(ksTableName, tableMetaData);
        return tableMetaData;
    }

    /**
     * 自动发现schema
     * @return 返回schema输入流
     * @throws IOException IO异常
     */
    public static InputStream findSchema() throws IOException {
        // 从系统变量中获取schema文件路径
        // 用户可以在.bash_profile中设置sstabletools.schema，
        // 将其指向一个确定的schema路径
        String cqlPath = System.getProperty("sstabletools.schema");
        InputStream in;
        if (!Strings.isNullOrEmpty(cqlPath)) {
            in = new FileInputStream(cqlPath);
        } else {
            // 从相对路径中读取schema文件
            // 若系统变量中没有设置schema文件路径，则可以将schema文件放置于
            // 本工具相同的目录下并命名为schema.cql
            in = Driver.class.getClassLoader().getResourceAsStream("schema.cql");
            if (in == null && new File("schema.cql").exists()) {
                in = new FileInputStream("schema.cql");
            }
        }
        return in;
    }

    /**
     * 从cql中获取表元数据
     * @param source cql语句数据流
     * @return 返回表元数据
     * @throws IOException IO异常
     */
    public static CFMetaData tableFromCQL(InputStream source) throws IOException {
        return tableFromCQL(source, null);
    }

    /**
     * 从cql中获取表元数据
     * @param source cql输入流
     * @param cfid 表id
     * @return 返回表元数据
     * @throws IOException IO异常，输入流读取失败时抛出该异常
     */
    private static CFMetaData tableFromCQL(InputStream source, UUID cfid) throws IOException {
        // 从输入流中读取schema信息,
        // 并将schema转化为CFStatement对象
        String schema = CharStreams.toString(new InputStreamReader(source, "UTF-8"));
        CFStatement statement = (CFStatement) QueryProcessor.parseStatement(schema);
        // 若keyspace不存在则应设置默认keyspace，
        // 此处将默认keyspace设置为sstable_tools
        String keyspace;
        try {
            keyspace = statement.keyspace() == null ? "sstable_tools" : statement.keyspace();
        } catch (AssertionError e) { // 如果添加-ea选项可能会打印大量警告日志
            logger.warn("-ea parameter is not recommanded");
            keyspace = "sstable_tools";
        }
        // 设置keyspace元数据，
        // 若keyspace元数据不存在则重新创建
        statement.prepareKeyspace(keyspace);
        if(Schema.instance.getKSMetaData(keyspace) == null) {
            Schema.instance.setKeyspaceMetadata(KeyspaceMetadata.create(keyspace, KeyspaceParams.local(), Tables.none(),
                    Views.none(), getTypes(), Functions.none()));
        }
        // 创建表元数据
        CFMetaData cfm;
        if(cfid != null) {
            cfm = ((CreateTableStatement) statement.prepare().statement).metadataBuilder().withId(cfid).build();
            KeyspaceMetadata prev = Schema.instance.getKSMetaData(keyspace);
            List<CFMetaData> tables = Lists.newArrayList(prev.tablesAndViews());
            tables.add(cfm);
            Schema.instance.setKeyspaceMetadata(prev.withSwapped(Tables.of(tables)));
            Schema.instance.load(cfm);
        } else {
            cfm = ((CreateTableStatement) statement.prepare().statement).getCFMetaData();
        }
        return cfm;
    }

    /**
     * 根据keyspace及table名称获取SSTable文件
     * @param keyspaceName keyspace名称
     * @param tableName table名称
     * @param includeSymbolicLink 是否包含软连接
     * @return 返回SSTable文件列表
     */
    public static List<File> ssTableFromName(String keyspaceName, String tableName, boolean includeSymbolicLink)
            throws NoSuchKeyspaceException, NoSuchTableException {
        CFMetaData metaData = tableFromName(keyspaceName, tableName);


        Directories directories = new Directories(metaData, ColumnFamilyStore.getInitialDirectories());
        List<File> fileList = new ArrayList<>();
        // 非数据文件暂时不迁移；软连接文件表示已经迁移过，无需再迁移，因此，
        // 过滤非数据文件,同时根据notFilterExpired标识决定是否过滤软连接文件
        directories.getCFDirectories().forEach(dir -> LifecycleTransaction.getFiles(dir.toPath(),
                (file, fileType) -> fileType == Directories.FileType.FINAL
                        && file.getName().endsWith("Data.db")
                        && (includeSymbolicLink ||!Files.isSymbolicLink(file.toPath())),
                Directories.OnTxnErr.THROW).forEach(fileList::add));
        return fileList;
    }

    /**
     * 通过表名，获取冷化的sstable
     * 首先从集群中获取表的元数据，
     * 然后从元数据中获取数据存储路径
     * 最后获取各个路径下的所有sstable
     * @param keyspaceName keyspace名称
     * @param tableName table名称
     * @param expiredSecond 冷化时间
     * @return 返回所有已冷化且为迁移的sstable
     */
    public static List<File> expiredSSTableFromName(String keyspaceName, String tableName, @NotNull Long expiredSecond)
            throws Exception {
        Long maxEntireMicroSecond = (expiredSecond + maxWindowSizeSeconds(keyspaceName, tableName)) *  million;
        Long nowTimestamp = System.currentTimeMillis() * 1000;

        CFMetaData metaData = tableFromName(keyspaceName, tableName);
        Directories directories = new Directories(metaData, ColumnFamilyStore.getInitialDirectories());
        List<File> fileList = new ArrayList<>();
        // 尝试过使用jmx直接从cassandra进程中获取所有ssTable文件，
        // 虽然该方法可行且更为方便，
        // 但鉴于目前原生cassandra并不提供该接口，
        // 若想使用该方法须自行修改cassandra代码添加该接口，
        // 这样难以保证安全性，因此暂不推荐该方法
        // 但目前的方法还无法判断ssTable是否在线
        // 非数据文件暂时不迁移；软连接文件表示已经迁移过，无需再迁移，因此，
        // 过滤非数据文件及软连接文件
        directories.getCFDirectories().forEach(dir -> LifecycleTransaction.getFiles(dir.toPath(),
                (file, fileType) -> {
                    try {
                        return fileType == Directories.FileType.FINAL
                                && file.getName().endsWith("Data.db")
                                && !Files.isSymbolicLink(file.toPath())
                                && nowTimestamp - maxTimestampOfSSTable(file.getAbsolutePath()) >= maxEntireMicroSecond;
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                        return false;
                    }
                },
                Directories.OnTxnErr.THROW).forEach(fileList::add));
        return fileList;
    }
    public static List<File> getSstableFromTime(String keyspaceName, String tableName, @NotNull Long expiredSecond)
            throws Exception {
        Long maxEntireMicroSecond = (expiredSecond ) *  million;

        CFMetaData metaData = tableFromName(keyspaceName, tableName);
        Directories directories = new Directories(metaData, ColumnFamilyStore.getInitialDirectories());
        List<File> fileList = new ArrayList<>();
        // 尝试过使用jmx直接从cassandra进程中获取所有ssTable文件，
        // 虽然该方法可行且更为方便，
        // 但鉴于目前原生cassandra并不提供该接口，
        // 若想使用该方法须自行修改cassandra代码添加该接口，
        // 这样难以保证安全性，因此暂不推荐该方法
        // 但目前的方法还无法判断ssTable是否在线
        // 非数据文件暂时不迁移；软连接文件表示已经迁移过，无需再迁移，因此，
        // 过滤非数据文件及软连接文件
        directories.getCFDirectories().forEach(dir -> LifecycleTransaction.getFiles(dir.toPath(),
                (file, fileType) -> {
                    try {
                        return fileType == Directories.FileType.FINAL
                                && file.getName().endsWith("Data.db")
                                && !Files.isSymbolicLink(file.toPath())
                                && maxTimestampOfSSTable(file.getAbsolutePath()) <= maxEntireMicroSecond;
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                        return false;
                    }
                },
                Directories.OnTxnErr.THROW).forEach(fileList::add));
        return fileList;
    }

    /**
     * 获取某个table的合并最大时间窗口
     * 前提是该table的合并策略必须为DateTieredCompactionStrategy
     * @param keyspaceName keyspace名称
     * @param tableName table名称
     * @return 返回table的最大合并时间窗口
     */
    private static Long maxWindowSizeSeconds(String keyspaceName, String tableName)
            throws Exception {
        // 由于缓存中key的格式为keyspace.table，因此需要将table名称装换成真正的table名
        // 获取最大合并时间窗口时优先从缓存中获取，若缓存中不存在则寻找相应的表元数据，
        // 然后从表元数据中获取该时间窗口，同时将该值加入缓存中
        String ksTableName = String.format("%s.%s", keyspaceName, tableName);
        if(maxWindowSizeSecondsMap.containsKey(ksTableName)){
            return maxWindowSizeSecondsMap.get(ksTableName);
        }
        CFMetaData metaData = tableFromKeyspace(keyspaceName, tableName);
        logger.info("compaction class:{}",metaData.params.compaction.klass().getName());
        if(DateTieredCompactionStrategy.class.isAssignableFrom(metaData.params.compaction.klass())) {
            // 如果合并策略设置为DateTieredCompactionStrategy，则元数据中应包含max_window_size_seconds
            // 若不包含该属性，则有可能是集群出现问题，应首先检查集群健康状态及相关keyspace与table状态

            Map<String, String> params = metaData.params.compaction.options();
            if (params.containsKey("max_window_size_seconds")) {
                String seconds = params.get("max_window_size_seconds");
                Long secondDigit = Long.parseLong(seconds);
                maxWindowSizeSecondsMap.put(ksTableName, secondDigit);
                return secondDigit;
            } else {
                throw new Exception("Property [max_window_size_seconds] not exist，please check the schema");
            }
        } else if(TimeWindowCompactionStrategy.class.isAssignableFrom(metaData.params.compaction.klass())){
            // 如果合并策略设置为TimeWindowCompactionStrategy，
            // 则元数据中应包含compaction_window_size及compaction_window_unit
            Map<String, String> params = metaData.params.compaction.options();
            String size;
            String unit;
            logger.info("params:{}",params.toString());
            if (params.containsKey("compaction_window_size")) {
                size = params.get("compaction_window_size");
            } else {
                size="1";
                //throw new Exception("Property [compaction_window_size] not exist，please check the schema");
            }
            if(params.containsKey("compaction_window_unit")){
                unit = params.get("compaction_window_unit");
            } else{
                unit="DAYS";
                //throw new Exception("Property [compaction_window_unit] not exist，please check the schema");
            }
            Long secondDigit;
            switch (unit){
                case "MINUTES":
                    secondDigit = Long.parseLong(size) * 60;
                    break;
                case "HOURS":
                    secondDigit = Long.parseLong(size) * 3600;
                    break;
                case "DAYS":
                    secondDigit= Long.parseLong(size) * 86400;
                    break;
                default:
                    throw new Exception(String.format("[compaction_window_unit not] not correct：%s", unit));
            }
            maxWindowSizeSecondsMap.put(ksTableName, secondDigit);
            return secondDigit;
        } else {
            throw new Exception(String.format("DateTieredCompactionStrategy、TimeWindowCompactionStrategy expected for %s.%s, " +
                            "rather than %s", keyspaceName, tableName, InvokeUtils.genericSuperclass(metaData.params.compaction.klass())));
        }
    }

    /**
     * 根据SSTable文件获取该文件中的记录的最大时间戳
     * @param ssTablePath SSTable文件路径
     * @return 返回最大时间戳
     */
    public static Long maxTimestampOfSSTable(String ssTablePath) throws Exception {
        // 从SSTable元数据中获取
        Long maxTimestamp = SSTableUtils.maxTimestamp(ssTablePath);
        if (maxTimestamp <= 0) {
            throw new Exception(String.format("The timestamp of %s is not correct", ssTablePath));
        }
        //logger.info("STable：{} MaxTimestamp：{}({})",
         //       ssTablePath, maxTimestamp, SSTableUtils.toDateString(maxTimestamp, TimeUnit.MICROSECONDS, false));
        return maxTimestamp;
    }

    /**
     * 根据keyspace及table名称获取表元数据
     * @param keyspaceName keyspace名称
     * @param tableName table名称
     * @return 返回表元数据
     */
    private static CFMetaData tableFromName(String keyspaceName, String tableName)
            throws NoSuchKeyspaceException, NoSuchTableException {

        return tableFromKeyspace(keyspaceName, tableName);
    }

    /**
     * 获取支持的类型。
     * 若预先定义的已知类型集合为空，则直接返回NONE类型；
     * 否则则根据已知类型集合设置支持的类型
     * @return 返回支持的类型
     */
    public static Types getTypes() {
        if (knownTypes.isEmpty()) {
            return Types.none();
        } else {
            return Types.of(knownTypes.values().toArray(new UserType[0]));
        }
    }

    public static String getTableUid(String keyspaceName, String tableName)
            throws NoSuchKeyspaceException, NoSuchTableException {
        CFMetaData table = tableFromName(keyspaceName, tableName);
        return table.cfName + "-" + ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(table.cfId));
    }

    public static void getlocalRecord(String ks,String tb,String pk,String ... ck){
        Schema.instance.loadFromDisk(false);
        Keyspace.setInitialized();
        Keyspace.open("system");
        Keyspace.open("system_schema");
        Collection<ColumnFamilyStore> cfsColl=Keyspace.open(ks).getColumnFamilyStores();
        ColumnFamilyStore cfs=null;
        for(ColumnFamilyStore cf:cfsColl){
            if(cf.name.equals(tb))
                cfs=cf;
        }
        DecoratedKey dk=null;
        NavigableSet<Clustering> navCluster=null;
        CFMetaData cfm=null;
        cfm=Schema.instance.getCFMetaData(ks,tb);
        if(cfm.clusteringColumns().size()>0){
            ByteBuffer l=ByteBufferUtil.bytes(pk);
            dk=new BufferDecoratedKey(Murmur3Partitioner.instance.getToken(l),l);
            NavigableSet<Clustering> sortedClusterings = new TreeSet<>(cfs.metadata.comparator);
            for(String c:ck){
                ByteBuffer cl=ByteBufferUtil.bytes(c);
                sortedClusterings.addAll(Arrays.asList(Clustering.make(cl)));
            }
            navCluster=sortedClusterings;
        }else{
            ByteBuffer l=ByteBufferUtil.bytes(pk);
            dk=new BufferDecoratedKey(Murmur3Partitioner.instance.getToken(l),l);
            navCluster= BTreeSet.of(cfs.metadata.comparator,Clustering.EMPTY);
        }
        System.out.println("kd:"+dk+" clustering:"+navCluster);
        final CFMetaData tmpcfm=cfm;
        int[] count=new int[1];
        printPartitionKey(tmpcfm,dk);
        read(cfs,dk,navCluster,(int)System.currentTimeMillis()/1000).
                forEachRemaining(Unfiltered->{
                    System.out.println("row:"+Unfiltered.toString(tmpcfm));
                    count[0]++;
                });
        System.out.println("record Count:"+count[0]);
        shutdownBackgroundTasks();
    }

    public static void getlocalRecordByHex(String ks,String tb,String hex){
        ByteBuffer bb=ByteBufferUtil.hexToBytes(hex);
        Schema.instance.loadFromDisk(false);
        Keyspace.setInitialized();
        Keyspace.open("system");
        Keyspace.open("system_schema");
        Collection<ColumnFamilyStore> cfsColl=Keyspace.open(ks).getColumnFamilyStores();
        ColumnFamilyStore cfs=null;
        for(ColumnFamilyStore cf:cfsColl){
            if(cf.name.equals(tb))
                cfs=cf;
        }
        DecoratedKey dk=null;
        NavigableSet<Clustering> navCluster=null;
        CFMetaData cfm=null;
        cfm=Schema.instance.getCFMetaData(ks,tb);
        if(cfm.clusteringColumns().size()>0){
            ByteBuffer [] l=split(bb,2);
            dk=new BufferDecoratedKey(Murmur3Partitioner.instance.getToken(l[0]),l[0]);
            ByteBuffer [] cl=split(l[1],cfm.clusteringColumns().size());
            NavigableSet<Clustering> sortedClusterings = new TreeSet<>(cfs.metadata.comparator);
            if (cl.length > 0) {
                sortedClusterings.addAll(Arrays.asList(Clustering.make(cl)));
            }
            navCluster=sortedClusterings;
        }else{
            dk=new BufferDecoratedKey(Murmur3Partitioner.instance.getToken(bb),bb);
            navCluster= BTreeSet.of(cfs.metadata.comparator,Clustering.EMPTY);
        }
        System.out.println("kd:"+dk+" clustering:"+navCluster);
        final CFMetaData tmpcfm=cfm;
        int[] count=new int[1];
        printPartitionKey(tmpcfm,dk);
        read(cfs,dk,navCluster,(int)System.currentTimeMillis()/1000).
                forEachRemaining(Unfiltered->{
                    System.out.println("row:"+Unfiltered.toString(tmpcfm));
                    count[0]++;
                });
        System.out.println("record Count:"+count[0]);
        shutdownBackgroundTasks();
    }

    private static void printPartitionKey(CFMetaData cfm,DecoratedKey key){
        List<ColumnDefinition> columnDefinitions = cfm.partitionKeyColumns();
        AbstractType<?> partitionType=cfm.getKeyValidator();
        ByteBuffer[] components = partitionType instanceof CompositeType
                ? ((CompositeType) partitionType).split(key.getKey())
                : new ByteBuffer[]{key.getKey()};

        for(ColumnDefinition cdf:columnDefinitions){
            String name = cdf.name.toString();
            ByteBuffer value = components[cdf.position()];
            AbstractType<?> valueType = cdf.cellValueType();
            Object va=compose(value,valueType);
            System.out.println(name+":"+va);
        }
    }

    public static Object compose(ByteBuffer value, AbstractType<?> type){
        if(type instanceof SimpleDateType){
            return ((SimpleDateType)type).toTimeInMillis(value);
        }
        return type.compose(value);
    }

    public static UnfilteredRowIterator read(ColumnFamilyStore table, DecoratedKey key,
                                             NavigableSet<Clustering> clusterings,
                                             int nowInSec) {
        ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings, false);
        ColumnFilter columnFilter = ColumnFilter.all(table.metadata);
        SinglePartitionReadCommand cmd = SinglePartitionReadCommand.create(
                table.metadata, nowInSec, key, columnFilter, filter);
        ReadExecutionController controller = ReadUtil.controller(cmd);
        UnfilteredRowIterator it =  cmd.queryMemtableAndDisk(table, controller);
        controller.close();
        return it;
    }

    private static boolean readStatic(ByteBuffer bb)
    {
        if (bb.remaining() < 2)
            return false;

        int header = ByteBufferUtil.getShortLength(bb, bb.position());
        if ((header & 0xFFFF) != 0Xffff)
            return false;

        ByteBufferUtil.readShortLength(bb); // Skip header
        return true;
    }

    public static ByteBuffer[] split(ByteBuffer name,int size)
    {
        // Assume all components, we'll trunk the array afterwards if need be, but
        // most names will be complete.
        ByteBuffer[] l = new ByteBuffer[size];
        ByteBuffer bb = name.duplicate();
        readStatic(bb);
        int i = 0;
        while (bb.remaining() > 0)
        {
            l[i++] = ByteBufferUtil.readBytesWithShortLength(bb);
            bb.get(); // skip end-of-component
        }
        return i == l.length ? l : Arrays.copyOfRange(l, 0, i);
    }
}
