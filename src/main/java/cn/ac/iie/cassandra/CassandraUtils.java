package cn.ac.iie.cassandra;

import cn.ac.iie.drive.Driver;
import cn.ac.iie.sstable.SSTableUtils;
import cn.ac.iie.utils.InvokeUtils;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.jeffjirsa.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.DateTieredCompactionStrategy;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
                logger.info("共启动{}个后台任务，其中{}个正在执行中，{}个已完成",
                        ScheduledExecutors.optionalTasks.getTaskCount(),
                        ScheduledExecutors.optionalTasks.getActiveCount(),
                        ScheduledExecutors.optionalTasks.getCompletedTaskCount());
                ScheduledExecutors.optionalTasks.getQueue().forEach(runnable -> {
                    if (runnable instanceof RunnableScheduledFuture) {
                        RunnableScheduledFuture task = (RunnableScheduledFuture) runnable;
                        if (task.cancel(true)) {
                            logger.info("已停止一个{}类型的任务", task.getClass().getName());
                        }
                    }
                });
                ScheduledExecutors.optionalTasks.shutdown();
                logger.info("共启动{}个后台任务，其中{}个正在执行中，{}个已完成",
                        ScheduledExecutors.optionalTasks.getTaskCount(),
                        ScheduledExecutors.optionalTasks.getActiveCount(),
                        ScheduledExecutors.optionalTasks.getCompletedTaskCount());
                try {
                    CommitLog.instance.shutdownBlocking();
                    logger.info("停止commit_log_service");
                    ColumnFamilyStore.shutdownPostFlushExecutor();
                    logger.info("停止mem_table_post_flush");
                    MessagingService.instance().shutdown();
                    logger.info("停止message_service");
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
                logger.error("不存在keyspace: {}", keyspaceName);
                throw new NoSuchKeyspaceException(String.format("不存在keyspace: %s", keyspaceName));
            }
            logger.info("从集群中获取了keyspace（{}）的元数据：{}", keyspaceName, metadata.toString());
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
            logger.error("表不存在： {}", tableName);
            throw new NoSuchTableException(String.format("表不存在: %s", tableName));
        }
        logger.info("从keyspace（{}）元数据中获取了table（{}）的元数据：{}",
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
            logger.warn("在使用sstable-tools工具时请删除-ea选项");
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
                throw new Exception("不存在max_window_size_seconds属性，请确认schema是否正确");
            }
        } else if(TimeWindowCompactionStrategy.class.isAssignableFrom(metaData.params.compaction.klass())){
            // 如果合并策略设置为TimeWindowCompactionStrategy，
            // 则元数据中应包含compaction_window_size及compaction_window_unit
            Map<String, String> params = metaData.params.compaction.options();
            String size;
            String unit;
            if (params.containsKey("compaction_window_size")) {
                size = params.get("compaction_window_size");
            } else {
                throw new Exception("不存在compaction_window_size属性，请确认schema是否正确");
            }
            if(params.containsKey("compaction_window_unit")){
                unit = params.get("compaction_window_unit");
            } else{

                throw new Exception("不存在compaction_window_unit属性，请确认schema是否正确");
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
                    throw new Exception(String.format("compaction_window_unit设置不正确：%s", unit));
            }
            maxWindowSizeSecondsMap.put(ksTableName, secondDigit);
            return secondDigit;
        } else {
            throw new Exception(String.format("表%s.%s的合并策略应为DateTieredCompactionStrategy、TimeWindowCompactionStrategy,而不是%s",
                    keyspaceName, tableName, InvokeUtils.genericSuperclass(metaData.params.compaction.klass())));
        }
    }

    /**
     * 根据SSTable文件获取该文件中的记录的最大时间戳
     * @param ssTablePath SSTable文件路径
     * @return 返回最大时间戳
     */
    private static Long maxTimestampOfSSTable(String ssTablePath) throws Exception {
        // 从SSTable元数据中获取
        Long maxTimestamp = SSTableUtils.maxTimestamp(ssTablePath);
        if (maxTimestamp <= 0) {
            throw new Exception(String.format("文件%s的最大时间戳不正确", ssTablePath));
        }
        logger.info("获取STable文件： {} 的最大时间戳：{}，格式化时间为：{}",
                ssTablePath, maxTimestamp, SSTableUtils.toDateString(maxTimestamp, TimeUnit.MICROSECONDS, false));
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
}
