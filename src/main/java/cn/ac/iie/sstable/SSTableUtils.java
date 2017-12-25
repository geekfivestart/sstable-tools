package cn.ac.iie.sstable;

import cn.ac.iie.cassandra.CassandraUtils;
import cn.ac.iie.cassandra.NoSuchKeyspaceException;
import cn.ac.iie.cassandra.NoSuchTableException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import jline.console.ConsoleReader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.*;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FBUtilities;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static cn.ac.iie.cassandra.CassandraUtils.findSchema;
import static cn.ac.iie.cassandra.CassandraUtils.ssTableFromName;
import static cn.ac.iie.utils.InvokeUtils.readPrivate;

/**
 * SSTable操作工具类
 */
public class SSTableUtils {
    private static final Logger logger = LoggerFactory.getLogger(SSTableUtils.class);
    private static final AtomicInteger cfCounter = new AtomicInteger();
    private static String cqlOverride = null;
    private static String FULL_BAR = Strings.repeat("*", 30);
    private static String EMPTY_BAR = Strings.repeat("-", 30);


    /**
     * 选择最佳的来源并获取表元数据，优先获取cql定义的元数据
     * @param ssTablePath SSTable文件
     * @return 返回表元数据
     * @throws IOException IO异常
     * @throws NoSuchFieldException 不存在相应属性异常
     * @throws IllegalAccessException 非法访问权限异常
     */
    private static CFMetaData tableFromBestSource(File ssTablePath) throws IOException, NoSuchFieldException, IllegalAccessException {

        CFMetaData metadata;
        if (!Strings.isNullOrEmpty(cqlOverride)) {
            logger.debug("Using metadata from CQL");
            metadata = CassandraUtils.tableFromCQL(new ByteArrayInputStream(cqlOverride.getBytes()));
        } else {
            InputStream in = findSchema();
            if (in == null) {
                logger.debug("Using metadata from SSTable");
                metadata = SSTableUtils.tableFromSSTable(ssTablePath);
            } else {
                metadata = CassandraUtils.tableFromCQL(in);
            }
        }
        return metadata;
    }


    /**
     * 从SSTable中获取表元数据
     * @param path SSTable文件对象
     * @return 返回sstable元数据
     */
    @SuppressWarnings("unchecked")
    static CFMetaData tableFromSSTable(File path) throws IOException, NoSuchFieldException, IllegalAccessException {
        Preconditions.checkNotNull(path);
        Descriptor desc = Descriptor.fromFilename(path.getAbsolutePath());
        EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);
        Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
        ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
        Preconditions.checkNotNull(validationMetadata, "Validation Metadata could not be resolved, accompanying Statistics.db file must be missing.");
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);
        Preconditions.checkNotNull(header, "Metadata could not be resolved, accompanying Statistics.db file must be missing.");

        IPartitioner partitioner = validationMetadata.partitioner.endsWith("LocalPartitioner") ?
                new LocalPartitioner(header.getKeyType()) :
                FBUtilities.newPartitioner(validationMetadata.partitioner);

        DatabaseDescriptor.setPartitionerUnsafe(partitioner);
        AbstractType<?> keyType = header.getKeyType();
        List<AbstractType<?>> clusteringTypes = header.getClusteringTypes();
        Map<ByteBuffer, AbstractType<?>> staticColumns = header.getStaticColumns();
        Map<ByteBuffer, AbstractType<?>> regularColumns = header.getRegularColumns();
        int id = cfCounter.incrementAndGet();
        CFMetaData.Builder builder = CFMetaData.Builder.create("turtle" + id, "turtles" + id);
        staticColumns.entrySet().forEach(entry ->
                builder.addStaticColumn(UTF8Type.instance.getString(entry.getKey()), entry.getValue()));
        regularColumns.entrySet().forEach(entry ->
                builder.addRegularColumn(UTF8Type.instance.getString(entry.getKey()), entry.getValue()));
        List<AbstractType<?>> partTypes = keyType.getComponents();
        for(int i = 0; i < partTypes.size(); i++) {
            builder.addPartitionKey("partition" + (i > 0 ? i : ""), partTypes.get(i));
        }
        for (int i = 0; i < clusteringTypes.size(); i++) {
            builder.addClusteringColumn("row" + (i > 0 ? i : ""), clusteringTypes.get(i));
        }
        CFMetaData metaData = builder.build();
        Schema.instance.setKeyspaceMetadata(KeyspaceMetadata.create(metaData.ksName, KeyspaceParams.local(),
                Tables.of(metaData), Views.none(), CassandraUtils.getTypes(), Functions.none()));
        return metaData;
    }

    public static void printSSTables(String keyspaceName, String tableName, boolean includeSymbolicLink)
            throws NoSuchKeyspaceException, NoSuchTableException {
        ssTableFromName(keyspaceName, tableName, includeSymbolicLink).forEach(file ->
                System.out.println(String.format("%s%s isSymbolicLink:%b%s",
                TableTransformer.ANSI_PURPLE,
                file.getAbsolutePath(),
                Files.isSymbolicLink(file.toPath()),
                TableTransformer.ANSI_RESET)));
    }

    private static <T> Stream<T> asStream(Iterator<T> iter) {
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iter, Spliterator.IMMUTABLE);
        return StreamSupport.stream(spliterator, false);
    }

    private static String wrapQuiet(String toWrap, boolean color) {
        StringBuilder sb = new StringBuilder();
        if (color) {
            sb.append(TableTransformer.ANSI_WHITE);
        }
        sb.append("(");
        sb.append(toWrap);
        sb.append(")");
        if (color) {
            sb.append(TableTransformer.ANSI_RESET);
        }
        return sb.toString();
    }

    public static String toDateString(long time, TimeUnit unit, boolean color) {
        return wrapQuiet(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(unit.toMillis(time))), color);
    }

    private static String toDurationString(long duration, TimeUnit unit, boolean color) {
        return wrapQuiet(PeriodFormat.getDefault().print(new Duration(unit.toMillis(duration)).toPeriod()), color);
    }

    private static String toByteString(long bytes, boolean si, boolean color) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return wrapQuiet(String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre), color);
    }

    private static class ValuedByteBuffer {
        long value;
        ByteBuffer buffer;

        ValuedByteBuffer(ByteBuffer buffer, long value) {
            this.value = value;
            this.buffer = buffer;
        }

        long getValue() {
            return value;
        }
    }

    private static Comparator<ValuedByteBuffer> VCOMP = Comparator.comparingLong(ValuedByteBuffer::getValue).reversed();

    public static void printStats(String fName, PrintStream out) throws IOException, NoSuchFieldException, IllegalAccessException {
        printStats(fName, out, null);
    }

    private static void printStats(String fName, PrintStream out, ConsoleReader console) throws IOException, NoSuchFieldException, IllegalAccessException {
        boolean color = console == null || console.getTerminal().isAnsiSupported();
        String c = color ? TableTransformer.ANSI_BLUE : "";
        String s = color ? TableTransformer.ANSI_CYAN : "";
        String r = color ? TableTransformer.ANSI_RESET : "";
        if (new File(fName).exists()) {
            Descriptor descriptor = Descriptor.fromFilename(fName);

            Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer().deserialize(descriptor, EnumSet.allOf(MetadataType.class));
            ValidationMetadata validation = (ValidationMetadata) metadata.get(MetadataType.VALIDATION);
            StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);
            CompactionMetadata compaction = (CompactionMetadata) metadata.get(MetadataType.COMPACTION);
            CompressionMetadata compression = null;
            File compressionFile = new File(descriptor.filenameFor(Component.COMPRESSION_INFO));
            if (compressionFile.exists())
                compression = CompressionMetadata.create(fName);
            SerializationHeader.Component header = (SerializationHeader.Component) metadata.get(MetadataType.HEADER);

            CFMetaData cfm = tableFromBestSource(new File(fName));
            SSTableReader reader = SSTableReader.openNoValidation(descriptor, cfm);
            ISSTableScanner scanner = reader.getScanner();
            long bytes = scanner.getLengthInBytes();
            MinMaxPriorityQueue<ValuedByteBuffer> widestPartitions = MinMaxPriorityQueue
                    .orderedBy(VCOMP)
                    .maximumSize(5)
                    .create();
            MinMaxPriorityQueue<ValuedByteBuffer> largestPartitions = MinMaxPriorityQueue
                    .orderedBy(VCOMP)
                    .maximumSize(5)
                    .create();
            MinMaxPriorityQueue<ValuedByteBuffer> mostTombstones = MinMaxPriorityQueue
                    .orderedBy(VCOMP)
                    .maximumSize(5)
                    .create();
            long partitionCount = 0;
            long rowCount = 0;
            long tombstoneCount = 0;
            long cellCount = 0;
            double totalCells = stats.totalColumnsSet;
            int lastPercent = 0;
            while (scanner.hasNext()) {
                UnfilteredRowIterator partition = scanner.next();

                long pSize = 0;
                long pCount = 0;
                int pTombCount = 0;
                partitionCount++;
                if (!partition.staticRow().isEmpty()) {
                    rowCount++;
                    pCount++;
                    pSize += partition.staticRow().dataSize();
                }
                if (!partition.partitionLevelDeletion().isLive()) {
                    tombstoneCount++;
                    pTombCount++;
                }
                while (partition.hasNext()) {
                    Unfiltered unfiltered = partition.next();
                    switch (unfiltered.kind()) {
                        case ROW:
                            rowCount++;
                            Row row = (Row) unfiltered;
                            pSize += row.dataSize();
                            pCount++;
                            for (Cell cell : row.cells()) {
                                cellCount++;
                                double percentComplete = Math.min(1.0, cellCount / totalCells);
                                if (lastPercent != (int) (percentComplete * 100)) {
                                    lastPercent = (int) (percentComplete * 100);
                                    int cols = (int) (percentComplete * 30);
                                    System.out.printf("\r%sAnalyzing SSTable...  %s%s%s %s(%%%s)", c, s, FULL_BAR.substring(30 - cols), EMPTY_BAR.substring(cols), r, (int) (percentComplete * 100));
                                    System.out.flush();
                                }
                                if (cell.isTombstone()) {
                                    tombstoneCount++;
                                    pTombCount++;
                                }
                            }
                            break;
                        case RANGE_TOMBSTONE_MARKER:
                            tombstoneCount++;
                            pTombCount++;
                            break;
                    }
                }
                widestPartitions.add(new ValuedByteBuffer(partition.partitionKey().getKey(), pCount));
                largestPartitions.add(new ValuedByteBuffer(partition.partitionKey().getKey(), pSize));
                mostTombstones.add(new ValuedByteBuffer(partition.partitionKey().getKey(), pTombCount));
            }
            out.printf("\r%80s\r", " ");
            out.printf("%sPartitions%s:%s %s%n", c, s, r, partitionCount);
            out.printf("%sRows%s:%s %s%n", c, s, r, rowCount);
            out.printf("%sTombstones%s:%s %s%n", c, s, r, tombstoneCount);
            out.printf("%sCells%s:%s %s%n", c, s, r, cellCount);
            out.printf("%sWidest Partitions%s:%s%n", c, s, r);
            asStream(widestPartitions.iterator()).sorted(VCOMP).forEach(p ->
                    out.printf("%s   [%s%s%s]%s %s%n", s, r, cfm.getKeyValidator().getString(p.buffer),
                            s, r, p.value));
            out.printf("%sLargest Partitions%s:%s%n", c, s, r);
            asStream(largestPartitions.iterator()).sorted(VCOMP).forEach(p ->
                    out.printf("%s   [%s%s%s]%s %s %s%n", s, r, cfm.getKeyValidator().getString(p.buffer),
                            s, r, p.value, toByteString(p.value, true, color)));
            out.printf("%sTombstone Leaders%s:%s%n", c, s, r);
            asStream(mostTombstones.iterator()).sorted(VCOMP).forEach(p -> {
                if (p.value > 0) {
                    out.printf("%s   [%s%s%s]%s %s%n", s, r, cfm.getKeyValidator().getString(p.buffer), s, r, p.value);
                }
            });

            @SuppressWarnings("unchecked")
            List<AbstractType<?>> clusteringTypes = (List<AbstractType<?>>) readPrivate(header, "clusteringTypes");
            if (validation != null) {
                out.printf("%sPartitioner%s:%s %s%n", c, s, r, validation.partitioner);
                out.printf("%sBloom Filter FP chance%s:%s %f%n", c, s, r, validation.bloomFilterFPChance);
            }
            out.printf("%sSize%s:%s %s %s %n", c, s, r, bytes, toByteString(bytes, true, color));
            out.printf("%sCompressor%s:%s %s%n", c, s, r, compression != null ? compression.compressor().getClass().getName() : "-");
            if (compression != null)
                out.printf("%s  Compression ratio%s:%s %s%n", c, s, r, stats.compressionRatio);

            out.printf("%sMinimum timestamp%s:%s %s %s%n", c, s, r, stats.minTimestamp, toDateString(stats.minTimestamp, TimeUnit.MICROSECONDS, color));
            out.printf("%sMaximum timestamp%s:%s %s %s%n", c, s, r, stats.maxTimestamp, toDateString(stats.maxTimestamp, TimeUnit.MICROSECONDS, color));

            out.printf("%sSSTable min local deletion time%s:%s %s %s%n", c, s, r, stats.minLocalDeletionTime, toDateString(stats.minLocalDeletionTime, TimeUnit.SECONDS, color));
            out.printf("%sSSTable max local deletion time%s:%s %s %s%n", c, s, r, stats.maxLocalDeletionTime, toDateString(stats.maxLocalDeletionTime, TimeUnit.SECONDS, color));

            out.printf("%sTTL min%s:%s %s %s%n", c, s, r, stats.minTTL, toDurationString(stats.minTTL, TimeUnit.SECONDS, color));
            out.printf("%sTTL max%s:%s %s %s%n", c, s, r, stats.maxTTL, toDurationString(stats.maxTTL, TimeUnit.SECONDS, color));
            if (header != null && clusteringTypes.size() == stats.minClusteringValues.size()) {
                List<ByteBuffer> minClusteringValues = stats.minClusteringValues;
                List<ByteBuffer> maxClusteringValues = stats.maxClusteringValues;
                String[] minValues = new String[clusteringTypes.size()];
                String[] maxValues = new String[clusteringTypes.size()];
                for (int i = 0; i < clusteringTypes.size(); i++) {
                    minValues[i] = clusteringTypes.get(i).getString(minClusteringValues.get(i));
                    maxValues[i] = clusteringTypes.get(i).getString(maxClusteringValues.get(i));
                }
                out.printf("%sminClustringValues%s:%s %s%n", c, s, r, Arrays.toString(minValues));
                out.printf("%smaxClustringValues%s:%s %s%n", c, s, r, Arrays.toString(maxValues));
            }
            out.printf("%sEstimated droppable tombstones%s:%s %s%n", c, s, r, stats.getEstimatedDroppableTombstoneRatio((int) (System.currentTimeMillis() / 1000)));
            out.printf("%sSSTable Level%s:%s %d%n", c, s, r, stats.sstableLevel);
            out.printf("%sRepaired at%s:%s %d %s%n", c, s, r, stats.repairedAt, toDateString(stats.repairedAt, TimeUnit.MILLISECONDS, color));
            out.printf("  %sLower bound%s:%s %s%n", c, s, r, stats.commitLogIntervals.lowerBound());
            out.printf("  %sUpper bound%s:%s %s%n", c, s, r, stats.commitLogIntervals.upperBound());
            out.printf("%stotalColumnsSet%s:%s %s%n", c, s, r, stats.totalColumnsSet);
            out.printf("%stotalRows%s:%s %s%n", c, s, r, stats.totalRows);
            out.printf("%sEstimated tombstone drop times%s:%s%n", c, s, r);

            TermHistogram h = new TermHistogram(stats.estimatedTombstoneDropTime.getAsMap().entrySet());
            String bcolor = color ? "\u001B[36m" : "";
            String reset = color ? "\u001B[0m" : "";
            String histoColor = color ? "\u001B[37m" : "";
            out.printf("%s  %-" + h.maxValueLength + "s                       | %-" + h.maxCountLength + "s   %%   Histogram %n", bcolor, "Value", "Count");
            stats.estimatedTombstoneDropTime.getAsMap().entrySet().forEach(e -> {
                String histo = h.asciiHistogram(e.getValue()[0], 30);
                out.printf(reset +
                                "  %-" + h.maxValueLength + "d %s %s|%s %" + h.maxCountLength + "s %s %s%s %n",
                        e.getKey().longValue(), toDateString(e.getKey().intValue(), TimeUnit.SECONDS, color),
                        bcolor, reset,
                        e.getValue()[0],
                        wrapQuiet(String.format("%3s", (int) (100 * (e.getValue()[0] / h.sum))), color),
                        histoColor,
                        histo);
            });

            out.printf("%sEstimated partition size%s:%s%n", c, s, r);
            TermHistogram.printHistogram(stats.estimatedPartitionSize, out, color);

            out.printf("%sEstimated column count%s:%s%n", c, s, r);
            TermHistogram.printHistogram(stats.estimatedColumnCount, out, color);
            if (compaction != null) {
                out.printf("%sEstimated cardinality%s:%s %s%n", c, s, r, compaction.cardinalityEstimator.cardinality());
            }
            if (header != null) {
                EncodingStats encodingStats = header.getEncodingStats();
                AbstractType<?> keyType = header.getKeyType();
                Map<ByteBuffer, AbstractType<?>> staticColumns = header.getStaticColumns();
                Map<ByteBuffer, AbstractType<?>> regularColumns = header.getRegularColumns();
                Map<String, String> statics = staticColumns.entrySet().stream()
                        .collect(Collectors.toMap(
                                e -> UTF8Type.instance.getString(e.getKey()),
                                e -> e.getValue().toString()));
                Map<String, String> regulars = regularColumns.entrySet().stream()
                        .collect(Collectors.toMap(
                                e -> UTF8Type.instance.getString(e.getKey()),
                                e -> e.getValue().toString()));

                out.printf("%sEncodingStats minTTL%s:%s %s %s%n", c, s, r, encodingStats.minTTL, toDurationString(encodingStats.minTTL, TimeUnit.SECONDS, color));
                out.printf("%sEncodingStats minLocalDeletionTime%s:%s %s %s%n", c, s, r, encodingStats.minLocalDeletionTime, toDateString(encodingStats.minLocalDeletionTime, TimeUnit.MILLISECONDS, color));
                out.printf("%sEncodingStats minTimestamp%s:%s %s %s%n", c, s, r, encodingStats.minTimestamp, toDateString(encodingStats.minTimestamp, TimeUnit.MICROSECONDS, color));
                out.printf("%sKeyType%s:%s %s%n", c, s, r, keyType.toString());
                out.printf("%sClusteringTypes%s:%s %s%n", c, s, r, clusteringTypes.toString());
                out.printf("%sStaticColumns%s:%s {%s}%n", c, s, r, FBUtilities.toString(statics));
                out.printf("%sRegularColumns%s:%s {%s}%n", c, s, r, FBUtilities.toString(regulars));
            }
        }
    }

    public static void printTimestampRange(String fName, PrintStream out, ConsoleReader console) throws IOException, NoSuchFieldException, IllegalAccessException {
        boolean color = console == null || console.getTerminal().isAnsiSupported();
        String c = color ? TableTransformer.ANSI_BLUE : "";
        String s = color ? TableTransformer.ANSI_CYAN : "";
        String r = color ? TableTransformer.ANSI_RESET : "";
        if (new File(fName).exists()) {
            Descriptor descriptor = Descriptor.fromFilename(fName);
            Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer().deserialize(descriptor, EnumSet.allOf(MetadataType.class));
            StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);

            if (stats != null) {

                out.printf("%sMinimum timestamp%s:%s %s %s%n", c, s, r, stats.minTimestamp, toDateString(stats.minTimestamp, TimeUnit.MICROSECONDS, color));
                out.printf("%sMaximum timestamp%s:%s %s %s%n", c, s, r, stats.maxTimestamp, toDateString(stats.maxTimestamp, TimeUnit.MICROSECONDS, color));
            }
        }
    }

    public static Long maxTimestamp(String ssTablePath) throws IOException {
        Descriptor descriptor = Descriptor.fromFilename(ssTablePath);
        Map<MetadataType, MetadataComponent> metadata = descriptor.
                getMetadataSerializer().
                deserialize(descriptor, EnumSet.allOf(MetadataType.class));
        StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);
        return stats.maxTimestamp;

    }

    private static final TreeMap<Double, String> bars = new TreeMap<Double, String>() {{
        this.put(7.0 / 8.0, "▉"); // 7/8ths left block
        this.put(3.0 / 4.0, "▊"); // 3/4th block
        this.put(5.0 / 8.0, "▋"); // five eighths
        this.put(3.0 / 8.0, "▍"); // three eighths
        this.put(1.0 / 4.0, "▎");
        this.put(1.0 / 8.0, "▏");
    }};

    private static class TermHistogram {
        long max;
        long min;
        double sum;
        int maxCountLength = 5;
        int maxValueLength = 5;

        static void printHistogram(EstimatedHistogram histogram, PrintStream out, boolean colors) {
            String bcolor = colors ? "\u001B[36m" : "";
            String reset = colors ? "\u001B[0m" : "";
            String histoColor = colors ? "\u001B[37m" : "";

            TermHistogram h = new TermHistogram(histogram);
            out.printf("%s  %-" + h.maxValueLength + "s | %-" + h.maxCountLength + "s   %%   Histogram %n", bcolor, "Value", "Count");
            long[] counts = histogram.getBuckets(false);
            long[] offsets = histogram.getBucketOffsets();
            for (int i = 0; i < counts.length; i++) {
                if (counts[i] > 0) {
                    String histo = h.asciiHistogram(counts[i], 30);
                    out.printf(reset +
                                    "  %-" + h.maxValueLength + "d %s|%s %" + h.maxCountLength + "s %s %s%s %n",
                            offsets[i],
                            bcolor, reset,
                            counts[i],
                            wrapQuiet(String.format("%3s", (int) (100 * ((double) counts[i] / h.sum))), true),
                            histoColor,
                            histo);
                }
            }
        }

        TermHistogram(Set<Map.Entry<Number, long[]>> histogram) {
            histogram.forEach(e -> {
                max = Math.max(max, e.getValue()[0]);
                min = Math.min(min, e.getValue()[0]);
                sum += e.getValue()[0];
                maxCountLength = Math.max(maxCountLength, ("" + e.getValue()[0]).length());
                maxValueLength = Math.max(maxValueLength, ("" + e.getKey().longValue()).length());
            });
        }

        TermHistogram(EstimatedHistogram histogram) {
            long[] counts = histogram.getBuckets(false);
            for (int i = 0; i < counts.length; i++) {
                long e = counts[i];
                if (e > 0) {
                    max = Math.max(max, e);
                    min = Math.min(min, e);
                    sum += e;
                    maxCountLength = Math.max(maxCountLength, ("" + e).length());
                    maxValueLength = Math.max(maxValueLength, ("" + histogram.getBucketOffsets()[i]).length());
                }
            }
        }

        String asciiHistogram(long count, int length) {
            logger.info("count:{} length:{} max:{}",count,length,max);
            StringBuilder sb = new StringBuilder();
            int intWidth = (int) (count * 1.0 / max * length);
            logger.info("intWidth:{}",intWidth);
            double remainderWidth = (count * 1.0 / max * length) - intWidth;
            sb.append(Strings.repeat("▉", intWidth));
            if (bars.floorKey(remainderWidth) != null) {
                sb.append("").append(bars.get(bars.floorKey(remainderWidth)));
            }
            return sb.toString();
        }
    }

}
