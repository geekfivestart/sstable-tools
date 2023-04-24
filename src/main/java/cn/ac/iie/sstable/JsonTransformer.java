package cn.ac.iie.sstable;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.impl.Indenter;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import org.codehaus.jackson.util.DefaultPrettyPrinter.NopIndenter;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JsonTransformer
{

    private static final Logger logger = LoggerFactory.getLogger(JsonTransformer.class);

    private static final JsonFactory jsonFactory = new JsonFactory();

    private final JsonGenerator json;

    private final CompactIndenter objectIndenter = new CompactIndenter();

    private final CompactIndenter arrayIndenter = new CompactIndenter();

    private final CFMetaData metadata;

    private final ISSTableScanner currentScanner;

    private boolean rawTime = false;

    private long currentPosition = 0;

    private String outfilePrefix;

    private int postfix=0;

    private File outputFile;
    private BufferedWriter bw;
    private JsonTransformer(JsonGenerator json, ISSTableScanner currentScanner, boolean rawTime, CFMetaData metadata,String outfilePrefix) throws IOException {
        this.json = json;
        this.metadata = metadata;
        this.currentScanner = currentScanner;
        this.rawTime = rawTime;
        this.outfilePrefix=outfilePrefix;

        DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
        objectIndenter.setCompact(true);
        arrayIndenter.setCompact(true);
        prettyPrinter.indentObjectsWith(objectIndenter);
        prettyPrinter.indentArraysWith(arrayIndenter);
        json.setPrettyPrinter(prettyPrinter);
        outputFileCheck();
    }

    private void outputFileCheck() throws IOException {
       if(outputFile==null || outputFile.length() > SSTableExport.fileSize){
           outputFile=getValidOutputFile();
           if(bw!=null){
              bw.close();
           }
           bw=new BufferedWriter(new FileWriter(outputFile,SSTableExport.append_flag));
       }
    }


    private File getValidOutputFile(){
        while(true){
            String tmp=outfilePrefix+"-"+String.format("%06d",postfix++);
            if((!new File(tmp).exists())||
                    (SSTableExport.append_flag&&new File(tmp).length()<SSTableExport.fileSize)){
                return new File(tmp);
            }
        }
    }

    public static void toJson(ISSTableScanner currentScanner, Stream<UnfilteredRowIterator> partitions, boolean rawTime, CFMetaData metadata, OutputStream out,String outfilePrefix)
            throws IOException
    {
        try (JsonGenerator json = jsonFactory.createJsonGenerator(new OutputStreamWriter(out, StandardCharsets.UTF_8)))
        {
            JsonTransformer transformer = new JsonTransformer(json, currentScanner, rawTime, metadata,outfilePrefix);
            //json.writeStartArray();
            partitions.forEach(transformer::serializePartition);
            transformer.bw.close();
            //json.writeEndArray();
        }
    }

    public static void keysToJson(ISSTableScanner currentScanner, Stream<DecoratedKey> keys, boolean rawTime, CFMetaData metadata, OutputStream out) throws IOException
    {
        try (JsonGenerator json = jsonFactory.createJsonGenerator(new OutputStreamWriter(out, StandardCharsets.UTF_8)))
        {
            JsonTransformer transformer = new JsonTransformer(json, currentScanner, rawTime, metadata,null);
            json.writeStartArray();
            keys.forEach(transformer::serializePartitionKey);
            json.writeEndArray();
        }
    }

    private void updatePosition()
    {
        this.currentPosition = currentScanner.getCurrentPosition();
    }

    private boolean isNumericType(AbstractType keyValidator){
        if(keyValidator instanceof LongType || keyValidator instanceof ShortType ||
                keyValidator instanceof IntegerType || keyValidator instanceof Int32Type ||
                keyValidator instanceof FloatType || keyValidator instanceof DoubleType ||
                keyValidator instanceof DecimalType)
            return true;
        return false;
    }

    private String quoting(String str){
        return "\""+str+"\"";
    }

    private String serializePartitionKeyToString(DecoratedKey key) {
        StringBuilder ret = new StringBuilder();
        AbstractType<?> keyValidator = metadata.getKeyValidator();

        if (keyValidator instanceof CompositeType) {
            // if a composite type, the partition has multiple keys.
            CompositeType compositeType = (CompositeType) keyValidator;
            ByteBuffer keyBytes = key.getKey().duplicate();
            // Skip static data if it exists.
            if (keyBytes.remaining() >= 2) {
                int header = ByteBufferUtil.getShortLength(keyBytes, keyBytes.position());
                if ((header & 0xFFFF) == 0xFFFF) {
                    ByteBufferUtil.readShortLength(keyBytes);
                }
            }

            int i = 0;
            while (keyBytes.remaining() > 0 && i < compositeType.getComponents().size()) {
                AbstractType<?> colType = compositeType.getComponents().get(i);

                ByteBuffer value = ByteBufferUtil.readBytesWithShortLength(keyBytes);
                String colValue = colType.getString(value);

                String pkn = SSTableExport.pkName[i];
                //if(SSTableExport.selectColumn.size()==0
                //        ||SSTableExport.selectColumn.containsKey(pkn)) {
                    if (ret.length() > 0) {
                        ret.append(",");
                    }
                    ret.append(quoting(pkn) + ":");

                    if (isNumericType(colType)) {
                        ret.append(colValue);
                    } else {
                        ret.append(quoting(colValue));
                    }
                //json.writeString(colValue);

                byte b = keyBytes.get();
                if (b != 0) {
                    break;
                }
                ++i;
            }
        } else {
            // if not a composite type, assume a single column partition key.
            assert metadata.partitionKeyColumns().size() == 1;
            String value = keyValidator.getString(key.getKey());
            //String pkn = metadata.partitionKeyColumns().get(0).name.toString();
            String pkn = SSTableExport.pkName[0];
            ret.append(quoting(pkn) + ":");
            if (isNumericType(keyValidator)) {
                ret.append(value);
            } else {
                ret.append(quoting(value));
            }
        }

        return ret.toString();
    }


    @Deprecated
    private void serializePartitionKey(DecoratedKey key)
    {
        AbstractType<?> keyValidator = metadata.getKeyValidator();
        objectIndenter.setCompact(true);
        try
        {
            arrayIndenter.setCompact(true);
            json.writeStartArray();
            if (keyValidator instanceof CompositeType)
            {
                // if a composite type, the partition has multiple keys.
                CompositeType compositeType = (CompositeType) keyValidator;
                ByteBuffer keyBytes = key.getKey().duplicate();
                // Skip static data if it exists.
                if (keyBytes.remaining() >= 2)
                {
                    int header = ByteBufferUtil.getShortLength(keyBytes, keyBytes.position());
                    if ((header & 0xFFFF) == 0xFFFF)
                    {
                        ByteBufferUtil.readShortLength(keyBytes);
                    }
                }

                int i = 0;
                while (keyBytes.remaining() > 0 && i < compositeType.getComponents().size())
                {
                    AbstractType<?> colType = compositeType.getComponents().get(i);

                    ByteBuffer value = ByteBufferUtil.readBytesWithShortLength(keyBytes);
                    String colValue = colType.getString(value);

                    json.writeString(colValue);

                    byte b = keyBytes.get();
                    if (b != 0)
                    {
                        break;
                    }
                    ++i;
                }
            }
            else
            {
                // if not a composite type, assume a single column partition key.
                assert metadata.partitionKeyColumns().size() == 1;
                json.writeString(keyValidator.getString(key.getKey()));
            }
            json.writeEndArray();
            objectIndenter.setCompact(false);
            arrayIndenter.setCompact(false);
        }
        catch (IOException e)
        {
            logger.error("Failure serializing partition key.", e);
        }
    }

    private void serializePartition(UnfilteredRowIterator partition) {
        if(SSTableExport.tkRange.size()>0){
            boolean flag=false;
            for(Range<Token> r:SSTableExport.tkRange){
                if(r.contains(partition.partitionKey().getToken())){
                    flag=true;
                    break;
                }
            }
            if(flag==false){
                return ;
            }
        }

        StringBuilder key = new StringBuilder();
        if (!partition.partitionLevelDeletion().isLive())
            return;

        String str = serializePartitionKeyToString(partition.partitionKey());

        if (Strings.isNullOrEmpty(str)) {
            return;
        }
        key.append(str);

        if (partition.hasNext() || partition.staticRow() != null) {
            updatePosition();
            if (!partition.staticRow().isEmpty()) {
                String tmp = serializeRowToString(partition.staticRow());
                if (!Strings.isNullOrEmpty(tmp)){
                    key.append(",");
                    key.append(tmp);
                }
            }
            Unfiltered unfiltered;
            updatePosition();
            while (partition.hasNext()) {
                unfiltered = partition.next();
                StringBuilder rowsb=new StringBuilder();
                if (unfiltered instanceof Row) {
                    String tmp=serializeRowToString((Row) unfiltered);
                    if (!Strings.isNullOrEmpty(tmp)){
                        rowsb.append(",");
                        rowsb.append(tmp);
                    }
                }
                updatePosition();
                try {
                    outputFileCheck();
                    if(Strings.isNullOrEmpty(rowsb.toString())){
                        continue;
                    }
                    String st="{"+key.toString()+rowsb.toString()+"}";
                    bw.write(st);
                    bw.newLine();
                    //System.out.println("{"+key.toString()+rowsb.toString()+"}");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private String serializeRowToString(Row row){
        StringBuilder sb=new StringBuilder();
        try
        {
            if (!row.deletion().isLive())
            {
                return "";
            }

            // Only print clustering information for non-static rows.
            if (!row.isStatic())
            {
                String st=serializeClusteringToString(row.clustering());
                sb.append(st);
            }

            LivenessInfo liveInfo = row.primaryKeyLivenessInfo();

            // If this is a deletion, indicate that, otherwise write cells.

            for (ColumnData cd : row)
            {
                String tmp=serializeColumnDataToString(cd, liveInfo);
                if(!Strings.isNullOrEmpty(tmp)){
                    if(sb.length()>0)
                        sb.append(",");
                    sb.append(tmp);
                }
            }
        }
        catch (IOException e)
        {
            logger.error("Fatal error parsing row.", e);
        }
        return sb.toString();
    }

    @Deprecated
    private void serializeRow(Row row)
    {
        try
        {
            //json.writeStartObject();
            //String rowType = row.isStatic() ? "static_block" : "row";
            //json.writeFieldName("type");
            //json.writeString(rowType);
            //json.writeNumberField("position", this.currentPosition);

            // Only print clustering information for non-static rows.
            if (!row.isStatic())
            {
                serializeClustering(row.clustering());
            }

            LivenessInfo liveInfo = row.primaryKeyLivenessInfo();
            if (!liveInfo.isEmpty())
            {
                //objectIndenter.setCompact(false);
                //json.writeFieldName("liveness_info");
                //objectIndenter.setCompact(true);
                //json.writeStartObject();
                //json.writeFieldName("tstamp");
                //json.writeString(dateString(TimeUnit.MICROSECONDS, liveInfo.timestamp()));
                if (liveInfo.isExpiring())
                {
                    json.writeNumberField("ttl", liveInfo.ttl());
                    json.writeFieldName("expires_at");
                    json.writeString(dateString(TimeUnit.SECONDS, liveInfo.localExpirationTime()));
                    json.writeFieldName("expired");
                    json.writeBoolean(liveInfo.localExpirationTime() < (System.currentTimeMillis() / 1000));
                }
                json.writeEndObject();
                //objectIndenter.setCompact(false);
            }

            // If this is a deletion, indicate that, otherwise write cells.
            if (!row.deletion().isLive())
            {
                serializeDeletion(row.deletion().time());
            }
            json.writeFieldName("cells");
            json.writeStartArray();
            for (ColumnData cd : row)
            {
                serializeColumnData(cd, liveInfo);
            }
            json.writeEndArray();
            json.writeEndObject();
        }
        catch (IOException e)
        {
            logger.error("Fatal error parsing row.", e);
        }
    }

    private void serializeTombstone(RangeTombstoneMarker tombstone)
    {
        try
        {
            json.writeStartObject();
            json.writeFieldName("type");

            if (tombstone instanceof RangeTombstoneBoundMarker)
            {
                json.writeString("range_tombstone_bound");
                RangeTombstoneBoundMarker bm = (RangeTombstoneBoundMarker) tombstone;
                serializeBound(bm.clustering(), bm.deletionTime());
            }
            else
            {
                assert tombstone instanceof RangeTombstoneBoundaryMarker;
                json.writeString("range_tombstone_boundary");
                RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker) tombstone;
                serializeBound(bm.openBound(false), bm.openDeletionTime(false));
                serializeBound(bm.closeBound(false), bm.closeDeletionTime(false));
            }
            json.writeEndObject();
            //objectIndenter.setCompact(false);
        }
        catch (IOException e)
        {
            logger.error("Failure parsing tombstone.", e);
        }
    }

    private void serializeBound(ClusteringBound bound, DeletionTime deletionTime) throws IOException
    {
        json.writeFieldName(bound.isStart() ? "start" : "end");
        json.writeStartObject();
        json.writeFieldName("type");
        json.writeString(bound.isInclusive() ? "inclusive" : "exclusive");
        serializeClustering(bound.clustering());
        serializeDeletion(deletionTime);
        json.writeEndObject();
    }

    private String serializeClusteringToString(ClusteringPrefix clustering) throws IOException
    {
        StringBuilder sb=new StringBuilder();
        if (clustering.size() > 0)
        {
            List<ColumnDefinition> clusteringColumns = metadata.clusteringColumns();
            for (int i = 0; i < clusteringColumns.size(); i++)
            {
                ColumnDefinition column = clusteringColumns.get(i);
                if (i < clustering.size())
                {
                    if(sb.length()>0)
                        sb.append(",");
                    String value=column.cellValueType().toJSONString(clustering.get(i), ProtocolVersion.CURRENT);
                    String cName=SSTableExport.ckName[i];
                    sb.append(quoting(cName)+":");
                    sb.append(value);
//                    if(isNumericType(column.cellValueType())){
//                        sb.append(value);
//                    }else{
//                        sb.append(quoting(value));
//                    }
                }
            }
        }
        return sb.toString();
    }

    @Deprecated
    private void serializeClustering(ClusteringPrefix clustering) throws IOException
    {
        if (clustering.size() > 0)
        {
            json.writeFieldName("clustering");
            //objectIndenter.setCompact(true);
            json.writeStartArray();
            //arrayIndenter.setCompact(true);
            List<ColumnDefinition> clusteringColumns = metadata.clusteringColumns();
            for (int i = 0; i < clusteringColumns.size(); i++)
            {
                ColumnDefinition column = clusteringColumns.get(i);
                if (i >= clustering.size())
                {
                    json.writeString("*");
                }
                else
                {
                    json.writeRawValue(column.cellValueType().toJSONString(clustering.get(i), ProtocolVersion.CURRENT));
                }
            }
            json.writeEndArray();
            //objectIndenter.setCompact(false);
            //arrayIndenter.setCompact(false);
        }
    }

    private void serializeDeletion(DeletionTime deletion) throws IOException
    {
        json.writeFieldName("deletion_info");
        //objectIndenter.setCompact(true);
        json.writeStartObject();
        json.writeFieldName("marked_deleted");
        json.writeString(dateString(TimeUnit.MICROSECONDS, deletion.markedForDeleteAt()));
        json.writeFieldName("local_delete_time");
        json.writeString(dateString(TimeUnit.SECONDS, deletion.localDeletionTime()));
        json.writeEndObject();
        //objectIndenter.setCompact(false);
    }

    private String serializeColumnDataToString(ColumnData cd, LivenessInfo liveInfo)
    {
        StringBuilder sb=new StringBuilder();
        if (cd.column().isSimple())
        {
            String st=serializeCellToString((Cell) cd, liveInfo);
            if(Strings.isNullOrEmpty(st)){
                return "";
            }else
                return st;
        }
        else
        {
            ComplexColumnData complexData = (ComplexColumnData) cd;
            String st="";
            for (Cell cell : complexData){
                String tmp=serializeCellToString(cell, liveInfo);
                if(Strings.isNullOrEmpty(tmp)) {
                    if(st.length()>0)
                        st+=","+tmp;
                    else
                        st+=tmp;
                }
            }
            if(Strings.isNullOrEmpty(st))
                return "";
            else
                return st;
        }
    }

    @Deprecated
    private void serializeColumnData(ColumnData cd, LivenessInfo liveInfo)
    {
        if (cd.column().isSimple())
        {
            serializeCell((Cell) cd, liveInfo);
        }
        else
        {
            ComplexColumnData complexData = (ComplexColumnData) cd;
            if (!complexData.complexDeletion().isLive())
            {
                try
                {
                    //objectIndenter.setCompact(true);
                    json.writeStartObject();
                    json.writeFieldName("name");
                    json.writeString(cd.column().name.toCQLString());
                    serializeDeletion(complexData.complexDeletion());
                    //objectIndenter.setCompact(true);
                    json.writeEndObject();
                    //objectIndenter.setCompact(false);
                }
                catch (IOException e)
                {
                    logger.error("Failure parsing ColumnData.", e);
                }
            }
            for (Cell cell : complexData){
                serializeCell(cell, liveInfo);
            }
        }
    }


    private String serializeCellToString(Cell cell, LivenessInfo liveInfo) {
        StringBuilder sb = new StringBuilder();
        if (cell.isTombstone()) {
            return "";
        }
        AbstractType<?> type = cell.column().type;
        AbstractType<?> cellType = null;
        String colName = cell.column().name.toCQLString();
        if(SSTableExport.selectColumn.size()!=0 && !SSTableExport.selectColumn.containsKey(colName)){
            return "";
        }
        sb.append(quoting(colName) + ":");
        if (type.isCollection() && type.isMultiCell()) // non-frozen collection
        {
            cellType = cell.column().cellValueType();
        } else if (type.isUDT() && type.isMultiCell()) // non-frozen udt
        {
            Short fieldPosition = ((UserType) type).nameComparator().compose(cell.path().get(0));
            cellType = ((UserType) type).fieldType(fieldPosition);
        } else {
            cellType = cell.column().cellValueType();
        }

        String raw = cellType.toJSONString(cell.value(), ProtocolVersion.CURRENT);
//        if (cell.path().size() > 1) {
//            sb.append("[");
//            sb.append(raw);
//            sb.append("]");
//        } else
            sb.append(raw);

        return sb.toString();
    }

    @Deprecated
    private void serializeCell(Cell cell, LivenessInfo liveInfo)
    {
        try
        {
            json.writeStartObject();
            //objectIndenter.setCompact(true);
            json.writeFieldName("name");
            AbstractType<?> type = cell.column().type;
            AbstractType<?> cellType = null;
            json.writeString(cell.column().name.toCQLString());

            if (type.isCollection() && type.isMultiCell()) // non-frozen collection
            {
                CollectionType ct = (CollectionType) type;
                json.writeFieldName("path");
                //arrayIndenter.setCompact(true);
                json.writeStartArray();
                for (int i = 0; i < cell.path().size(); i++)
                {
                    json.writeString(ct.nameComparator().getString(cell.path().get(i)));
                }
                json.writeEndArray();
                //arrayIndenter.setCompact(false);

                cellType = cell.column().cellValueType();
            }
            else if (type.isUDT() && type.isMultiCell()) // non-frozen udt
            {
                UserType ut = (UserType) type;
                json.writeFieldName("path");
                //arrayIndenter.setCompact(true);
                json.writeStartArray();
                for (int i = 0; i < cell.path().size(); i++)
                {
                    Short fieldPosition = ut.nameComparator().compose(cell.path().get(i));
                    json.writeString(ut.fieldNameAsString(fieldPosition));
                }
                json.writeEndArray();
                //arrayIndenter.setCompact(false);

                // cellType of udt
                Short fieldPosition = ((UserType) type).nameComparator().compose(cell.path().get(0));
                cellType = ((UserType) type).fieldType(fieldPosition);
            }
            else
            {
                cellType = cell.column().cellValueType();
            }
            if (cell.isTombstone())
            {
                json.writeFieldName("deletion_info");
                //objectIndenter.setCompact(true);
                json.writeStartObject();
                json.writeFieldName("local_delete_time");
                json.writeString(dateString(TimeUnit.SECONDS, cell.localDeletionTime()));
                json.writeEndObject();
                //objectIndenter.setCompact(false);
            }
            else
            {
                json.writeFieldName("value");
                json.writeRawValue(cellType.toJSONString(cell.value(), ProtocolVersion.CURRENT));
            }


            json.writeEndObject();
            //objectIndenter.setCompact(false);
        }
        catch (IOException e)
        {
            logger.error("Failure parsing cell.", e);
        }
    }

    private String dateString(TimeUnit from, long time)
    {
        if (rawTime)
        {
            return Long.toString(time);
        }

        long secs = from.toSeconds(time);
        long offset = Math.floorMod(from.toNanos(time), 1000_000_000L); // nanos per sec
        return Instant.ofEpochSecond(secs, offset).toString();
    }

    /**
     * A specialized {@link Indenter} that enables a 'compact' mode which puts all subsequent json values on the same
     * line. This is manipulated via {@link CompactIndenter#setCompact(boolean)}
     */
    private static final class CompactIndenter extends DefaultPrettyPrinter.NopIndenter
    {

        private static final int INDENT_LEVELS = 16;
        private final char[] indents;
        private final int charsPerLevel;
        private final String eol;
        private static final String space = " ";

        private boolean compact = false;

        CompactIndenter()
        {
            this("  ", System.lineSeparator());
        }

        CompactIndenter(String indent, String eol)
        {
            this.eol = eol;

            charsPerLevel = indent.length();

            indents = new char[indent.length() * INDENT_LEVELS];
            int offset = 0;
            for (int i = 0; i < INDENT_LEVELS; i++)
            {
                indent.getChars(0, indent.length(), indents, offset);
                offset += indent.length();
            }
        }

        @Override
        public boolean isInline()
        {
            return false;
        }

        /**
         * Configures whether or not subsequent json values should be on the same line delimited by string or not.
         *
         * @param compact
         *            Whether or not to compact.
         */
        public void setCompact(boolean compact)
        {
            this.compact = compact;
        }

        @Override
        public void writeIndentation(JsonGenerator jg, int level)
        {
            try
            {
                if (!compact)
                {
                    jg.writeRaw(eol);
                    if (level > 0)
                    { // should we err on negative values (as there's some flaw?)
                        level *= charsPerLevel;
                        while (level > indents.length)
                        { // unlike to happen but just in case
                            jg.writeRaw(indents, 0, indents.length);
                            level -= indents.length;
                        }
                        jg.writeRaw(indents, 0, level);
                    }
                }
                else
                {
                    jg.writeRaw(space);
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
}
