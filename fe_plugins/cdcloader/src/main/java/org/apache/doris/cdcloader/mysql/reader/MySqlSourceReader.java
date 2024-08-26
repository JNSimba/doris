// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.cdcloader.mysql.reader;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.document.Array;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.cdcloader.mysql.constants.LoadConstants;
import org.apache.doris.cdcloader.mysql.rest.model.FetchRecordReq;
import org.apache.doris.cdcloader.mysql.rest.model.JobConfig;
import org.apache.doris.cdcloader.mysql.serialize.DorisRecordSerializer;
import org.apache.doris.cdcloader.mysql.serialize.JsonSerializer;
import org.apache.doris.cdcloader.mysql.utils.ConfigUtil;
import org.apache.doris.job.extensions.cdc.state.AbstractSourceSplit;
import org.apache.doris.job.extensions.cdc.state.BinlogSplit;
import org.apache.doris.job.extensions.cdc.state.SnapshotSplit;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.BinlogSplitReader;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.DebeziumReader;
import org.apache.flink.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader;
import org.apache.flink.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import static org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner.BINLOG_SPLIT_ID;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlSnapshotSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplitState;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.ChunkUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.TableDiscoveryUtils;
import org.apache.flink.cdc.connectors.mysql.table.StartupMode;
import org.apache.flink.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlSourceReader implements SourceReader<RecordWithMeta, FetchRecordReq> {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceReader.class);
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static final String SPLIT_ID = "splitId";
    private static final String FINISH_SPLITS = "finishSplits";
    private static final String ASSIGNED_SPLITS = "assignedSplits";
    private static final String SNAPSHOT_TABLE = "snapshotTable";
    private static final String PURE_BINLOG_PHASE = "pureBinlogPhase";
    private static final FlinkJsonTableChangeSerializer TABLE_CHANGE_SERIALIZER =
        new FlinkJsonTableChangeSerializer();
    private Map<Long, MySqlSourceConfig> sourceConfigMap;
    private Map<Long, SnapshotSplitReader> reusedSnapshotReaderMap;
    private Map<Long, BinlogSplitReader> reusedBinlogReaderMap;
    private Map<Long, DebeziumReader<SourceRecords, MySqlSplit>> currentReaderMap;
    private DorisRecordSerializer<SourceRecord, List<String>> serializer;
    private Map<Long, Map<TableId, TableChanges.TableChange>> tableSchemaMaps;
    private Map<Long, SplitRecords> currentSplitRecordsMap;

    public MySqlSourceReader() {
        this.sourceConfigMap = new ConcurrentHashMap<>();
        this.reusedSnapshotReaderMap = new ConcurrentHashMap<>();
        this.reusedBinlogReaderMap = new ConcurrentHashMap<>();
        this.currentReaderMap = new ConcurrentHashMap<>();
        this.tableSchemaMaps = new ConcurrentHashMap<>();
        this.serializer = new JsonSerializer();
        this.currentSplitRecordsMap = new ConcurrentHashMap<>();
    }

    @Override
    public void initialize() {
    }

    @Override
    public List<AbstractSourceSplit> getSourceSplits(JobConfig config) throws JsonProcessingException {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        StartupMode startupMode = sourceConfig.getStartupOptions().startupMode;
        List<MySqlSnapshotSplit> remainingSnapshotSplits = new ArrayList<>();
        MySqlBinlogSplit remainingBinlogSplit = null;
        if(startupMode.equals(StartupMode.INITIAL)){
            String snapshotTable = config.getConfig().get(SNAPSHOT_TABLE);
            remainingSnapshotSplits = startSplitChunks(sourceConfig, snapshotTable, config.getConfig());
        }else{
            remainingBinlogSplit = new MySqlBinlogSplit(
                BINLOG_SPLIT_ID,
                sourceConfig.getStartupOptions().binlogOffset,
                BinlogOffset.ofNonStopping(),
                new ArrayList<>(),
                new HashMap<>(),
                0);
        }
        List<AbstractSourceSplit> splits = new ArrayList<>();
        if(!remainingSnapshotSplits.isEmpty()){
            for(MySqlSnapshotSplit snapshotSplit: remainingSnapshotSplits){
                String splitId = snapshotSplit.splitId();
                String tableId = snapshotSplit.getTableId().identifier();
                String splitStart = snapshotSplit.getSplitStart() == null ? null : objectMapper.writeValueAsString(snapshotSplit.getSplitStart());
                String splitEnd = snapshotSplit.getSplitEnd() == null ? null : objectMapper.writeValueAsString(snapshotSplit.getSplitEnd());
                String splitKey = snapshotSplit.getSplitKeyType().getFieldNames().get(0);
                SnapshotSplit split = new SnapshotSplit(splitId, tableId, splitKey, splitStart, splitEnd, null);
                splits.add(split);
            }
        }else{
            BinlogOffset startingOffset = remainingBinlogSplit.getStartingOffset();
            BinlogSplit binlogSplit = new BinlogSplit(remainingBinlogSplit.splitId(), startingOffset.getOffset());
            splits.add(binlogSplit);
        }
        return splits;
    }

    @Override
    public RecordWithMeta read(FetchRecordReq fetchRecord) throws Exception {
        JobConfig jobConfig = new JobConfig(fetchRecord.getJobId(), fetchRecord.getConfig());
        int count=0;
        RecordWithMeta recordResponse = new RecordWithMeta();
        int fetchSize = fetchRecord.getFetchSize();
        boolean schedule = fetchRecord.isSchedule();
        MySqlSplit split = null;
        boolean pureBinlogPhase = false;
        if(schedule){
            Map<String, String> offset = fetchRecord.getMeta();
            if(offset.isEmpty()){
                throw new RuntimeException("miss meta offset");
            }
            Tuple2<MySqlSplit, Boolean> splitFlag = createMySqlSplit(offset, jobConfig);
            split = splitFlag.f0;
            pureBinlogPhase = splitFlag.f1;
            //reset current reader
            closeBinlogReader(jobConfig.getJobId());
            SplitRecords currentSplitRecords = pollSplitRecords(split, jobConfig);
            currentSplitRecordsMap.put(jobConfig.getJobId(), currentSplitRecords);
        }

        if(split == null){
            throw new RuntimeException("Can not generate split");
        }

        SplitRecords currentSplitRecords = currentSplitRecordsMap.get(jobConfig.getJobId());
        MySqlSplitState currentSplitState = null;
        if (currentSplitRecords != null && !currentSplitRecords.isEmpty()) {
            Iterator<SourceRecord> recordIt = currentSplitRecords.getIterator();

            if(split.isSnapshotSplit()){
                currentSplitState = new MySqlSnapshotSplitState(split.asSnapshotSplit());
            }

            while (recordIt.hasNext()) {
                SourceRecord element = recordIt.next();

                if (RecordUtils.isWatermarkEvent(element)) {
                    BinlogOffset watermark = RecordUtils.getWatermark(element);
                    if (RecordUtils.isHighWatermarkEvent(element) && currentSplitState instanceof MySqlSnapshotSplitState) {
                        currentSplitState.asSnapshotSplitState().setHighWatermark(watermark);
                    }
                }else if (RecordUtils.isHeartbeatEvent(element)) {
                    LOG.debug("Receive heartbeat event: {}", element);
                } else if (RecordUtils.isDataChangeRecord(element)) {
                    List<String> serialize = serializer.serialize(jobConfig.getConfig(), element);
                    if(CollectionUtils.isEmpty(serialize)){
                        continue;
                    }
                    count += serialize.size();
                    Map<String, String> lastMeta = RecordUtils.getBinlogPosition(element).getOffset();
                    if(split.isBinlogSplit()){
                        lastMeta.put(SPLIT_ID, BINLOG_SPLIT_ID);
                        lastMeta.put(PURE_BINLOG_PHASE, String.valueOf(pureBinlogPhase));
                        recordResponse.setMeta(lastMeta);
                    }
                    recordResponse.getRecords().addAll(serialize);
                    if(count >= fetchSize){
                        return recordResponse;
                    }
                } else if(RecordUtils.isSchemaChangeEvent(element) && split.isBinlogSplit()) {
                    refreshTableChanges(element, jobConfig.getJobId());
                    MySqlSourceConfig sourceConfig = getSourceConfig(jobConfig);
                    if(sourceConfig.isIncludeSchemaChanges()){
                        List<String> sqls = serializer.serialize(jobConfig.getConfig(), element);
                        if(!sqls.isEmpty()){
                            recordResponse.setSqls(sqls);
                            Map<String, String>  lastMeta = RecordUtils.getBinlogPosition(element).getOffset();
                            lastMeta.put(SPLIT_ID, BINLOG_SPLIT_ID);
                            recordResponse.setMeta(lastMeta);
                            return recordResponse;
                        }
                    }
                } else {
                    LOG.debug("Ignore event: {}", element);
                }
            }
        }
        if(split.isSnapshotSplit()){
            BinlogOffset highWatermark = currentSplitState.asSnapshotSplitState().getHighWatermark();
            Map<String, String> offset = highWatermark.getOffset();
            offset.put(SPLIT_ID, split.splitId());
            recordResponse.setMeta(offset);
        }
        if(CollectionUtils.isEmpty(recordResponse.getRecords())){
            if(split.isBinlogSplit()){
                Map<String, String> offset = split.asBinlogSplit().getStartingOffset().getOffset();
                offset.put(SPLIT_ID, BINLOG_SPLIT_ID);
                offset.put(PURE_BINLOG_PHASE, String.valueOf(pureBinlogPhase));
                recordResponse.setMeta(offset);
            }else{
                recordResponse.setMeta(fetchRecord.getMeta());
            }
        }
        return recordResponse;
    }

    /**
     * refresh table changes after schema change
     * @param element
     * @param jobId
     * @throws IOException
     */
    private void refreshTableChanges(SourceRecord element, Long jobId) throws IOException {
        HistoryRecord historyRecord = RecordUtils.getHistoryRecord(element);
        Array tableChanges =
            historyRecord.document().getArray(HistoryRecord.Fields.TABLE_CHANGES);
        TableChanges changes = TABLE_CHANGE_SERIALIZER.deserialize(tableChanges, true);
        Map<TableId, TableChanges.TableChange> tableChangeMap = tableSchemaMaps.get(jobId);
        for(TableChanges.TableChange tblChange : changes){
            tableChangeMap.put(tblChange.getTable().id(), tblChange);
        }
    }

    private Tuple2<MySqlSplit, Boolean> createMySqlSplit(Map<String, String> offset, JobConfig jobConfig) throws JsonProcessingException {
        Tuple2<MySqlSplit, Boolean> splitRes = null;
        String splitId = offset.get(SPLIT_ID);
        if(!BINLOG_SPLIT_ID.equals(splitId)){
            MySqlSnapshotSplit split = createSnapshotSplit(offset, jobConfig);
            splitRes = Tuple2.of(split, false);
        }else{
            splitRes = createBinlogSplit(offset, jobConfig);
        }
        return splitRes;
    }

    private MySqlSnapshotSplit createSnapshotSplit(Map<String, String> offset, JobConfig jobConfig) throws JsonProcessingException {
        String splitId = offset.get(SPLIT_ID);
        SnapshotSplit snapshotSplit = objectMapper.convertValue(offset, SnapshotSplit.class);
        TableId tableId = TableId.parse(snapshotSplit.getTableId());
        Object[] splitStart = snapshotSplit.getSplitStart() == null ? null : objectMapper.readValue(snapshotSplit.getSplitStart(), Object[].class);
        Object[] splitEnd = snapshotSplit.getSplitEnd() == null ? null : objectMapper.readValue(snapshotSplit.getSplitEnd(), Object[].class);
        String splitKey = snapshotSplit.getSplitKey();
        Map<TableId, TableChanges.TableChange> tableSchemas = getTableSchemas(jobConfig);
        TableChanges.TableChange tableChange = tableSchemas.get(tableId);
        Column splitColumn = tableChange.getTable().columnWithName(splitKey);
        RowType splitType = ChunkUtils.getChunkKeyColumnType(splitColumn);
        MySqlSnapshotSplit split = new MySqlSnapshotSplit(tableId,splitId, splitType, splitStart, splitEnd, null, tableSchemas);
        return split;
    }

    private Tuple2<MySqlSplit, Boolean> createBinlogSplit(Map<String, String> meta, JobConfig config) throws JsonProcessingException {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        BinlogOffset offsetConfig = null;
        if(sourceConfig.getStartupOptions() != null){
            offsetConfig = sourceConfig.getStartupOptions().binlogOffset;
        }

        List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();
        BinlogOffset minOffsetFinishSplits = null;
        BinlogOffset maxOffsetFinishSplits = null;
        if(meta.containsKey(FINISH_SPLITS) && meta.containsKey(ASSIGNED_SPLITS)) {
            //Construct binlogsplit based on the finished split and assigned split.
            String finishSplitsOffset = meta.remove(FINISH_SPLITS);
            String assignedSplits = meta.remove(ASSIGNED_SPLITS);
            Map<String, Map<String, String>> splitFinishedOffsets = objectMapper.readValue(finishSplitsOffset, new TypeReference<Map<String, Map<String, String>>>() {});
            Map<String, SnapshotSplit> assignedSplitsMap = objectMapper.readValue(assignedSplits, new TypeReference<Map<String, SnapshotSplit>>() {});
            List<SnapshotSplit> assignedSplitLists = assignedSplitsMap.values().stream()
                .sorted(Comparator.comparing(AbstractSourceSplit::getSplitId))
                .collect(Collectors.toList());

            for (SnapshotSplit split : assignedSplitLists) {
                // find the min binlog offset
                Map<String, String> offsetMap = splitFinishedOffsets.get(split.getSplitId());
                BinlogOffset binlogOffset = new BinlogOffset(offsetMap);
                if (minOffsetFinishSplits == null || binlogOffset.isBefore(minOffsetFinishSplits)) {
                    minOffsetFinishSplits = binlogOffset;
                }
                if (maxOffsetFinishSplits == null || binlogOffset.isAfter(maxOffsetFinishSplits)) {
                    maxOffsetFinishSplits = binlogOffset;
                }
                Object[] splitStart = split.getSplitStart() == null ? null : objectMapper.readValue(split.getSplitStart(), Object[].class);
                Object[] splitEnd = split.getSplitEnd() == null ? null : objectMapper.readValue(split.getSplitEnd(), Object[].class);

                finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                        TableId.parse(split.getTableId()),
                        split.getSplitId(),
                        splitStart,
                        splitEnd,
                        binlogOffset));
            }
        }

        BinlogOffset startOffset;
        BinlogOffset lastOffset = new BinlogOffset(meta);
        if (minOffsetFinishSplits != null && lastOffset.getOffsetKind() == null) {
            startOffset = minOffsetFinishSplits;
        }else if(lastOffset.getOffsetKind() != null && lastOffset.getFilename() != null){
            startOffset = lastOffset;
        }else if(offsetConfig != null){
            startOffset = offsetConfig;
        }else{
            startOffset = BinlogOffset.ofEarliest();
        }

        boolean pureBinlogPhase = false;
        if(maxOffsetFinishSplits == null){
            pureBinlogPhase = true;
        }else if(startOffset.isAtOrAfter(maxOffsetFinishSplits)){
            // All the offsets of the current split are smaller than the offset of the binlog,
            // indicating that the binlog phase has been fully entered.
            pureBinlogPhase = true;
            LOG.info("The binlog phase has been fully entered, the current split is: {}", startOffset);
        }

        MySqlBinlogSplit split = new MySqlBinlogSplit(BINLOG_SPLIT_ID, startOffset, BinlogOffset.ofNonStopping(), finishedSnapshotSplitInfos, new HashMap<>(), 0);
        //filterTableSchema
        MySqlBinlogSplit binlogSplit = MySqlBinlogSplit.fillTableSchemas(split.asBinlogSplit(), getTableSchemas(config));
        return Tuple2.of(binlogSplit, pureBinlogPhase);
    }

    private List<MySqlSnapshotSplit> startSplitChunks(MySqlSourceConfig sourceConfig, String snapshotTable, Map<String, String> config){
        List<TableId> remainingTables = new ArrayList<>();
        if(snapshotTable != null){
            //need add database name
            String database = config.get(LoadConstants.DATABASE_NAME);
            remainingTables.add(TableId.parse(database + "." + snapshotTable));
        }
        List<MySqlSnapshotSplit> remainingSplits = new ArrayList<>();
        MySqlSnapshotSplitAssigner splitAssigner =
            new MySqlSnapshotSplitAssigner(sourceConfig, 1, remainingTables, false);
        splitAssigner.open();
        while (true) {
            Optional<MySqlSplit> mySqlSplit = splitAssigner.getNext();
            if (mySqlSplit.isPresent()) {
                MySqlSnapshotSplit snapshotSplit = mySqlSplit.get().asSnapshotSplit();
                remainingSplits.add(snapshotSplit);
            } else {
                break;
            }
        }
        return remainingSplits;
    }

    private SplitRecords pollSplitRecords(MySqlSplit split, JobConfig jobConfig) throws Exception {
        Iterator<SourceRecords> dataIt = null;
        String currentSplitId = null;
        DebeziumReader<SourceRecords, MySqlSplit> currentReader = currentReaderMap.get(jobConfig.getJobId());
        if (currentReader == null) {
            LOG.info("Get a split: {}", split.splitId());
            if (split instanceof MySqlSnapshotSplit) {
                currentReader = getSnapshotSplitReader(jobConfig);
            } else if (split instanceof MySqlBinlogSplit) {
                currentReader = getBinlogSplitReader(jobConfig);
            }
            currentReaderMap.put(jobConfig.getJobId(), currentReader);
            currentReader.submitSplit(split);
            currentSplitId = split.splitId();
            //make split record available
            Thread.sleep(100);
            dataIt = currentReader.pollSplitRecords();
            if(currentReader instanceof SnapshotSplitReader){
                closeSnapshotReader(jobConfig.getJobId());
            }
            return dataIt == null ? null : new SplitRecords(currentSplitId, dataIt.next());
        }
        else {
            throw new IllegalStateException("Unsupported reader type.");
        }
    }

    private SnapshotSplitReader getSnapshotSplitReader(JobConfig config) {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        SnapshotSplitReader reusedSnapshotReader = reusedSnapshotReaderMap.computeIfAbsent(config.getJobId(), v->{
            final MySqlConnection jdbcConnection =
                DebeziumUtils.createMySqlConnection(sourceConfig);
            final BinaryLogClient binaryLogClient =
                DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
            final StatefulTaskContext statefulTaskContext =
                new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
            return new SnapshotSplitReader(statefulTaskContext, 0);
        });
        return reusedSnapshotReader;
    }

    private BinlogSplitReader getBinlogSplitReader(JobConfig config) {
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        BinlogSplitReader reusedBinlogReader = reusedBinlogReaderMap.computeIfAbsent(config.getJobId(), v->{
            //todo: reuse binlog reader
            final MySqlConnection jdbcConnection =
                DebeziumUtils.createMySqlConnection(sourceConfig);
            final BinaryLogClient binaryLogClient =
                DebeziumUtils.createBinaryClient(sourceConfig.getDbzConfiguration());
            final StatefulTaskContext statefulTaskContext =
                new StatefulTaskContext(sourceConfig, binaryLogClient, jdbcConnection);
            return new BinlogSplitReader(statefulTaskContext, 0);
        });
        return reusedBinlogReader;
    }

    private void closeSnapshotReader(Long jobId) {
        SnapshotSplitReader reusedSnapshotReader = reusedSnapshotReaderMap.remove(jobId);
        if (reusedSnapshotReader != null) {
            LOG.debug(
                "Close snapshot reader {}", reusedSnapshotReader.getClass().getCanonicalName());
            reusedSnapshotReader.close();
            DebeziumReader<SourceRecords, MySqlSplit> currentReader = currentReaderMap.get(jobId);
            if (reusedSnapshotReader == currentReader) {
                currentReader = null;
                currentReaderMap.remove(jobId);
            }
            reusedSnapshotReader = null;
        }
    }

    private void closeBinlogReader(Long jobId) {
        BinlogSplitReader reusedBinlogReader = reusedBinlogReaderMap.remove(jobId);
        if (reusedBinlogReader != null) {
            LOG.debug("Close binlog reader {}", reusedBinlogReader.getClass().getCanonicalName());
            reusedBinlogReader.close();
            DebeziumReader<SourceRecords, MySqlSplit> currentReader = currentReaderMap.get(jobId);
            if (reusedBinlogReader == currentReader) {
                currentReader = null;
                currentReaderMap.remove(jobId);
            }
            reusedBinlogReader = null;
        }
    }

    private MySqlSourceConfig getSourceConfig(JobConfig config){
        return sourceConfigMap.computeIfAbsent(config.getJobId(), v -> ConfigUtil.generateMySqlConfig(config.getConfig()));
    }

    @Override
    public void close(Long jobId) {
        closeSnapshotReader(jobId);
        closeBinlogReader(jobId);
        sourceConfigMap.remove(jobId);
        tableSchemaMaps.remove(jobId);
        currentSplitRecordsMap.remove(jobId);
        LOG.info("Close source reader for job {}", jobId);
    }

    private Map<TableId, TableChanges.TableChange> getTableSchemas(JobConfig config){
        return tableSchemaMaps.computeIfAbsent(config.getJobId(), v -> discoverTableSchemas(config));
    }

    private Map<TableId, TableChanges.TableChange> discoverTableSchemas(JobConfig config){
        MySqlSourceConfig sourceConfig = getSourceConfig(config);
        try (MySqlConnection jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)) {
            MySqlPartition partition =
                new MySqlPartition(sourceConfig.getMySqlConnectorConfig().getLogicalName());
            return TableDiscoveryUtils.discoverSchemaForCapturedTables(
                partition, sourceConfig, jdbc);
        }catch (SQLException ex){
            throw new RuntimeException(ex);
        }
    }

}
