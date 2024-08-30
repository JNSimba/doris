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

package org.apache.doris.job.extensions.cdc;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.Config;
import org.apache.doris.common.CustomThreadFactory;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.httpv2.rest.manager.HttpUtils;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.base.TimerDefinition;
import org.apache.doris.job.common.IntervalUnit;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.cdc.state.AbstractSourceSplit;
import org.apache.doris.job.extensions.cdc.state.BinlogSplit;
import org.apache.doris.job.extensions.cdc.state.SnapshotSplit;
import org.apache.doris.job.extensions.cdc.utils.CdcLoadConstants;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PRequestCdcScannerResult;
import org.apache.doris.proto.InternalService.PStartCdcScannerResult;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.resource.Tag;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TStatusCode;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CdcDatabaseJob extends AbstractJob<CdcDatabaseTask, Map<Object, Object>> {
    public static final String BINLOG_SPLIT_ID = "binlog-split";
    public static final String SPLIT_ID = "splitId";
    public static final String FINISH_SPLITS = "finishSplits";
    public static final String ASSIGNED_SPLITS = "assignedSplits";
    public static final String SNAPSHOT_TABLE = "snapshotTable";
    private static final String PURE_BINLOG_PHASE = "pureBinlogPhase";

    public static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("Id", ScalarType.createStringType()),
            new Column("Name", ScalarType.createStringType()),
            new Column("Definer", ScalarType.createStringType()),
            new Column("JobConfig", ScalarType.createStringType()),
            new Column("ExecuteType", ScalarType.createStringType()),
            new Column("RecurringStrategy", ScalarType.createStringType()),
            new Column("Status", ScalarType.createStringType()),
            new Column("ErrorMsg", ScalarType.createStringType()),
            new Column("CreateTime", ScalarType.createStringType()),
            new Column("Progress", ScalarType.createStringType()));
    public static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;
    private static final Logger LOG = LogManager.getLogger(CdcDatabaseJob.class);
    private static ObjectMapper objectMapper = new ObjectMapper();
    @SerializedName("rs")
    List<SnapshotSplit> remainingSplits = new CopyOnWriteArrayList<>();
    @SerializedName("as")
    Map<String, SnapshotSplit> assignedSplits = new ConcurrentHashMap<>();
    @SerializedName("sfo")
    Map<String, Map<String, String>> splitFinishedOffsets = new ConcurrentHashMap<>();
    @SerializedName("retbl")
    List<String> remainingTables;
    @SerializedName("ibsa")
    boolean isBinlogSplitAssigned = false;
    @SerializedName("co")
    Map<String, String> currentOffset;
    @SerializedName("ht")
    ConcurrentLinkedQueue<CdcDatabaseTask> historyTasks = new ConcurrentLinkedQueue<>();
    @SerializedName("spfm")
    volatile String splitFailMsg;
    ExecutorService executor;
    @SerializedName("did")
    private long dbId;
    @SerializedName("cf")
    private Map<String, String> config;
    @SerializedName("pbg")
    private boolean pureBinlogPhase;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    public CdcDatabaseJob(long dbId, String jobName, List<String> syncTables, Map<String, String> config,
            JobExecutionConfiguration jobExecutionConfiguration) {
        super(getNextJobId(), jobName, JobStatus.RUNNING, jobName,
                "", ConnectContext.get().getCurrentUserIdentity(), jobExecutionConfiguration);
        this.remainingTables = new CopyOnWriteArrayList<>(syncTables);
        this.config = config;
        this.dbId = dbId;
    }

    public static JobExecutionConfiguration generateJobExecConfig(Map<String, String> properties) {
        JobExecutionConfiguration jobExecutionConfiguration = new JobExecutionConfiguration();
        jobExecutionConfiguration.setExecuteType(JobExecuteType.RECURRING);
        TimerDefinition timerDefinition = new TimerDefinition();
        String interval = properties.get(CdcLoadConstants.MAX_BATCH_INTERVAL);
        timerDefinition.setInterval(Long.parseLong(interval));
        timerDefinition.setIntervalUnit(IntervalUnit.SECOND);
        jobExecutionConfiguration.setTimerDefinition(timerDefinition);
        return jobExecutionConfiguration;
    }

    @Override
    protected void checkJobParamsInternal() {
    }

    @Override
    public TRow getTvfInfo() {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(super.getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(super.getJobName()));
        trow.addToColumnValue(new TCell().setStringVal(super.getCreateUser().getQualifiedUser()));
        trow.addToColumnValue(new TCell().setStringVal(new Gson().toJson(config)));
        trow.addToColumnValue(new TCell().setStringVal(super.getJobConfig().getExecuteType().name()));
        trow.addToColumnValue(new TCell().setStringVal(super.getJobConfig().convertRecurringStrategyToString()));
        trow.addToColumnValue(new TCell().setStringVal(super.getJobStatus().name()));
        trow.addToColumnValue(new TCell().setStringVal(splitFailMsg == null ? FeConstants.null_string : splitFailMsg));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(super.getCreateTimeMs())));
        StringBuilder progress = new StringBuilder();
        if (currentOffset != null) {
            progress.append(new Gson().toJson(currentOffset));
        } else if (remainingSplits.isEmpty() && !isBinlogSplitAssigned && !remainingTables.isEmpty()) {
            progress.append("Waiting split");
        } else if (!remainingSplits.isEmpty()) {
            progress.append("Snapshot reading");
        }
        trow.addToColumnValue(new TCell().setStringVal(progress.toString()));
        return trow;
    }

    @Override
    public void onRegister() throws JobException {
        super.onRegister();
    }

    @Override
    public void onUnRegister() throws JobException {
        super.onUnRegister();
        closeCdcResource();
        if (executor != null) {
            executor.shutdown();
        }
    }

    private void closeCdcResource() throws JobException {
        Backend backend = selectBackend(getJobId());
        InternalService.PRequestCdcScannerRequest request = InternalService.PRequestCdcScannerRequest.newBuilder()
                .setApi("/api/close/" + getJobId())
                .setParams("").build();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        InternalService.PRequestCdcScannerResult result = null;
        try {
            Future<PRequestCdcScannerResult> future =
                    BackendServiceProxy.getInstance().requestCdcScanner(address, request);
            result = future.get();
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                LOG.warn("Close cdc scanner failed, {}", result.getStatus().getErrorMsgs(0));
            }
        } catch (RpcException | ExecutionException | InterruptedException ex) {
            LOG.warn("Close cdc scanner error, {}", ex.getMessage());
        }
    }

    private void startSplitAsync(Backend backend) {
        if (executor == null || executor.isShutdown()) {
            CustomThreadFactory threadFactory = new CustomThreadFactory("split-chunks");
            executor = Executors.newSingleThreadExecutor(threadFactory);
        }
        executor.submit(() -> {
            // reset failMsg
            splitFailMsg = null;
            try{
                remainingTables = new CopyOnWriteArrayList<>(remainingTables);
                for (String splitTbl : remainingTables) {
                    splitTable(backend, splitTbl);
                    if (StringUtils.isNotEmpty(splitFailMsg)) {
                        break;
                    }
                    if (isBinlogSplitAssigned) {
                        LOG.info("get binlog split {}", currentOffset);
                        break;
                    }
                    LOG.info("table {} split finished", splitTbl);

                }
                LOG.info("snapshot split finished");
            } catch (Exception ex){
                LOG.error("Split table error, ", ex);
                splitFailMsg = ex.getMessage();
                try {
                    updateJobStatus(JobStatus.PAUSED);
                } catch (JobException e) {
                    LOG.error("Change job status to pause failed, jobId {}", getJobId(), e);
                }
            }
        });
    }

    private void splitTable(Backend backend, String splitTbl) {
        List<? extends AbstractSourceSplit> splits = new ArrayList<>();
        try {
            // todo: Refining the granularity of the state to chunks, already split chunks do not need to be split again.
            // Currently, it is at the table level, meaning that tables which have already been split do not need to be split again.
            // If it is initial, to get split based on each table
            if ("initial".equals(config.getOrDefault(CdcLoadConstants.SCAN_STARTUP_MODE, "initial"))) {
                config.put(SNAPSHOT_TABLE, splitTbl);
            }
            splits = requestTableSplits(backend);
        } catch (Exception ex) {
            LOG.error("Fail to get split, ", ex);
            throw new RuntimeException(ex);
        }

        if (splits.isEmpty()) {
            throw new RuntimeException("split is empty");
        }

        LOG.info("fetch splits {}", splits);
        for (AbstractSourceSplit split : splits) {
            if (Objects.equals(split.getSplitId(), BINLOG_SPLIT_ID)) {
                BinlogSplit binlogSplit = (BinlogSplit) split;
                currentOffset = binlogSplit.getOffset();
                isBinlogSplitAssigned = true;
                break;
            } else {
                SnapshotSplit snapshotSplit = (SnapshotSplit) split;
                remainingSplits.add(snapshotSplit);
                LOG.info("add split " + snapshotSplit.getSplitId());
            }
        }
        remainingTables.remove(splitTbl);
        logUpdateOperation();
    }

    private List<? extends AbstractSourceSplit> requestTableSplits(Backend backend) throws JobException {
        Map<String, Object> params = new HashMap<>();
        params.put("jobId", getJobId());
        params.put("config", config);
        InternalService.PRequestCdcScannerRequest request = InternalService.PRequestCdcScannerRequest.newBuilder()
                .setApi("/api/fetchSplits")
                .setParams(GsonUtils.GSON.toJson(params)).build();
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        InternalService.PRequestCdcScannerResult result = null;
        List<? extends AbstractSourceSplit> responseList;
        try {
            Future<PRequestCdcScannerResult> future =
                    BackendServiceProxy.getInstance().requestCdcScanner(address, request);
            result = future.get();
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                LOG.error("Failed to get split from backend, {}", result.getStatus().getErrorMsgs(0));
                throw new JobException("Failed to get split from backend," + result.getStatus().getErrorMsgs(0) + ", response: " + result.getResponse());
            }
        } catch (RpcException | ExecutionException | InterruptedException ex) {
            LOG.error("Get splits error: ", ex);
            throw new JobException(ex);
        }

        String response = HttpUtils.parseResponse(result.getResponse());
        try{
            List<Map<String, Object>> mapList = new ObjectMapper().readValue(response,
                    new TypeReference<List<Map<String, Object>>>() {
                    });
            Map<String, Object> split = mapList.get(0);
            String splitId = split.get(CdcDatabaseJob.SPLIT_ID).toString();
            if (CdcDatabaseJob.BINLOG_SPLIT_ID.equals(splitId)) {
                responseList = objectMapper.convertValue(mapList, new TypeReference<List<BinlogSplit>>() {
                });
            } else {
                responseList = objectMapper.convertValue(mapList, new TypeReference<List<SnapshotSplit>>() {
                });
            }
            return responseList;
        }catch (IOException ex){
            LOG.error("Get splits error: ", ex);
            throw new RuntimeException("Get splits error");
        }
    }

    @Override
    public List<CdcDatabaseTask> createTasks(TaskType taskType, Map<Object, Object> taskContext) {
        try {
            Map<String, String> readOffset = new HashMap<>();
            // Call the BE interface and pass host, port, jobId
            // select backends
            Backend backend = selectBackend(getJobId());
            if (!isBinlogSplitAssigned) {
                if (!remainingSplits.isEmpty()) {
                    SnapshotSplit snapshotSplit = remainingSplits.get(0);
                    System.out.println("remove split " + snapshotSplit.getSplitId());
                    LOG.info("consumer snapshot split {}", snapshotSplit.getSplitId());
                    readOffset =
                            new ObjectMapper().convertValue(snapshotSplit, new TypeReference<Map<String, String>>() {
                            });
                    assignedSplits.put(snapshotSplit.getSplitId(), snapshotSplit);
                } else if (!remainingTables.isEmpty()) {
                    LOG.info("wait table {} split finished.", remainingTables);
                    return new ArrayList<>();
                } else if (assignedSplits.size() == splitFinishedOffsets.size()) {
                    readOffset.put(SPLIT_ID, BINLOG_SPLIT_ID);
                    readOffset.put(FINISH_SPLITS, objectMapper.writeValueAsString(splitFinishedOffsets));
                    readOffset.put(ASSIGNED_SPLITS, objectMapper.writeValueAsString(assignedSplits));
                    isBinlogSplitAssigned = true;
                } else {
                    throw new RuntimeException("miss split");
                }
            } else {
                readOffset.put(SPLIT_ID, BINLOG_SPLIT_ID);
                // todo: When fully entering the binlog phase, there is no need to pass splits
                if (!pureBinlogPhase && !splitFinishedOffsets.isEmpty() && !assignedSplits.isEmpty()) {
                    readOffset.put(FINISH_SPLITS, objectMapper.writeValueAsString(splitFinishedOffsets));
                    readOffset.put(ASSIGNED_SPLITS, objectMapper.writeValueAsString(assignedSplits));
                }
                if (currentOffset != null) {
                    readOffset.putAll(currentOffset);
                }
            }
            Preconditions.checkArgument(readOffset != null && !readOffset.isEmpty(), "read offset is empty");
            CdcDatabaseTask cdcDatabaseTask = new CdcDatabaseTask(dbId, backend, getJobId(), readOffset, config);
            ArrayList<CdcDatabaseTask> tasks = new ArrayList<>();
            tasks.add(cdcDatabaseTask);
            super.initTasks(tasks, taskType);
            LOG.info("finish create cdc task, task: {}", cdcDatabaseTask);
            return tasks;
        } catch (Exception ex) {
            LOG.error("Create task failed,", ex);
            throw new RuntimeException("Create task failed");
        }
    }

    @Override
    public boolean isReadyForScheduling(Map<Object, Object> taskContext) {
        if (!CollectionUtils.isEmpty(getRunningTasks())) {
            return false;
        }
        if (!remainingSplits.isEmpty()
                || isBinlogSplitAssigned
                || (!assignedSplits.isEmpty() && assignedSplits.size() == splitFinishedOffsets.size())) {
            return true;
        }
        LOG.info("job {} not ready scheduling", getJobId());
        return false;
    }

    @Override
    public ShowResultSetMetaData getTaskMetaData() {
        return null;
    }

    @Override
    public JobType getJobType() {
        return JobType.CDC;
    }

    @Override
    public List<CdcDatabaseTask> queryTasks() {
        return Lists.newArrayList(historyTasks);
    }

    @Override
    public void initialize() throws JobException {
        super.initialize();
        if (!remainingTables.isEmpty()) {
            Backend backend = selectBackend(getJobId());
            startCdcScanner(backend);
            startSplitAsync(backend);
        }
    }

    @Override
    public void onStatusChanged(JobStatus oldStatus, JobStatus newStatus) throws JobException {
        super.onStatusChanged(oldStatus, newStatus);
        if (JobStatus.PAUSED.equals(oldStatus) && JobStatus.RUNNING.equals(newStatus)) {
            if (!remainingTables.isEmpty()) {
                Backend backend = selectBackend(getJobId());
                startCdcScanner(backend);
                startSplitAsync(backend);
            }
        }

        if (JobStatus.RUNNING.equals(oldStatus) && JobStatus.PAUSED.equals(newStatus)) {
            if (executor != null){
                executor.shutdown();
            }
        }

        if (JobStatus.STOPPED.equals(newStatus)) {
            closeCdcResource();
        }
    }

    public static void startCdcScanner(Backend backend) throws JobException {
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        //Reserved parameters, currently empty
        String params = "";
        InternalService.PStartCdcScannerRequest request =
                InternalService.PStartCdcScannerRequest.newBuilder().setParams(params).build();
        InternalService.PStartCdcScannerResult result = null;
        try {
            Future<PStartCdcScannerResult> future =
                    BackendServiceProxy.getInstance().startCdcScanner(address, request);
            result = future.get();
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            if (code != TStatusCode.OK) {
                LOG.error("Failed to start cdc server on backend, {}", result.getStatus().getErrorMsgs(0));
                throw new JobException("Failed to start cdc server on backend, " + result.getStatus().getErrorMsgs(0));
            }
        } catch (RpcException | ExecutionException | InterruptedException ex) {
            LOG.error("Error to start cdc server on backend, ", ex);
            throw new JobException(ex);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public void updateOffset(Map<String, String> meta) {
        String splitId = meta.get(SPLIT_ID);
        if (splitId == null) {
            return;
        }
        if(meta.containsKey(PURE_BINLOG_PHASE)){
            pureBinlogPhase = Boolean.parseBoolean(meta.remove(PURE_BINLOG_PHASE));
        }
        if (!BINLOG_SPLIT_ID.equals(splitId)) {
            remainingSplits.remove(0);
            splitFinishedOffsets.put(splitId, meta);
        } else {
            currentOffset = meta;
        }
    }

    public void recordTasks(CdcDatabaseTask tasks) {
        if (Config.max_persistence_task_count < 1) {
            return;
        }
        historyTasks.add(tasks);

        while (historyTasks.size() > Config.max_persistence_task_count) {
            historyTasks.poll();
        }
        Env.getCurrentEnv().getEditLog().logUpdateJob(this);
    }

    public static Backend selectBackend(Long jobId) throws JobException {
        Backend backend = null;
        BeSelectionPolicy policy = null;
        Set<Tag> userTags = new HashSet<>();
        if (ConnectContext.get() != null) {
            String qualifiedUser = ConnectContext.get().getQualifiedUser();
            userTags = Env.getCurrentEnv().getAuth().getResourceTags(qualifiedUser);
        }

        policy = new BeSelectionPolicy.Builder()
                .addTags(userTags)
                .setEnableRoundRobin(true)
                .needLoadAvailable().build();
        List<Long> backendIds;
        backendIds = Env.getCurrentSystemInfo().selectBackendIdsByPolicy(policy, 1);
        if (backendIds.isEmpty()) {
            throw new JobException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }
        //jobid % backendSize
        long index = backendIds.get(jobId.intValue() % backendIds.size());
        backend = Env.getCurrentSystemInfo().getBackend(index);
        if (backend == null) {
            throw new JobException(SystemInfoService.NO_BACKEND_LOAD_AVAILABLE_MSG + ", policy: " + policy);
        }
        return backend;
    }
}
