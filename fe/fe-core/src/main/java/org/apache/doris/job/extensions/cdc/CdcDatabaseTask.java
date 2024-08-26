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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.cdc.utils.RestService;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.service.ExecuteEnv;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.TransactionException;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.transaction.TxnCommitAttachment;
import org.apache.doris.transaction.TxnStateChangeCallback;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import static org.apache.doris.job.extensions.cdc.CdcDatabaseJob.startCdcScanner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class CdcDatabaseTask extends AbstractTask implements TxnStateChangeCallback {

    private static final Logger LOG = LogManager.getLogger(CdcDatabaseTask.class);

    public static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("TaskId", ScalarType.createStringType()),
            new Column("JobId", ScalarType.createStringType()),
            new Column("JobName", ScalarType.createStringType()),
            new Column("Status", ScalarType.createStringType()),
            new Column("ErrorMsg", ScalarType.createStringType()),
            new Column("CreateTime", ScalarType.createStringType()),
            new Column("StartTime", ScalarType.createStringType()),
            new Column("FinishTime", ScalarType.createStringType()),
            new Column("Backend", ScalarType.createStringType()),
            new Column("StartOffset", ScalarType.createStringType()),
            new Column("EndOffset", ScalarType.createStringType()));

    public static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    private long dbId;
    @SerializedName(value = "bd")
    private Backend backend;
    private Long jobId;
    private Map<String, String> meta;
    private Map<String, String> finishedMeta;
    private Map<String, String> config;
    private volatile long txnId = -1;

    public CdcDatabaseTask(long dbId, Backend backend, Long jobId, Map<String, String> meta,
            Map<String, String> config) {
        this.dbId = dbId;
        this.backend = backend;
        this.jobId = jobId;
        this.meta = meta;
        this.config = config;
    }

    @Override
    protected void closeOrReleaseResources() {
    }

    @Override
    protected void executeCancelLogic() throws Exception {

    }

    @Override
    public void run() throws JobException {
        // if(beginTxn()){
        //     //call be rpc
        //     //waitlock
        // }
        startCdcScanner(backend);
        // Call the BE interface and pass host, port, jobId
        // mock pull data
        System.out.println("====run task...");
        try {
            String response = RestService.fetchRecords(Pair.of(backend.getHost(), 9096), jobId, meta, config);
            System.out.println(response);
            Map map = new ObjectMapper().readValue(response, Map.class);
            Map<String, String> res = new ObjectMapper().convertValue(map.get("meta"), Map.class);
            finishedMeta = res;
        } catch (Exception ex) {
            LOG.error("Run task failed,", ex);
            throw new JobException(ex.getMessage());
        }
    }

    public boolean beginTxn() throws JobException {
        TUniqueId id = new TUniqueId(getJobId(), getTaskId());
        // begin a txn for task
        try {
            txnId = Env.getCurrentGlobalTransactionMgr().beginTransaction(dbId,
                    Lists.newArrayList(), "cdc_scanner_" + DebugUtil.printId(id), null,
                    new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, 0,
                            FrontendOptions.getLocalHostAddress(), ExecuteEnv.getInstance().getStartupTime()),
                    TransactionState.LoadJobSourceType.BACKEND_STREAMING, getJobId(),
                    60000);
        } catch (Exception ex) {
            LOG.warn("failed to begin txn for cdc load task: {}, task id {}, job id: {}",
                    DebugUtil.printId(id), getTaskId(), jobId, ex);
            return false;
        }
        return true;
    }


    @Override
    public TRow getTvfInfo(String jobName) {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(super.getTaskId())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(super.getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(super.getJobName())));
        trow.addToColumnValue(new TCell().setStringVal(
                super.getStatus() == null ? FeConstants.null_string : super.getStatus().toString()));
        trow.addToColumnValue(new TCell().setStringVal(super.getErrMsg()));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(super.getCreateTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(super.getStartTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(super.getFinishTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(backend == null ? FeConstants.null_string : backend.getHost()));
        trow.addToColumnValue(new TCell().setStringVal(new Gson().toJson(meta)));
        trow.addToColumnValue(new TCell()
                .setStringVal(finishedMeta == null ? FeConstants.null_string : new Gson().toJson(finishedMeta)));
        return trow;
    }

    @Override
    public void onSuccess() throws JobException {
        CdcDatabaseJob job = (CdcDatabaseJob) Env.getCurrentEnv().getJobManager().getJob(getJobId());
        job.updateOffset(finishedMeta);
        super.onSuccess();
        job.recordTasks(this);
    }

    @Override
    public void onFail() throws JobException {
        CdcDatabaseJob job = (CdcDatabaseJob) Env.getCurrentEnv().getJobManager().getJob(getJobId());
        super.onFail();
        job.recordTasks(this);
    }

    @Override
    public long getId() {
        return getJobId();
    }

    @Override
    public void beforeCommitted(TransactionState txnState) throws TransactionException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.SCHEDULER_TASK, txnState.getLabel())
                    .add("txn_state", txnState)
                    .add("msg", "task before committed")
                    .build());
        }
        TaskStatus status = getStatus();
        if(status == TaskStatus.CANCELED || status == TaskStatus.FAILED) {
            throw new TransactionException("txn " + txnState.getTransactionId()
                    + " could not be " + TransactionStatus.COMMITTED
                    + " while task " + txnState.getLabel() + " has been aborted.");
        }
    }

    @Override
    public void beforeAborted(TransactionState txnState) throws TransactionException {
        //do nothing
    }

    @Override
    public void afterCommitted(TransactionState txnState, boolean txnOperated) throws UserException {
        if(txnOperated){
            if(txnState.getTransactionId() != txnId){
                LOG.info("Can not find task with transaction {} after committed, task id: {}, job: {}",
                        txnState.getTransactionId(), getTaskId(), getJobId());
                return;
            }

            CdcDatabaseJob job = (CdcDatabaseJob) Env.getCurrentEnv().getJobManager().getJob(getJobId());
            TxnCommitAttachment txnCommitAttachment = txnState.getTxnCommitAttachment();
            // txnCommitAttachment.getFinishedMeta()
            // updateOffset
            job.updateOffset(finishedMeta);
            //release lock
        }

    }

    @Override
    public void replayOnCommitted(TransactionState txnState) {
    }

    @Override
    public void afterAborted(TransactionState txnState, boolean txnOperated, String txnStatusChangeReason)
            throws UserException {
        // nothing to do
        // release lock
    }

    @Override
    public void replayOnAborted(TransactionState txnState) {
        //do nothing
    }

    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        //do nothing
    }

    @Override
    public void replayOnVisible(TransactionState txnState) {
        //do nothing
    }
}
