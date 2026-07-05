package com.cicd.observability.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

/**
 * Canonical event model produced by the Jenkins pipeline.
 * Maps exactly to the JSON structure emitted by the Jenkinsfile
 * stageEvent() / stageEventFailure() functions.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CicdEvent implements Serializable {

    @JsonProperty("event_id")        private String eventId;
    @JsonProperty("pipeline_id")     private String pipelineId;
    @JsonProperty("repository_id")   private String repositoryId;
    @JsonProperty("analysis_name")   private String analysisName;
    @JsonProperty("analysis_type")   private String analysisType;
    @JsonProperty("service_name")    private String serviceName;
    @JsonProperty("branch")          private String branch;
    @JsonProperty("commit_sha")      private String commitSha;
    @JsonProperty("stage")           private String stage;
    @JsonProperty("event_type")      private String eventType;
    @JsonProperty("event_timestamp") private String eventTimestamp;
    @JsonProperty("status")          private String status;

    /** Epoch-ms assigned by the deserialiser from event_timestamp string. */
    private long timestampMs;

    public CicdEvent() {}

    public String getEventId()           { return eventId; }
    public void   setEventId(String v)   { this.eventId = v; }
    public String getPipelineId()        { return pipelineId; }
    public void   setPipelineId(String v){ this.pipelineId = v; }
    public String getRepositoryId()      { return repositoryId; }
    public void   setRepositoryId(String v){ this.repositoryId = v; }
    public String getAnalysisName()      { return analysisName; }
    public void   setAnalysisName(String v){ this.analysisName = v; }
    public String getAnalysisType()      { return analysisType; }
    public void   setAnalysisType(String v){ this.analysisType = v; }
    public String getServiceName()       { return serviceName; }
    public void   setServiceName(String v){ this.serviceName = v; }
    public String getBranch()            { return branch; }
    public void   setBranch(String v)    { this.branch = v; }
    public String getCommitSha()         { return commitSha; }
    public void   setCommitSha(String v) { this.commitSha = v; }
    public String getStage()             { return stage; }
    public void   setStage(String v)     { this.stage = v; }
    public String getEventType()         { return eventType; }
    public void   setEventType(String v) { this.eventType = v; }
    public String getEventTimestamp()    { return eventTimestamp; }
    public void   setEventTimestamp(String v){ this.eventTimestamp = v; }
    public String getStatus()            { return status; }
    public void   setStatus(String v)    { this.status = v; }
    public long   getTimestampMs()       { return timestampMs; }
    public void   setTimestampMs(long v) { this.timestampMs = v; }

    public boolean isFailure() { return "FAILURE".equalsIgnoreCase(status); }
    public boolean isSuccess() { return "SUCCESS".equalsIgnoreCase(status); }

    @Override
    public String toString() {
        return "CicdEvent{pipeline=" + pipelineId + ", type=" + eventType
                + ", status=" + status + ", ts=" + eventTimestamp + "}";
    }
}
