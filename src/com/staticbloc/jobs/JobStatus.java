package com.staticbloc.jobs;

/**
 * Created with IntelliJ IDEA.
 * User: eygraber
 * Date: 9/10/2014
 * Time: 12:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class JobStatus {
    /*package*/ static final int NO_ERROR = 0;
    /*package*/ static final int IDENTICAL_JOB_REJECTED = 1;

    public static enum State {
        NOTHING,
        QUEUE_NOT_READY,
        ADDED,
        COLD_STORAGE,
        CANCELED,
        QUEUED,
        ACTIVE,
        FINISHED
    }

    private State state = State.NOTHING;

    private boolean success;
    private int errorFlags;
    private long jobId;
    private Job job;

    public JobStatus(long jobId, Job job) {
        this.success = true;
        this.errorFlags = NO_ERROR;

        this.jobId = jobId;
        this.job = job;
    }

    public JobStatus(int errorFlags) {
        this.success = false;
        this.errorFlags = errorFlags;

        this.jobId = -1;
        this.job = null;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public boolean isSuccess() {
        return success;
    }

    public boolean isError() {
        return errorFlags != NO_ERROR;
    }

    public boolean wasIdenticalJobRejected() {
        return checkFlag(IDENTICAL_JOB_REJECTED);
    }

    public long getJobId() {
        return jobId;
    }

    public Job getJob() {
        return job;
    }

    private boolean checkFlag(int flag) {
        return (errorFlags & flag) == flag;
    }
}
