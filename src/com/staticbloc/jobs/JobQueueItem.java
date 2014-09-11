package com.staticbloc.jobs;

public interface JobQueueItem {
    public void obtainGroupIndex();

    public Job getJob();

    public JobStatus getStatus();

    public long getJobId();

    public long getValidAtTime();

    public void setValidAtTime(long validAtTime);

    public void incrementNetworkRetryCount();

    public long getNetworkRetryBackoffMillis();

    public void incrementGroupRetryCount();

    public long getGroupRetryBackoffMillis();

    public boolean isGroupMember();

    public int getGroupIndex();

    public String getGroup();

    public boolean equals(JobQueueItem other);
}
