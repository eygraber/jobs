package com.staticbloc.jobs;

/**
 * Created with IntelliJ IDEA.
 * User: eygraber
 * Date: 9/16/2014
 * Time: 6:40 AM
 * To change this template use File | Settings | File Templates.
 */
public interface JobQueueEventListener {
    public void onStarted();

    public void onJobAdded(JobStatus jobStatus);

    public void onJobRemoved(JobStatus jobStatus);

    public void onJobModified(JobStatus jobStatus);

    public void onAllJobsCanceled();

    public void onShutdown(boolean keepPersisted);
}
