package com.staticbloc.jobs;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: eygraber
 * Date: 9/16/2014
 * Time: 6:40 AM
 * To change this template use File | Settings | File Templates.
 */
public interface JobQueueEventListener {
    public void onStarted(List<Job> loadedPersistedJobs);

    public void onJobAdded(Job jobStatus);

    public void onJobRemoved(Job jobStatus);

    public void onJobModified(Job jobStatus);

    public void onAllJobsCanceled();

    public void onShutdown(boolean keepPersisted);
}
