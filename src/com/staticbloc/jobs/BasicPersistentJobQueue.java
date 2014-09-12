package com.staticbloc.jobs;

import android.content.Context;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 * User: eygraber
 * Date: 9/12/2014
 * Time: 6:29 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class BasicPersistentJobQueue extends BasicJobQueue {
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    public BasicPersistentJobQueue(Context context) {
        super(context);
    }

    protected ExecutorService getPersistenceExecutor() {
        return executor;
    }

    @Override
    protected abstract void onLoadPersistedJobs();

    @Override
    protected abstract void onPersistJobAdded(JobQueueItem job);

    @Override
    protected abstract void onPersistJobRemoved(JobQueueItem job);

    @Override
    protected abstract void onPersistAllJobsCanceled();

    @Override
    protected abstract void onPersistJobModified(JobQueueItem job);
}
