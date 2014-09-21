package com.staticbloc.jobs;

import android.content.Context;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class BasicPersistentJobQueue extends BasicJobQueue {
    private ExecutorService executor;

    BasicPersistentJobQueue(Context context, JobQueueInitializer initializer) {
        super(context, initializer);
        executor = Executors.newSingleThreadExecutor();
    }

    protected ExecutorService getPersistenceExecutor() {
        return executor;
    }

    @Override
    protected abstract void onLoadPersistedData();

    @Override
    protected abstract void onPersistJobAdded(JobQueueItem job);

    @Override
    protected abstract void onPersistJobRemoved(JobQueueItem job);

    @Override
    protected abstract void onPersistAllJobsCanceled();

    @Override
    protected abstract void onPersistJobModified(JobQueueItem job);
}
