package com.staticbloc.jobs;

import java.util.concurrent.CountDownLatch;

public abstract class BasicJob implements Job, Waitable {
    public final static boolean DEFAULT_CAN_REACH_REQUIRED_NETWORK = true;

    private int priority;
    private boolean requiresNetwork;
    private boolean isPersistent;
    private String group;
    private int retryLimit;
    private BackoffPolicy backoffPolicy;

    private int retryCount;

    private CountDownLatch awaiter;

    /**
     * Will create a {@code BasiJob} using the defaults from {@link JobInitializer}.
     */
    public BasicJob() {
        this(new JobInitializer());
    }

    /**
     * Creates a {@code BasicJob} using a {@link JobInitializer}.
     * @param initializer a {@code JobInitializer} to populate runtime options
     */
    public BasicJob(JobInitializer initializer) {
        priority = initializer.getPriority();
        requiresNetwork = initializer.requiresNetwork();
        isPersistent = initializer.isPersistent();
        group = initializer.getGroup();
        retryLimit = initializer.getRetryLimit();
        backoffPolicy = initializer.getBackoffPolicy();

        retryCount = 0;

        if(getInitialLockCount() > 0) {
            awaiter = new CountDownLatch(getInitialLockCount());
        }
    }

    /**
     * {@inheritDoc}
     * The default is {@link JobInitializer#DEFAULT_PRIORITY}.
     */
    @Override
    public final int getPriority() {
        return priority;
    }

    /**
     * {@inheritDoc}
     * The default is {@link JobInitializer#DEFAULT_REQUIRES_NETWORK}.
     */
    @Override
    public final boolean requiresNetwork() {
        return requiresNetwork;
    }

    /**
     * {@inheritDoc}
     * The default is {@link BasicJob#DEFAULT_CAN_REACH_REQUIRED_NETWORK}.
     */
    @Override
    public boolean canReachRequiredNetwork() {
        return DEFAULT_CAN_REACH_REQUIRED_NETWORK;
    }

    /**
     * {@inheritDoc}
     * The default is {@link JobInitializer#DEFAULT_IS_PERSISTENT}.
     */
    @Override
    public final boolean isPersistent() {
        return isPersistent;
    }

    /**
     * {@inheritDoc}
     * The default is {@link JobInitializer#DEFAULT_GROUP}.
     */
    @Override
    public final String getGroup() {
        return group;
    }

    /**
     * {@inheritDoc}
     * The default is {@link JobInitializer#DEFAULT_RETRY_LIMIT}.
     */
    @Override
    public final int getRetryLimit() {
        return retryLimit;
    }

    /**
     * {@inheritDoc}
     * The default is {@link JobInitializer#DEFAULT_BACKOFF_POLICY}.
     */
    @Override
    public final BackoffPolicy getBackoffPolicy() {
        return backoffPolicy;
    }

    @Override
    public final int getRetryCount() {
        return retryCount;
    }

    @Override
    public final void incrementRetryCount() {
        retryCount++;
    }

    /**
     * {@inheritDoc}
     * The default is to not retry.
     */
    @Override
    public boolean onError(Throwable e) {
        return false;
    }

    @Override
    public void onRetry() {}

    @Override
    public int getInitialLockCount() {
        return 0;
    }

    @Override
    public final void await() throws InterruptedException {
        if(awaiter != null) {
            awaiter.await();
        }
    }

    @Override
    public final void unlock() {
        if(awaiter != null) {
            awaiter.countDown();
        }
    }
}
