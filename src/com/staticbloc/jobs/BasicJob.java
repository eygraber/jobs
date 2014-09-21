package com.staticbloc.jobs;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class BasicJob implements Job, Waitable {
    public final static boolean DEFAULT_CAN_REACH_REQUIRED_NETWORK = true;

    private int priority;
    private boolean requiresNetwork;
    private boolean isPersistent;
    private String group;
    private int retryLimit;
    private BackoffPolicy backoffPolicy;

    private Throwable asyncThrowable = null;

    private int retryCount;

    private CountDownLatch awaiter;
    private final AtomicInteger asyncCountdown;

    private Set<Object> subsections;

    /**
     * Will create a {@code BasicJob} using the defaults from {@link JobInitializer}.
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
        asyncCountdown = new AtomicInteger(getInitialLockCount());

        subsections = new HashSet<>();
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
    public boolean areMultipleInstancesAllowed() {
        return false;
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
    public void onCanceled() {}

    @Override
    public void onRetryLimitReached() {}

    @Override
    public int getInitialLockCount() {
        return 0;
    }

    @Override
    public final void waitForAsyncTasks() throws Throwable {
        if(awaiter != null) {
            awaiter.await();
            if(asyncThrowable != null) {
                throw asyncThrowable;
            }
        }
        if(asyncCountdown.get() > 0) {
            synchronized(asyncCountdown) {
                while(asyncCountdown.get() > 0) {
                    wait();
                    if(asyncThrowable != null) {
                        throw asyncThrowable;
                    }
                }
            }
        }
    }

    @Override
    public final void raiseThrowableFromAsyncTask(Throwable t) {
        asyncThrowable = t;
        notifyAsyncTaskDone();
    }

    @Override
    public final void notifyAsyncTaskDone() {
        synchronized(asyncCountdown) {
            asyncCountdown.decrementAndGet();
            notify();
        }
    }

    /**
     * Check if a subsection of this {@code BasicJob}'s code is complete. This is useful
     * when code should not be run more than once in a {@code BasicJob} even after a retry (such
     * as a mutating network call). Subsections are on a per-instance basis, and will be persisted
     * if needed.<br />
     *
     * Typical usage is:
     * <pre>
     * {@code
     *
     * public class MyBasicJob extends BasicJob {
     *     Object subsection2 = new Object();
     *
     *     public void performJob() {
     *         if(!isSubsectionComplete("subsection1") {
     *             // do something
     *             setSubsectionComplete("test");
     *         }
     *         if(!isSubsectionComplete(subsection2)) {
     *             // do something
     *             setSubsectionComplete(subsection2);
     *         }
     *     }
     * }
     * }
     * </pre>
     * @param subsectionKey an {@code Object} that functions as a key to identify a subsection
     * @see BasicJob#isSubsectionComplete(Object)
     */
    protected final boolean isSubsectionComplete(Object subsectionKey) {
        return subsections.contains(subsectionKey);
    }

    /**
     * Mark a subsection of this {@code BasicJob}'s code as being complete. This is useful
     * when code should not be run more than once in a {@code BasicJob} even after a retry (such
     * as a mutating network call). Subsections are on a per-instance basis, and will be persisted
     * if needed.<br /><br />
     *
     * Typical usage is:
     * <pre>
     * {@code
     *
     * public class MyBasicJob extends BasicJob {
     *     Object subsection2 = new Object();
     *
     *     public void performJob() {
     *         if(!isSubsectionComplete("subsection1") {
     *             // do something
     *             setSubsectionComplete("test");
     *         }
     *         if(!isSubsectionComplete(subsection2)) {
     *             // do something
     *             setSubsectionComplete(subsection2);
     *         }
     *     }
     * }
     * }
     * </pre>
     * @param subsectionKey an {@code Object} that functions as a key to identify a subsection
     * @see BasicJob#isSubsectionComplete(Object)
     */
    protected final void setSubsectionComplete(Object subsectionKey) {
        subsections.add(subsectionKey);
    }

    /**
     * Marks a subsection of this {@code BasicJob}'s code as being incomplete.
     * @param subsectionKey an {@code Object} that functions as a key to identify a subsection
     */
    protected final void clearSubsection(Object subsectionKey) {
        subsections.remove(subsectionKey);
    }

    /**
     * Marks all subsections of this {@code BasicJob}'s code as being incomplete.
     */
    protected final void clearAllSubsections() {
        subsections.clear();
    }
}
