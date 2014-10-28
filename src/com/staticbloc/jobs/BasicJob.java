package com.staticbloc.jobs;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class BasicJob implements Job, Waitable {
    public final static boolean DEFAULT_CAN_REACH_REQUIRED_NETWORK = true;

    private int priority;
    private boolean requiresNetwork;
    private boolean isPersistent;
    private String group;
    private int retryLimit;
    private BackoffPolicy backoffPolicy;

    private int retryCount;

    private transient Throwable asyncThrowable = null;

    private transient final AtomicInteger asyncCountdown;

    private transient Set<String> subsections;

    private transient State state = State.NOTHING;
    private transient final Object isDoneWaiter = new Object();

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

        asyncCountdown = new AtomicInteger(0);

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
    public final State getState() {
        return state;
    }

    @Override
    public final void setState(State state) {
        this.state = state;
        switch(state) {
            case CANCELED:
            case FAILED:
            case FINISHED:
                synchronized (isDoneWaiter) {
                    isDoneWaiter.notifyAll();
                }
        }
    }

    @Override
    public State waitUntilDone() throws InterruptedException {
        synchronized (isDoneWaiter) {
            while(state != State.CANCELED && state != State.FAILED && state != State.FINISHED) {
                isDoneWaiter.wait();
            }
        }
        return state;
    }

    @Override
    public State waitUntilDone(long timeoutMillis) throws IllegalArgumentException, InterruptedException {
        synchronized (isDoneWaiter) {
            while(state != State.CANCELED && state != State.FAILED && state != State.FINISHED) {
                isDoneWaiter.wait(timeoutMillis);
            }
        }
        return state;
    }

    @Override
    public final int registerAsyncTask() {
        return asyncCountdown.incrementAndGet();
    }

    @Override
    public final int asyncTaskCount() {
        return asyncCountdown.get();
    }

    @Override
    public final void waitForAsyncTasks() throws Throwable {
        if(asyncCountdown.get() > 0) {
            synchronized(asyncCountdown) {
                while(asyncCountdown.get() > 0) {
                    asyncCountdown.wait();
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
            asyncCountdown.notify();
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
     * public class MyBasicJob extends BasicJob {     *
     *     public void performJob() {
     *         if(!isSubsectionComplete("subsection1") {
     *             // do something
     *             setSubsectionComplete("subsection1");
     *         }
     *         if(!isSubsectionComplete("subsection2")) {
     *             // do something
     *             setSubsectionComplete("subsection2");
     *         }
     *     }
     * }
     * }
     * </pre>
     * @param subsectionKey an {@code Object} that functions as a key to identify a subsection
     * @see BasicJob#setSubsectionComplete(String)
     */
    protected final boolean isSubsectionComplete(String subsectionKey) {
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
     * public class MyBasicJob extends BasicJob {     *
     *     public void performJob() {
     *         if(!isSubsectionComplete("subsection1") {
     *             // do something
     *             setSubsectionComplete("subsection1");
     *         }
     *         if(!isSubsectionComplete("subsection2")) {
     *             // do something
     *             setSubsectionComplete("subsection2");
     *         }
     *     }
     * }
     * }
     * </pre>
     * @param subsectionKey an {@code String} that functions as a key to identify a subsection
     * @see BasicJob#isSubsectionComplete(String)
     */
    protected final void setSubsectionComplete(String subsectionKey) {
        subsections.add(subsectionKey);
    }

    /**
     * Marks a subsection of this {@code BasicJob}'s code as being incomplete.
     * @param subsectionKey an {@code String} that functions as a key to identify a subsection
     */
    protected final void clearSubsection(String subsectionKey) {
        subsections.remove(subsectionKey);
    }

    /**
     * Marks all subsections of this {@code BasicJob}'s code as being incomplete.
     */
    protected final void clearAllSubsections() {
        subsections.clear();
    }
}
