package com.staticbloc.jobs;

public interface Job {
    public static final int NO_RETRY_LIMIT = -1;

    /**
     * Returns this {@link Job}'s priority. Higher is more important.
     * @return the priority
     */
    public int getPriority();

    /**
     *
     * @return whether this {@link Job} needs a network connection in order to run
     */
    public boolean requiresNetwork();

    /**
     * This method will be called for a {@link Job} that {@link Job#requiresNetwork()}
     * returns {@code true}. It will only be called if the device is currently connected to a network,
     * so there shouldn't be any need to utilize {@link android.net.ConnectivityManager}.
     * It will be run on a background {@link java.lang.Thread}.
     * @return {@code true} if the required network can be reached
     */
    public boolean canReachRequiredNetwork();

    /**
     *
     * @return whether this {@link Job} should be persisted
     */
    public boolean isPersistent();

    /**
     *
     * @return the job group that this {@link Job} belongs to
     */
    public String getGroup();

    /**
     * Returns the maximum amount of times this {@link Job} can be retried.
     * If {@link Job#NO_RETRY_LIMIT} is returned, the {@code Job} will keep retrying
     * until it succeeds, or {@link Job#onError(Throwable)} returns {@code false}.
     * @return the maximum amount of times this {@code Job} can be retried
     */
    public int getRetryLimit();

    /**
     * Returns the {@link BackoffPolicy} for this {@code Job}.
     * @return the {@link BackoffPolicy} for this {@code Job}
     */
    public BackoffPolicy getBackoffPolicy();

    /**
     * This method returns whether multiple {@link Job}s with the same UID can be in a
     * {@link JobQueue} at the same time.
     */
    public boolean areMultipleInstancesAllowed();

    /**
     * Returns a UID used to identify if a current job is running.
     * If a class/instance member is being used as part of the UID,
     * it must be instantiated by the time the constructor returns.
     *
     * @return a UID represented as a {@link String} to uniquely identify this instance
     */
    public String getUID();

    /**
     *
     * @return the amount of times this {@link Job} has been retried
     */
    public int getRetryCount();

    /**
     * Called when a {@link Job} needs to retry so that implementations can
     * increment their retry counter.
     */
    public void incrementRetryCount();

    /**
     * This method will be called on a background thread. It is where your main {@link Job} logic
     * should go. If a {@link java.lang.Throwable} is thrown in this method,
     * {@link Job#onError(Throwable)} will be called.
     */
    public void performJob() throws Throwable;

    /**
     * Called when a {@link java.lang.Throwable} is raised in {@link Job#performJob()}.
     * If {@code true} is returned and the {@link Job} has not yet reached its max retries,
     * the {@code Job} will be retried. The amount of time waited until the next retry is determined
     * the {@code Job}'s priority and {@link BackoffPolicy}.
     * @param e the {@code Throwable} raised in {@code performJob()}
     * @return {@code true} to retry; otherwise {@code false}
     * @see BasicJob#performJob()
     * @see BasicJob#getRetryLimit()
     * @see BasicJob#getBackoffPolicy()
     */
    public boolean onError(Throwable e);

    /**
     * Called right after determining that a retry is needed.
     */
    public void onRetry();

    /**
     * Called when this {@code Job} is explicitly canceled.
     */
    public void onCanceled();

    /**
     * Called if we need to retry, but {@code getRetryCount() >= getRetryLimit()}.
     */
    public void onRetryLimitReached();
}
