package com.staticbloc.jobs;

public interface JobQueue {
    /**
     * Returns a name that uniquely identifies this {@link JobQueue}.
     */
    public String getName();

    /**
     * Starts this queue.
     */
    public void start();

    /**
     * Add a new {@link Job} to the queue.
     * @param job the {@code Job} to add
     * @return a {@link Job.State} indicating whether the {@code Job}
     * was added to the queue successfully or not
     */
    public Job.State add(Job job);

    /**
     * Add a new {@link Job} to the queue.
     * It will not be run until at least the {@code delayMillis} passed.
     * @param job the {@code Job} to add
     * @param delayMillis how long to wait before running the {@code Job}
     * @return a {@link Job.State} indicating whether the {@code Job}
     * was added to the queue successfully or not
     */
    public Job.State add(Job job, long delayMillis);

    /**
     * Makes a best effort attempt to cancel the {@link Job} before it runs.
     * Once it has started running, it will not get canceled.
     * @param job the {@code Job} to be canceled
     */
    public void cancel(Job job);

    /**
     * Makes a best effort to cancel all {@link Job}s currently in the queue.
     */
    public void cancelAll();

    /**
     * Get a {@link Job.State} for a specific {@link Job}.
     * @param job the {@code Job} to get the status for
     * @return the {@code Job}'s {@code State} if it is in the queue, or null
     */
    public Job.State getStatus(Job job);

    /**
     * Shuts down the {@link JobQueue} and releases its resources. Any non-persisted
     * {@link Job}s will be lost. The instance should not be used anymore
     * after calling shutdown.
     */
    public void shutdown();

    /**
     * Shuts down the {@link JobQueue} and releases its resources. Any non-persisted
     * {@link Job}s will be lost. The instance should not be used anymore
     * after calling shutdown.
     *
     * @param keepPersisted if {@code false} persisted {@code Job}s will be cleared
     */
    public void shutdown(boolean keepPersisted);
}
