package com.staticbloc.jobs;

public interface JobQueue {
    /**
     * Add a new {@link Job} to the queue.
     * @param job the {@code Job} to add
     * @return a {@link JobStatus} indicating whether the {@code Job}
     * was added successfully, and if so, some information about the {@code Job}.
     */
    public JobStatus enqueue(Job job);

    /**
     * Add a new {@link Job} to the queue. It will not be run until at least the {@code delayMillis}
     * passed.
     * @param job the {@code Job} to add
     * @param delayMillis how long to wait before running the {@code Job}
     * @return a {@link JobStatus} indicating whether the {@code Job}
     * was added successfully, and if so, some information about the {@code Job}.
     */
    public JobStatus enqueue(Job job, long delayMillis);

    /**
     * Makes a best effort attempt to cancel the {@link Job} before it runs.
     * Once it has started running, it will not get canceled.
     * @param jobId the id of the {@code Job} to be canceled
     */
    public void cancel(long jobId);

    /**
     * Makes a best effort to cancel all {@link Job}s currently in the queue.
     */
    public void cancelAll();

    /**
     * Get a {@link JobStatus} for a specific {@link Job}'s id.
     * @param jobId the id of the {@code Job}
     * @return the {@code Job}'s {@code JobStatus} if it is in the queue, or null
     */
    public JobStatus getStatus(long jobId);
}
