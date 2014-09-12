package com.staticbloc.jobs;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;

import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class BasicJobQueue implements JobQueue {
    private String name;

    private PriorityBlockingQueue<JobQueueItem> queue;

    private LinkedList<JobQueueItem> newJobs;
    private LinkedList<JobQueueItem> futureJobs;
    private Map<String, LinkedList<JobQueueItem>> groupMap;
    private Map<Long, JobQueueItem> jobItemMap;
    private Map<String, Integer> groupIndexMap;
    private Set<Long> canceledJobs;
    private Set<String> inFlightUIDs;

    private final Object groupIndexMapLock = new Object();

    private AtomicLong nextJobId;
    private AtomicBoolean shouldCancelAll;
    private AtomicBoolean isConnectedToNetwork;

    public BasicJobQueue(Context context) {
        queue = new PriorityBlockingQueue<JobQueueItem>(15, new JobComparator());

        newJobs = new LinkedList<JobQueueItem>();
        futureJobs = new LinkedList<JobQueueItem>();
        groupMap = new HashMap<String, LinkedList<JobQueueItem>>();
        jobItemMap = new HashMap<Long, JobQueueItem>();
        groupIndexMap = new HashMap<String, Integer>();
        canceledJobs = new HashSet<Long>();
        inFlightUIDs = new HashSet<String>();

        nextJobId = new AtomicLong(0);
        shouldCancelAll = new AtomicBoolean(false);
        isConnectedToNetwork = new AtomicBoolean(false);

        initNetworkConnectionChecker(context);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public JobStatus add(Job job) {
        if(job == null) {
            throw new IllegalArgumentException("Can't pass a null Job");
        }

        JobStatus status;
        if(job.areMultipleInstancesAllowed() || !inFlightUIDs.contains(job.getUID())) {
            status = new JobStatus(nextJobId.getAndIncrement(), job);
            JobQueueItem jobQueueItem = new JobQueueItem(status);
            newJobs.addLast(jobQueueItem);
            status.setState(JobStatus.State.ADDED);
            onPersistJobAdded(jobQueueItem);
            inFlightUIDs.add(job.getUID());
            jobItemMap.put(status.getJobId(), jobQueueItem);
        }
        else {
            status = new JobStatus(JobStatus.IDENTICAL_JOB_REJECTED);
        }
        return status;
    }

    @Override
    public JobStatus add(Job job, long delayMillis) {
        if(job == null) {
            throw new IllegalArgumentException("Can't pass a null Job");
        }

        JobStatus status;
        if(job.areMultipleInstancesAllowed() || !inFlightUIDs.contains(job.getUID())) {
            status = new JobStatus(nextJobId.getAndIncrement(), job);
            JobQueueItem jobQueueItem = new JobQueueItem(status, System.currentTimeMillis() + delayMillis);
            futureJobs.addLast(jobQueueItem);
            status.setState(JobStatus.State.COLD_STORAGE);
            onPersistJobAdded(jobQueueItem);
            inFlightUIDs.add(job.getUID());
            jobItemMap.put(status.getJobId(), jobQueueItem);
        }
        else {
            status = new JobStatus(JobStatus.IDENTICAL_JOB_REJECTED);
        }
        return status;
    }

    private void moveNewJobsToQueue() {
        while(!newJobs.isEmpty()) {
            JobQueueItem job = newJobs.getFirst();
            if(job != null) {
                if(job.isGroupMember()) {
                    job.obtainGroupIndex();
                }
                queue.add(job);
                job.getStatus().setState(JobStatus.State.QUEUED);
            }
        }
    }

    private void moveFutureJobsToQueueIfReady() {
        while(!futureJobs.isEmpty()) {
            JobQueueItem job = futureJobs.getFirst();
            if(job != null && job.getValidAtTime() <= System.currentTimeMillis()) {
                queue.add(job);
                job.getStatus().setState(JobStatus.State.QUEUED);
            }
        }
    }

    @Override
    public void cancel(long jobId) {
        JobQueueItem jobQueueItem = jobItemMap.get(jobId);
        if(jobQueueItem != null) {
            JobStatus status = jobQueueItem.getStatus();
            if(status != null) {
                Job j = status.getJob();
                if(j != null) {
                    canceledJobs.add(jobId);
                    onPersistJobRemoved(jobQueueItem);
                }
            }
        }
    }

    @Override
    public void cancelAll() {
        shouldCancelAll.set(true);
    }

    private void clearAll() {
        queue.clear();
        newJobs.clear();
        ArrayList<Long> jobStatusKeys = new ArrayList<Long>(jobItemMap.keySet());
        for (Long id : jobStatusKeys) {
            JobQueueItem jobQueueItem = jobItemMap.get(id);
            if(jobQueueItem != null && jobQueueItem.getStatus() != null) {
                jobQueueItem.getStatus().setState(JobStatus.State.CANCELED);
            }
        }
        jobItemMap.clear();
        canceledJobs.clear();
        inFlightUIDs.clear();
    }

    @Override
    public JobStatus getStatus(long jobId) {
        JobQueueItem jobQueueItem = jobItemMap.get(jobId);
        if(jobQueueItem != null) {
            return jobQueueItem.getStatus();
        }
        else {
            return null;
        }
    }



    @Override
    public void shutdown() {
        shutdown(true);
    }

    @Override
    public void shutdown(boolean keepPersisted) {

    }

    /**
     * Should only be called by subclasses of {@link BasicJobQueue} from {@link BasicJobQueue#onLoadPersistedJobs()}.
     * Adds a {@link java.util.List} of persisted {@code Job}s to the queue.
     * @param jobs the {@code List} of loaded persisted {@code Job}s that need to be added to the queue.
     */
    protected final void addPersistedJobs(List<JobQueueItem> jobs) {
        for(JobQueueItem job : jobs) {
            if(job != null && job.getStatus() != null && job.getJob() != null) {
                if(job.getJob().areMultipleInstancesAllowed() || !inFlightUIDs.contains(job.getJob().getUID())) {
                    if(job.getValidAtTime() <= System.currentTimeMillis()) {
                        queue.add(job);
                        job.getStatus().setState(JobStatus.State.QUEUED);
                    }
                    else {
                        futureJobs.addLast(job);
                        job.getStatus().setState(JobStatus.State.COLD_STORAGE);
                    }
                    inFlightUIDs.add(job.getJob().getUID());
                    jobItemMap.put(job.getJobId(), job);
                }
            }
        }
    }

    /**
     * Called when this {@link BasicJobQueue} is created.
     * @see BasicJobQueue#addPersistedJobs(java.util.List)
     *
     */
    @SuppressWarnings("unused")
    protected void onLoadPersistedJobs() {/*Intentionally empty*/}

    /**
     * Called when a {@link Job} is added via the public API.
     * @param job the {@code Job} that was added
     * @see JobQueue#add(Job)
     * @see JobQueue#add(Job, long)
     */
    @SuppressWarnings("unused")
    protected void onPersistJobAdded(JobQueueItem job) {/*Intentionally empty*/}

    /**
     * Called when a {@link Job} has been canceled via the public API,
     * completed successfully, or failed and couldn't be retried.
     * @param job the {@code Job} that was removed
     * @see JobQueue#cancel(long)
     */
    @SuppressWarnings("unused")
    protected void onPersistJobRemoved(JobQueueItem job) {/*Intentionally empty*/}

    /**
     * Called when all {@link Job}s have been canceled via the public API.
     * @see JobQueue#cancelAll()
     */
    @SuppressWarnings("unused")
    protected void onPersistAllJobsCanceled() {/*Intentionally empty*/}

    /**
     * Called when a {@link Job} has failed, and will be retried at some point.
     * @param job the {@code Job} that has failed
     */
    @SuppressWarnings("unused")
    protected void onPersistJobModified(JobQueueItem job) {/*Intentionally empty*/}

    protected class JobQueueItem {
        private long validAtTime;
        private JobStatus status;
        private int groupIndex = -1;

        private int networkRetryCount = 0;
        private BackoffPolicy networkBackoffPolicy = new BackoffPolicy.Step(1000, 3000);
        private int groupRetryCount = 0;
        private BackoffPolicy groupBackoffPolicy = new BackoffPolicy.Linear(1000);

        public JobQueueItem(JobStatus status) {
            this(status, System.currentTimeMillis());
        }

        public JobQueueItem(JobStatus status, long validAtTime) {
            this.status = status;
            this.validAtTime = validAtTime;
        }

        public void obtainGroupIndex() {
            String group = status.getJob().getGroup();
            if(group != null) {
                synchronized(groupIndexMapLock) {
                    Integer index = groupIndexMap.get(group);
                    if(index == null) {
                        index = 0;
                    }
                    groupIndexMap.put(group, index + 1);
                    LinkedList<JobQueueItem> groupQueue = groupMap.get(group);
                    if(groupQueue == null) {
                        groupQueue = new LinkedList<JobQueueItem>();
                        groupMap.put(group, groupQueue);
                    }
                    groupQueue.addLast(this);
                    this.groupIndex = index;
                }
            }
        }

        public Job getJob() {
            return status.getJob();
        }

        public JobStatus getStatus() {
            return status;
        }

        public long getJobId() {
            return status.getJobId();
        }

        public long getValidAtTime() {
            return validAtTime;
        }

        public void setValidAtTime(long validAtTime) {
            this.validAtTime = validAtTime;
        }

        public void incrementNetworkRetryCount() {
            networkRetryCount++;
        }

        public long getNetworkRetryBackoffMillis() {
            return networkBackoffPolicy.getNextMillis(networkRetryCount);
        }

        public void incrementGroupRetryCount() {
            groupRetryCount++;
        }

        public long getGroupRetryBackoffMillis() {
            return groupBackoffPolicy.getNextMillis(groupRetryCount);
        }

        public boolean isGroupMember() {
            return groupIndex >= 0 && status.getJob().getGroup() != null;
        }

        public int getGroupIndex() {
            return groupIndex;
        }

        public String getGroup() {
            return status.getJob().getGroup();
        }

        public boolean equals(JobQueueItem other) {
            if(other == null) {
                return false;
            }
            else {
                return getJob().getUID().equals(other.getJob().getUID());
            }
        }
    }

    private static class JobComparator implements Comparator<JobQueueItem> {
        @Override
        public int compare(JobQueueItem lhs, JobQueueItem rhs) {
            // we shouldn't have to check JobWrappers for null because PriorityBlockingQueues don't allow null
            Job lhsJob = lhs.getJob();
            Job rhsJob = rhs.getJob();
            if(lhsJob == null) {
                if(rhsJob == null) {
                    return 0;
                }
                else {
                    return -1;
                }
            }
            else if(rhsJob == null) {
                return 1;
            }
            else {
                if(lhs.isGroupMember() && rhs.isGroupMember()) {
                    if(lhsJob.getGroup().equals(rhsJob.getGroup())) {
                        return rhs.getGroupIndex() - lhs.getGroupIndex();
                    }
                }

                return lhsJob.getPriority() - rhsJob.getPriority();
            }
        }
    }

    private class QueueThread extends Thread {
        private boolean keepRunning = true;

        public void stopRunning() {
            keepRunning = false;
        }

        @Override
        public void run() {
            while(keepRunning) {
                if(shouldCancelAll.get()) {
                    clearAll();
                    shouldCancelAll.set(false);
                }
                else {
                    moveNewJobsToQueue();
                    moveFutureJobsToQueueIfReady();
                }

                JobQueueItem job;
                try {
                    job = queue.take();
                } catch (InterruptedException e) {
                    continue;
                }

                if(shouldCancelAll.get()) {
                    clearAll();
                    shouldCancelAll.set(false);
                    continue;
                }
                else {
                    moveNewJobsToQueue();
                    moveFutureJobsToQueueIfReady();
                }

                if(canceledJobs.contains(job.getJobId())) {
                    job.getStatus().setState(JobStatus.State.CANCELED);
                    try {
                        job.getJob().onCanceled();
                    } catch(Throwable ignore) {}
                    canceledJobs.remove(job.getJobId());
                    continue;
                }

                if(job.getJob().requiresNetwork()) {
                    try {
                        if(!isConnectedToNetwork.get() || !job.getJob().canReachRequiredNetwork()) {
                            job.incrementNetworkRetryCount();
                            job.setValidAtTime(System.currentTimeMillis() + job.getNetworkRetryBackoffMillis());
                            job.getStatus().setState(JobStatus.State.COLD_STORAGE);
                            futureJobs.addLast(job);
                            continue;
                        }
                    } catch(Throwable ignore) {}
                }

                if(job.isGroupMember()) {
                    LinkedList<JobQueueItem> groupQueue = groupMap.get(job.getGroup());
                    // it should never be null
                    if(groupQueue != null) {
                        if(!job.equals(groupQueue.peekFirst())) {
                            job.incrementGroupRetryCount();
                            job.setValidAtTime(System.currentTimeMillis() + job.getGroupRetryBackoffMillis());
                            job.getStatus().setState(JobStatus.State.COLD_STORAGE);
                            futureJobs.addLast(job);
                            continue;
                        }
                    }
                }

                boolean retry = false;
                do {
                    try {
                        job.getJob().performJob();
                        if(job.getJob() instanceof Waitable) {
                            ((Waitable) job.getJob()).await();
                        }
                        onPersistJobRemoved(job);
                        if(job.isGroupMember()) {
                            popGroupQueue(job);
                        }
                    }
                    catch(Throwable e) {
                        try {
                            retry = job.getJob().onError(e);
                        } catch(Throwable ignore) {
                            // if onError throws a Throwable we don't retry it
                            retry = false;
                        }
                        if(retry) {
                            job.getJob().incrementRetryCount();
                            if(job.getJob().getRetryCount() >= job.getJob().getRetryLimit()) {
                                retry = false;
                                try {
                                    job.getJob().onRetryLimitReached();
                                } catch(Throwable ignore) {}
                                onPersistJobRemoved(job);
                                if(job.isGroupMember()) {
                                    popGroupQueue(job);
                                }
                            }
                            else {
                                try {
                                    job.getJob().onRetry();
                                } catch(Throwable ignore) {}
                                long backoffMillis = job.getJob().getBackoffPolicy()
                                                        .getNextMillis(job.getJob().getRetryCount());
                                // if backoffMillis is 0 continue with the retry
                                // if it's <= 1 second just sleep it off instead of reinserting into queue
                                // otherwise update the timestamp and stick it back in the queue
                                if(backoffMillis > 0 && backoffMillis <= 1000) {
                                    try {
                                        Thread.sleep(backoffMillis);
                                    } catch (InterruptedException ignore) {}
                                }
                                else if(backoffMillis > 1000) {
                                    retry = false;
                                    job.setValidAtTime(System.currentTimeMillis() + backoffMillis);
                                    job.getStatus().setState(JobStatus.State.COLD_STORAGE);
                                    futureJobs.addLast(job);
                                }
                                onPersistJobModified(job);
                            }
                        }
                        else {
                            onPersistJobRemoved(job);
                            if(job.isGroupMember()) {
                                popGroupQueue(job);
                            }
                        }
                    }
                } while(retry);

            }
        }

        private void popGroupQueue(JobQueueItem item) {
            synchronized(groupIndexMapLock) {
                LinkedList<JobQueueItem> groupQueue = groupMap.get(item.getGroup());
                // it should never be null
                if(groupQueue != null) {
                    // since we're the head of the queue
                    // pop the queue so the next job in the group can run
                    groupQueue.removeFirst();
                    //reset the group index
                    if(groupQueue.isEmpty()) {
                        groupIndexMap.put(item.getGroup(), 0);
                    }
                }
            }
        }
    }

    private void initNetworkConnectionChecker(Context context) {
        if(context == null || context.getApplicationContext() == null) {
            throw new IllegalArgumentException("Cannot pass a null Context");
        }

        context.getApplicationContext().registerReceiver(new BroadcastReceiver() {
            @Override
            public void onReceive(Context context, Intent intent) {
                ConnectivityManager cm = (ConnectivityManager) context.getSystemService(Context.CONNECTIVITY_SERVICE);
                NetworkInfo netInfo = cm.getActiveNetworkInfo();
                if(netInfo != null) {
                    isConnectedToNetwork.set(netInfo.isConnected());
                }
                else {
                    isConnectedToNetwork.set(false);
                }
            }
        }, new IntentFilter(ConnectivityManager.CONNECTIVITY_ACTION));
        ConnectivityManager cm = (ConnectivityManager) context.getSystemService(Context.CONNECTIVITY_SERVICE);
        NetworkInfo netInfo = cm.getActiveNetworkInfo();
        if(netInfo != null) {
            isConnectedToNetwork.set(netInfo.isConnected());
        }
        else {
            isConnectedToNetwork.set(false);
        }
    }
}
