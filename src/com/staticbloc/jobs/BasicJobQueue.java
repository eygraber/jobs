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
    private PriorityBlockingQueue<JobQueueItem> queue;

    private LinkedList<JobQueueItem> newJobs;
    private LinkedList<JobQueueItem> futureJobs;
    private Map<String, LinkedList<JobQueueItem>> groupMap;
    private Map<Long, JobStatus> jobStatusMap;
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
        jobStatusMap = new HashMap<Long, JobStatus>();
        groupIndexMap = new HashMap<String, Integer>();
        canceledJobs = new HashSet<Long>();
        inFlightUIDs = new HashSet<String>();

        nextJobId = new AtomicLong(0);
        shouldCancelAll = new AtomicBoolean(false);
        isConnectedToNetwork = new AtomicBoolean(false);

        initNetworkConnectionChecker(context);
    }

    @Override
    public JobStatus enqueue(Job job) {
        if(job == null) {
            throw new IllegalArgumentException("Can't pass a null Job");
        }

        JobStatus status;
        if(job.areMultipleInstancesAllowed() || !inFlightUIDs.contains(job.getUID())) {
            status = new JobStatus(nextJobId.getAndIncrement(), job);
            newJobs.addLast(new JobQueueItem(status));
            status.setState(JobStatus.State.ADDED);
            inFlightUIDs.add(job.getUID());
            jobStatusMap.put(status.getJobId(), status);
        }
        else {
            status = new JobStatus(JobStatus.IDENTICAL_JOB_REJECTED);
        }
        return status;
    }

    @Override
    public JobStatus enqueue(Job job, long delayMillis) {
        if(job == null) {
            throw new IllegalArgumentException("Can't pass a null Job");
        }

        JobStatus status;
        if(job.areMultipleInstancesAllowed() || !inFlightUIDs.contains(job.getUID())) {
            status = new JobStatus(nextJobId.getAndIncrement(), job);
            futureJobs.addLast(new JobQueueItem(status, System.currentTimeMillis() + delayMillis));
            status.setState(JobStatus.State.COLD_STORAGE);
            inFlightUIDs.add(job.getUID());
            jobStatusMap.put(status.getJobId(), status);
        }
        else {
            status = new JobStatus(JobStatus.IDENTICAL_JOB_REJECTED);
        }
        return status;
    }

    private void moveNewJobsToQueue() {
        while(!newJobs.isEmpty()) {
            JobQueueItem item = newJobs.getFirst();
            if(item != null) {
                if(item.isGroupMember()) {
                    item.obtainGroupIndex();
                }
                queue.add(item);
                item.getStatus().setState(JobStatus.State.QUEUED);
            }
        }
    }

    private void moveFutureJobsToQueueIfReady() {
        while(!futureJobs.isEmpty()) {
            JobQueueItem item = futureJobs.getFirst();
            if(item != null && item.getValidAtTime() <= System.currentTimeMillis()) {
                queue.add(item);
                item.getStatus().setState(JobStatus.State.QUEUED);
            }
        }
    }

    @Override
    public void cancel(long jobId) {
        JobStatus status = getStatus(jobId);
        if(status != null) {
            Job j = status.getJob();
            if(j != null) {
                canceledJobs.add(jobId);
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
        ArrayList<Long> jobStatusKeys = new ArrayList<Long>(jobStatusMap.keySet());
        for (Long id : jobStatusKeys) {
            JobStatus status = jobStatusMap.get(id);
            if(status != null) {
                status.setState(JobStatus.State.CANCELED);
            }
        }
        jobStatusMap.clear();
        canceledJobs.clear();
        inFlightUIDs.clear();
    }

    @Override
    public JobStatus getStatus(long jobId) {
        return jobStatusMap.get(jobId);
    }

    @Override
    public void shutdown() {

    }

    private class JobQueueItem {
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

                JobQueueItem item;
                try {
                    item = queue.take();
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

                Job j = item.getJob();

                if(canceledJobs.contains(item.getJobId())) {
                    item.getStatus().setState(JobStatus.State.CANCELED);
                    try {
                        j.onCanceled();
                    } catch(Throwable ignore) {}
                    canceledJobs.remove(item.getJobId());
                    continue;
                }

                if(j.requiresNetwork()) {
                    try {
                        if(!isConnectedToNetwork.get() || !j.canReachRequiredNetwork()) {
                            item.incrementNetworkRetryCount();
                            item.setValidAtTime(System.currentTimeMillis() + item.getNetworkRetryBackoffMillis());
                            item.getStatus().setState(JobStatus.State.COLD_STORAGE);
                            futureJobs.addLast(item);
                            continue;
                        }
                    } catch(Throwable ignore) {}
                }

                if(item.isGroupMember()) {
                    LinkedList<JobQueueItem> groupQueue = groupMap.get(item.getGroup());
                    // it should never be null
                    if(groupQueue != null) {
                        if(!item.equals(groupQueue.peekFirst())) {
                            item.incrementGroupRetryCount();
                            item.setValidAtTime(System.currentTimeMillis() + item.getGroupRetryBackoffMillis());
                            item.getStatus().setState(JobStatus.State.COLD_STORAGE);
                            futureJobs.addLast(item);
                            continue;
                        }
                    }
                }

                boolean retry = false;
                do {
                    try {
                        j.performJob();
                        if(j instanceof Waitable) {
                            ((Waitable) j).await();
                        }
                        if(item.isGroupMember()) {
                            popGroupQueue(item);
                        }
                    }
                    catch(Throwable e) {
                        try {
                            retry = j.onError(e);
                        } catch(Throwable ignore) {
                            // if onError throws a Throwable we don't retry it
                            retry = false;
                        }
                        if(retry) {
                            j.incrementRetryCount();
                            if(j.getRetryCount() >= j.getRetryLimit()) {
                                retry = false;
                                try {
                                    j.onRetryLimitReached();
                                } catch(Throwable ignore) {}
                                if(item.isGroupMember()) {
                                    popGroupQueue(item);
                                }
                            }
                            else {
                                j.onRetry();
                                long backoffMillis = j.getBackoffPolicy().getNextMillis(j.getRetryCount());
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
                                    item.setValidAtTime(System.currentTimeMillis() + backoffMillis);
                                    item.getStatus().setState(JobStatus.State.COLD_STORAGE);
                                    futureJobs.addLast(item);
                                }
                            }
                        }
                        else {
                            if(item.isGroupMember()) {
                                popGroupQueue(item);
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
