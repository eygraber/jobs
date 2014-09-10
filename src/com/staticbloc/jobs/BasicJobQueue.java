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
    private PriorityBlockingQueue<JobWrapper> queue;

    private LinkedList<JobWrapper> newJobs;
    private LinkedList<JobWrapper> futureJobs;
    private Map<Long, JobStatus> jobStatusMap;
    private Set<Long> canceledJobs;
    private Set<String> inFlightUIDs;

    private AtomicLong nextJobId;
    private AtomicBoolean shouldCancelAll;
    private AtomicBoolean isConnectedToNetwork;

    public BasicJobQueue(Context context) {
        queue = new PriorityBlockingQueue<JobWrapper>(15, new JobComparator());

        newJobs = new LinkedList<JobWrapper>();
        futureJobs = new LinkedList<JobWrapper>();
        jobStatusMap = new HashMap<Long, JobStatus>();
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
            newJobs.addLast(new JobWrapper(status));
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
            status.setState(JobStatus.State.COLD_STORAGE);
            futureJobs.addLast(new JobWrapper(status, System.currentTimeMillis() + delayMillis));
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
            JobWrapper jw = newJobs.getFirst();
            if(jw != null) {
                jw.getStatus().setState(JobStatus.State.QUEUED);
                queue.add(jw);
            }
        }
    }

    private void moveFutureJobsToQueueIfReady() {
        while(!futureJobs.isEmpty()) {
            JobWrapper jw = futureJobs.getFirst();
            if(jw != null && jw.getValidAtTime() <= System.currentTimeMillis()) {
                jw.getStatus().setState(JobStatus.State.QUEUED);
                queue.add(jw);
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
            status.setState(JobStatus.State.CANCELED);
        }
        jobStatusMap.clear();
        canceledJobs.clear();
        inFlightUIDs.clear();
    }

    @Override
    public JobStatus getStatus(long jobId) {
        return jobStatusMap.get(jobId);
    }

    private static class JobWrapper {
        private long validAtTime;
        private JobStatus status;

        private int networkRetryCount = 0;
        private BackoffPolicy networkBackoffPolicy = new BackoffPolicy.Step(1000, 3000);

        public JobWrapper(JobStatus status) {
            this(status, System.currentTimeMillis());
        }

        public JobWrapper(JobStatus status, long validAtTime) {
            this.status = status;
            this.validAtTime = validAtTime;
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
    }

    private static class JobComparator implements Comparator<JobWrapper> {
        @Override
        public int compare(JobWrapper lhs, JobWrapper rhs) {
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

                JobWrapper wrapper;
                try {
                    wrapper = queue.take();
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

                Job j = wrapper.getJob();

                if(canceledJobs.contains(wrapper.getJobId())) {
                    wrapper.getStatus().setState(JobStatus.State.CANCELED);
                    try {
                        j.onCanceled();
                    } catch(Throwable ignore) {}
                    canceledJobs.remove(wrapper.getJobId());
                    continue;
                }

                if(j.requiresNetwork()) {
                    try {
                        if(!isConnectedToNetwork.get() || !j.canReachRequiredNetwork()) {
                            wrapper.incrementNetworkRetryCount();
                            wrapper.setValidAtTime(System.currentTimeMillis() + wrapper.getNetworkRetryBackoffMillis());
                            wrapper.getStatus().setState(JobStatus.State.COLD_STORAGE);
                            futureJobs.addLast(wrapper);
                            continue;
                        }
                    } catch(Throwable ignore) {}
                }

                boolean retry = false;
                do {
                    try {
                        j.performJob();
                        if(j instanceof Waitable) {
                            ((Waitable) j).await();
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
                                    wrapper.setValidAtTime(System.currentTimeMillis() + backoffMillis);
                                    wrapper.getStatus().setState(JobStatus.State.COLD_STORAGE);
                                    futureJobs.addLast(wrapper);
                                }
                            }
                        }
                    }
                } while(retry);

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
