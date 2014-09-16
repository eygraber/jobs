package com.staticbloc.jobs;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.SharedPreferences;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class BasicJobQueue implements JobQueue {
    private static final String SHARED_PREFS_NAME = "com.staticbloc.jobs.%s";
    private static final String SHARED_PREFS_JOB_ID_KEY = "job_id";

    private final String name;

    private PriorityBlockingQueue<JobRunnable> queue;
    private JobExecutor executor;
    private ScheduledExecutorService frozenJobExecutor;

    private Map<String, LinkedList<JobQueueItem>> groupMap;
    private Map<Long, JobRunnable> jobItemMap;
    private Map<String, Integer> groupIndexMap;
    private Set<Long> canceledJobs;
    private Set<String> inFlightUIDs;

    private final Object groupIndexMapLock = new Object();

    private SharedPreferences sharedPrefs;

    private AtomicLong nextJobId;
    private AtomicBoolean isGroupsReady;
    private AtomicBoolean isConnectedToNetwork;

    private JobQueueEventListener externalEventListener;

    private Context context;
    private BroadcastReceiver networkStatusReceiver;

    private static class JobExecutor {
        private AtomicBoolean isShutdown = new AtomicBoolean(false);
        private ThreadPoolExecutor executor;

        // suppress unchecked warnings because we can't cast
        // PriorityBlockingQueue<JobRunnable> to BlockingQueue<Runnable>
        // http://stackoverflow.com/questions/25865910/java-blockingqueuerunnable-inconvertible-types
        @SuppressWarnings("unchecked")
        public JobExecutor(final String queueName, int corePoolSize, int maximumPoolSize, long keepAliveTime,
                           PriorityBlockingQueue<JobRunnable> workQueue) {
            executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.MILLISECONDS,
                    (BlockingQueue) workQueue, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, queueName);
                }
            });
            executor.allowCoreThreadTimeOut(false);
        }

        public boolean remove(JobRunnable runnable) {
            if(!isShutdown.get()) {
                return executor.remove(runnable);
            }
            else {
                return true;
            }
        }

        public void shutdownNow() {
            if(!isShutdown.getAndSet(true)) {
                executor.shutdownNow();
            }
        }
    }

    public BasicJobQueue(Context context, JobQueueInitializer initializer) {
        if(initializer.getName() == null) {
            throw new IllegalArgumentException("JobQueueInitializer must provide a non-null name");
        }
        this.name = initializer.getName();

        if(context == null || context.getApplicationContext() == null) {
            throw new IllegalArgumentException(("Context must not be null"));
        }
        this.context = context.getApplicationContext();

        this.sharedPrefs = this.context.getSharedPreferences(
                String.format(SHARED_PREFS_NAME, getName()), Context.MODE_PRIVATE);

        queue = new PriorityBlockingQueue<>(15, new JobComparator());
        executor = new JobExecutor(name, initializer.getMinLiveConsumers(), initializer.getMaxLiveConsumers(),
                initializer.getConsumerKeepAliveSeconds(), queue);
        frozenJobExecutor = Executors.newSingleThreadScheduledExecutor();

        this.externalEventListener = initializer.getJobQueueEventListener();

        groupMap = new HashMap<>();
        jobItemMap = new HashMap<>();
        groupIndexMap = new HashMap<>();
        canceledJobs = new HashSet<>();
        inFlightUIDs = new HashSet<>();

        nextJobId = loadNextJobId();
        isGroupsReady = new AtomicBoolean(false);
        isConnectedToNetwork = new AtomicBoolean(false);

        initNetworkConnectionChecker();

        onLoadPersistedData();
    }

    private AtomicLong loadNextJobId() {
        return new AtomicLong(sharedPrefs.getLong(SHARED_PREFS_JOB_ID_KEY, 0));
    }

    /**
     * This method is synchronized to ensure that there is no interleaving when
     * storing the next id in shared preferences.
     * This method could end up being called from the main thread, but it shouldn't
     * be a problem.
     */
    private synchronized long incrementJobId() {
        long id = nextJobId.getAndIncrement();
        sharedPrefs.edit().putLong(SHARED_PREFS_JOB_ID_KEY, nextJobId.get()).apply();
        return id;
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public final JobStatus add(Job job) {
        if(job == null) {
            throw new IllegalArgumentException("Can't pass a null Job");
        }

        JobStatus status;
        if(job.areMultipleInstancesAllowed() || !inFlightUIDs.contains(job.getUID())) {
            status = new JobStatus(incrementJobId(), job);
            JobQueueItem jobQueueItem = new JobQueueItem(status);
            JobRunnable runnable = new JobRunnable(jobQueueItem);
            queue.add(runnable);
            status.setState(JobStatus.State.ADDED);
            onJobAdded(jobQueueItem);
            inFlightUIDs.add(job.getUID());
            jobItemMap.put(status.getJobId(), runnable);
        }
        else {
            status = new JobStatus(JobStatus.IDENTICAL_JOB_REJECTED);
        }
        return status;
    }

    @Override
    public final JobStatus add(Job job, long delayMillis) {
        if(job == null) {
            throw new IllegalArgumentException("Can't pass a null Job");
        }

        JobStatus status;
        if(job.areMultipleInstancesAllowed() || !inFlightUIDs.contains(job.getUID())) {
            status = new JobStatus(incrementJobId(), job);
            JobQueueItem jobQueueItem = new JobQueueItem(status, System.currentTimeMillis() + delayMillis);
            JobRunnable runnable = new JobRunnable(jobQueueItem);
            frozenJobExecutor.schedule(new FrozenJobRunnable(runnable), delayMillis, TimeUnit.MILLISECONDS);
            status.setState(JobStatus.State.COLD_STORAGE);
            onJobAdded(jobQueueItem);
            inFlightUIDs.add(job.getUID());
            jobItemMap.put(status.getJobId(), runnable);
        }
        else {
            status = new JobStatus(JobStatus.IDENTICAL_JOB_REJECTED);
        }
        return status;
    }

    private void internalAddToQueueOrColdStorage(JobRunnable jobRunnable) {
        JobQueueItem job = jobRunnable.getJob();
        if(job != null) {
            if(job.getValidAtTime() <= System.currentTimeMillis()) {
                job.getStatus().setState(JobStatus.State.QUEUED);
                queue.add(jobRunnable);
            }
            else {
                job.getStatus().setState(JobStatus.State.COLD_STORAGE);
                frozenJobExecutor.schedule(new FrozenJobRunnable(jobRunnable),
                        job.getValidAtTime() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    public final void cancel(long jobId) {
        JobRunnable runnable = jobItemMap.get(jobId);
        if(runnable != null) {
            JobQueueItem jobQueueItem = runnable.getJob();
            if(executor.remove(runnable)) {
                if(jobQueueItem != null) {
                    JobStatus status = jobQueueItem.getStatus();
                    if(status != null) {
                        Job job = status.getJob();
                        if(job != null) {
                            try {
                                job.onCanceled();
                            } catch(Throwable ignore) {}
                            onJobRemoved(jobQueueItem);
                        }
                    }
                }
            }
            else {
                if(jobQueueItem != null) {
                    JobStatus status = jobQueueItem.getStatus();
                    if(status != null) {
                        Job job = status.getJob();
                        if(job != null) {
                            canceledJobs.add(status.getJobId());
                        }
                    }
                }
            }
        }
    }

    @Override
    public final void cancelAll() {
        queue.clear();
        ArrayList<Long> jobStatusKeys = new ArrayList<>(jobItemMap.keySet());
        for (Long id : jobStatusKeys) {
            JobRunnable runnable = jobItemMap.get(id);
            if(runnable != null) {
                JobQueueItem jobQueueItem = runnable.getJob();
                if(jobQueueItem != null && jobQueueItem.getStatus() != null) {
                    jobQueueItem.getStatus().setState(JobStatus.State.CANCELED);
                }
            }
        }
        jobItemMap.clear();
        canceledJobs.clear();
        inFlightUIDs.clear();
        onPersistAllJobsCanceled();
        if(externalEventListener != null) {
            externalEventListener.onAllJobsCanceled();
        }
    }

    @Override
    public final JobStatus getStatus(long jobId) {
        JobRunnable runnable = jobItemMap.get(jobId);
        if(runnable != null) {
            JobQueueItem jobQueueItem = runnable.getJob();
            if(jobQueueItem != null) {
                return jobQueueItem.getStatus();
            }
            else {
                return null;
            }
        }
        else {
            return null;
        }
    }

    @Override
    public final void shutdown() {
        shutdown(true);
    }

    @Override
    public final void shutdown(boolean keepPersisted) {
        // TODO: after shutdown all public calls should throw IllegalStateException
        executor.shutdownNow();
        frozenJobExecutor.shutdownNow();

        if(networkStatusReceiver != null) {
            try {
                context.unregisterReceiver(networkStatusReceiver);
            } catch(Exception ignore) {}
        }

        if(externalEventListener != null) {
            externalEventListener.onShutdown(keepPersisted);
        }
    }

    /**
     * Should only be called by subclasses of {@link BasicJobQueue} from {@link BasicJobQueue#onLoadPersistedData()}.
     * Adds a {@link java.util.List} of persisted {@code Job}s to the queue.
     * @param jobs the {@code List} of loaded persisted {@code Job}s that need to be added to the queue.
     */
    protected final void addPersistedJobs(List<JobQueueItem> jobs) {
        if(jobs != null) {
            for(JobQueueItem job : jobs) {
                if(job != null && job.getStatus() != null && job.getJob() != null) {
                    if(job.getJob().areMultipleInstancesAllowed() || !inFlightUIDs.contains(job.getJob().getUID())) {
                        JobRunnable runnable = new JobRunnable(job);
                        internalAddToQueueOrColdStorage(runnable);
                        onJobModified(job);
                        inFlightUIDs.add(job.getJob().getUID());
                        jobItemMap.put(job.getJobId(), runnable);
                    }
                }
            }
        }

        if(externalEventListener != null) {
            externalEventListener.onStarted();
        }
    }

    /**
     * Should only be called by subclasses of {@link BasicJobQueue} from {@link BasicJobQueue#onLoadPersistedData()}.
     * Adds a {@link java.util.Map} of persisted groups to this {@code BasicJobQueue}.
     * @param groupMap the {@code Map} of loaded persisted groups
     */
    protected final void addPersistedGroups(Map<String, LinkedList<JobQueueItem>> groupMap) {
        if(groupMap != null) {
            Set<String> groupNames = groupMap.keySet();
            for(String groupName : groupNames) {
                LinkedList<JobQueueItem> groupQueue = groupMap.get(groupName);
                if(groupQueue != null && !groupQueue.isEmpty()) {
                    if(this.groupMap.containsKey(groupName)) {
                        LinkedList<JobQueueItem> currentGroupQueue = this.groupMap.get(groupName);
                        if(currentGroupQueue != null) {
                            for(JobQueueItem job : currentGroupQueue) {
                                job.setGroupIndex(job.getGroupIndex() + groupQueue.size());
                            }
                            groupQueue.addAll(currentGroupQueue);
                        }
                    }
                    this.groupIndexMap.put(groupName, groupQueue.size());
                    this.groupMap.put(groupName, groupQueue);
                }
            }
        }

        isGroupsReady.set(true);
    }

    /**
     * Called when this {@link BasicJobQueue} is created.
     * @see BasicJobQueue#addPersistedJobs(java.util.List)
     *
     */
    @SuppressWarnings("unused")
    protected void onLoadPersistedData() {
        addPersistedJobs(null);
        addPersistedGroups(null);
    }

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

    private void onJobAdded(JobQueueItem job) {
        if(externalEventListener != null) {
            externalEventListener.onJobAdded(job.getStatus());
        }
        if(job.getJob().isPersistent()) {
            onPersistJobAdded(job);
        }
    }

    private void onJobRemoved(JobQueueItem job) {
        inFlightUIDs.remove(job.getJob().getUID());
        if(externalEventListener != null) {
            externalEventListener.onJobRemoved(job.getStatus());
        }
        if(job.getJob().isPersistent()) {
            onPersistJobRemoved(job);
        }
    }

    private void onJobModified(JobQueueItem job) {
        if(externalEventListener != null) {
            externalEventListener.onJobModified(job.getStatus());
        }
        if(job.getJob().isPersistent()) {
            onPersistJobModified(job);
        }
    }

    protected final class JobQueueItem {
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

            String group = status.getJob().getGroup();
            if(group != null) {
                /**
                 * WARNING: this gets called on the main thread, but the ops in the synchronized block should be
                 * small enough that it's not a big deal.
                 */
                synchronized(groupIndexMapLock) {
                    Integer index = groupIndexMap.get(group);
                    LinkedList<JobQueueItem> groupQueue = groupMap.get(group);
                    if(groupQueue == null) {
                        groupQueue = new LinkedList<>();
                        groupMap.put(group, groupQueue);
                    }
                    if(index == null || groupQueue.isEmpty()) {
                        index = 0;
                    }
                    groupIndexMap.put(group, index + 1);
                    this.groupIndex = index;
                    groupQueue.addLast(this);
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

        /**
         * This should only be called when we load persisted group data
         * @param groupIndex the newly calculated group index
         */
        private void setGroupIndex(int groupIndex) {
            this.groupIndex = groupIndex;
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

    private static class JobComparator implements Comparator<JobRunnable> {
        @Override
        public int compare(JobRunnable lhs, JobRunnable rhs) {
            // we shouldn't have to check JobWrappers for null because PriorityBlockingQueues don't allow null
            JobQueueItem lhsJob = lhs.getJob();
            JobQueueItem rhsJob = rhs.getJob();
            if(lhsJob == null || lhsJob.getJob() == null) {
                if(rhsJob == null || rhsJob.getJob() == null) {
                    return 0;
                }
                else {
                    return -1;
                }
            }
            else if(rhsJob == null || rhsJob.getJob() == null) {
                return 1;
            }
            else {
                if(lhsJob.isGroupMember() && rhsJob.isGroupMember()) {
                    if(lhsJob.getGroup().equals(rhsJob.getGroup())) {
                        return rhsJob.getGroupIndex() - lhs.getJob().getGroupIndex();
                    }
                }

                return lhsJob.getJob().getPriority() - rhsJob.getJob().getPriority();
            }
        }
    }

    private class JobRunnable implements Runnable {
        private JobQueueItem job;

        public JobRunnable(JobQueueItem job) {
            this.job = job;
        }

        public JobQueueItem getJob() {
            return job;
        }

        @Override
        public void run() {
            // if this specific job was canceled ignore it
            if(canceledJobs.contains(job.getJobId())) {
                job.getStatus().setState(JobStatus.State.CANCELED);
                try {
                    job.getJob().onCanceled();
                } catch(Throwable ignore) {}
                canceledJobs.remove(job.getJobId());
                onJobRemoved(job);
                return;
            }

            // ensure that the job can reach its required networks if it has any
            if(job.getJob().requiresNetwork()) {
                try {
                    if(!isConnectedToNetwork.get() || !job.getJob().canReachRequiredNetwork()) {
                        job.incrementNetworkRetryCount();
                        job.setValidAtTime(System.currentTimeMillis() + job.getNetworkRetryBackoffMillis());
                        internalAddToQueueOrColdStorage(this);
                        onJobModified(job);
                        return;
                    }
                } catch(Throwable ignore) {}
            }

            // if this job is part of a group, ensure that it is the next job
            // from that group that should be run
            if(job.isGroupMember()) {
                LinkedList<JobQueueItem> groupQueue = groupMap.get(job.getGroup());
                // it should never be null
                if(groupQueue != null) {
                    if(!isGroupsReady.get() || !job.equals(groupQueue.peekFirst())) {
                        job.incrementGroupRetryCount();
                        job.setValidAtTime(System.currentTimeMillis() + job.getGroupRetryBackoffMillis());
                        internalAddToQueueOrColdStorage(this);
                        onJobModified(job);
                        return;
                    }
                }
            }

            boolean retry = false;
            do {
                try {
                    job.getStatus().setState(JobStatus.State.ACTIVE);
                    job.getJob().performJob();
                    if(job.getJob() instanceof Waitable) {
                        ((Waitable) job.getJob()).await();
                    }
                    onJobRemoved(job);
                    if(job.isGroupMember()) {
                        popGroupQueue();
                    }
                    job.getStatus().setState(JobStatus.State.FINISHED);
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
                            onJobRemoved(job);
                            if(job.isGroupMember()) {
                                popGroupQueue();
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
                                internalAddToQueueOrColdStorage(this);
                            }
                            onJobModified(job);
                        }
                    }
                    else {
                        onJobRemoved(job);
                        if(job.isGroupMember()) {
                            popGroupQueue();
                        }
                    }
                }
            } while(retry);
        }

        private void popGroupQueue() {
            if(job.isGroupMember()) {
                LinkedList<JobQueueItem> groupQueue = groupMap.get(job.getGroup());
                // it should never be null
                if(groupQueue != null) {
                    // since we're the head of the queue
                    // pop the queue so the next job in the group can run
                    groupQueue.removeFirst();
                }
            }
        }
    }

    private class FrozenJobRunnable implements Runnable {
        private JobRunnable jobRunnable;

        public FrozenJobRunnable(JobRunnable jobRunnable) {
            this.jobRunnable = jobRunnable;
        }

        @Override
        public void run() {
            if(jobRunnable != null) {
                internalAddToQueueOrColdStorage(jobRunnable);
            }
        }
    }

    private void initNetworkConnectionChecker() {
        networkStatusReceiver = new BroadcastReceiver() {
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
        };
        context.getApplicationContext().registerReceiver(networkStatusReceiver,
                new IntentFilter(ConnectivityManager.CONNECTIVITY_ACTION));
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
