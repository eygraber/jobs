package com.staticbloc.jobs;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class BasicJobQueue implements JobQueue {
    private static final Set<String> activeQueues = Collections.synchronizedSet(new HashSet<String>());

    private final String name;

    private final PriorityBlockingQueue<JobRunnable> queue;
    private final JobExecutor executor;
    private final ScheduledExecutorService frozenJobExecutor;

    private final Map<String, LinkedList<JobQueueItem>> groupMap;
    private final Map<Job, JobRunnable> jobItemMap;
    private final Map<String, Integer> groupIndexMap;
    private final Set<Job> canceledJobs;
    private final Set<String> inFlightUIDs;
    private final LinkedList<JobQueueItem> queueNotReadyList;

    private final Object groupIndexMapLock = new Object();

    private final AtomicBoolean isQueueReady;
    private final AtomicBoolean isConnectedToNetwork;

    private boolean isShutdown;

    private final JobQueueEventListener externalEventListener;

    private final Context context;
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

    /**
     * Creates a new {@code BasicJobQueue}.
     * @param context a {@link android.content.Context} (will be stored as {@code context.getApplicationContext})
     * @param initializer a {@link JobQueueInitializer} containing initialization parameters
     *                    for this {@code BasicJobQueue}
     *
     * @throws java.lang.IllegalArgumentException if {@code initializer.getName() == null}
     * @throws java.lang.IllegalStateException if a {@code BasicJobQueue} with this name is currently active
     */
    public BasicJobQueue(Context context, JobQueueInitializer initializer) {
        if(initializer.getName() == null) {
            throw new IllegalArgumentException("JobQueueInitializer must provide a non-null name");
        }
        else {
            synchronized (activeQueues) {
                if(activeQueues.contains(initializer.getName())) {
                    throw new IllegalStateException(String.format("There is already an active queue with name %s",
                                                                    initializer.getName()));
                }
                else {
                    activeQueues.add(initializer.getName());
                }
            }
        }
        this.name = initializer.getName();

        if(context == null || context.getApplicationContext() == null) {
            throw new IllegalArgumentException(("Context must not be null"));
        }
        this.context = context.getApplicationContext();

        queue = new PriorityBlockingQueue<>(15, new JobComparator());
        executor = new JobExecutor(name, initializer.getMinLiveConsumers(), initializer.getMaxLiveConsumers(),
                initializer.getConsumerKeepAliveSeconds(), queue);
        frozenJobExecutor = Executors.newSingleThreadScheduledExecutor();

        externalEventListener = initializer.getJobQueueEventListener();

        groupMap = new HashMap<>();
        jobItemMap = new HashMap<>();
        groupIndexMap = new HashMap<>();
        canceledJobs = new HashSet<>();
        inFlightUIDs = new HashSet<>();
        queueNotReadyList = new LinkedList<>();

        isQueueReady = new AtomicBoolean(false);
        isConnectedToNetwork = new AtomicBoolean(false);

        isShutdown = false;

        initNetworkConnectionChecker();

        onLoadPersistedData();
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public final Job.State add(Job job) {
        return add(job, 0);
    }

    @Override
    public final Job.State add(Job job, long delayMillis) {
        if(isShutdown) {
            throw new IllegalStateException("Queue is shutdown");
        }
        if(job == null) {
            throw new IllegalArgumentException("Can't pass a null Job");
        }

        if(job.areMultipleInstancesAllowed() || !inFlightUIDs.contains(job.getUID())) {
            JobQueueItem jobQueueItem = new JobQueueItem(job, System.currentTimeMillis() + delayMillis);
            add(jobQueueItem);
            return jobQueueItem.getState();
        }
        else {
            return Job.State.IDENTICAL_JOB_REJECTED;
        }
    }

    private Job.State add(JobQueueItem job) {
        if(!isQueueReady.get()) {
            synchronized(isQueueReady) {
                if(!isQueueReady.get()) {
                    job.setState(Job.State.QUEUE_NOT_READY);
                    queueNotReadyList.addLast(job);
                    return job.getState();
                }
            }
        }

        String group = job.getGroup();
        if(group != null) {
            synchronized(groupIndexMapLock) {
                Integer groupIndex = groupIndexMap.get(group);
                LinkedList<JobQueueItem> groupQueue = groupMap.get(group);
                if(groupQueue == null) {
                    groupQueue = new LinkedList<>();
                    groupMap.put(group, groupQueue);
                }
                if(groupIndex == null || groupQueue.isEmpty()) {
                    groupIndex = 0;
                }
                groupIndexMap.put(group, groupIndex + 1);
                job.setGroupIndex(groupIndex);
                groupQueue.addLast(job);
            }
        }
        JobRunnable runnable = new JobRunnable(job);
        long delayMillis = job.getValidAtTime() - System.currentTimeMillis();
        if(delayMillis <= 0) {
            job.setState(Job.State.ADDED);
            queue.add(runnable);
        }
        else {
            job.setState(Job.State.COLD_STORAGE);
            frozenJobExecutor.schedule(new FrozenJobRunnable(runnable), delayMillis, TimeUnit.MILLISECONDS);
        }
        onJobAdded(job);
        inFlightUIDs.add(job.getJob().getUID());
        jobItemMap.put(job.getJob(), runnable);
        return job.getState();
    }

    private void internalAddToQueueOrColdStorage(JobRunnable jobRunnable) {
        JobQueueItem job = jobRunnable.getJob();
        if(job != null) {
            if(job.getValidAtTime() <= System.currentTimeMillis()) {
                job.setState(Job.State.QUEUED);
                queue.add(jobRunnable);
            }
            else {
                job.setState(Job.State.COLD_STORAGE);
                frozenJobExecutor.schedule(new FrozenJobRunnable(jobRunnable),
                        job.getValidAtTime() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    @Override
    public final void cancel(Job job) {
        if(isShutdown) {
            throw new IllegalStateException("Queue is shutdown");
        }
        JobRunnable runnable = jobItemMap.get(job);
        if(runnable != null) {
            JobQueueItem jobQueueItem = runnable.getJob();
            if(executor.remove(runnable)) {
                if(job != null) {
                    try {
                        job.onCanceled();
                    } catch (Throwable ignore) {}
                    onJobRemoved(jobQueueItem);
                }
            }
            else {
                if(job != null) {
                    canceledJobs.add(job);
                }
            }
        }
    }

    @Override
    public final void cancelAll() {
        if(isShutdown) {
            throw new IllegalStateException("Queue is shutdown");
        }
        queue.clear();
        ArrayList<Job> jobStatusKeys = new ArrayList<>(jobItemMap.keySet());
        for (Job job : jobStatusKeys) {
            JobRunnable runnable = jobItemMap.get(job);
            if(runnable != null) {
                JobQueueItem jobQueueItem = runnable.getJob();
                if(jobQueueItem != null) {
                    jobQueueItem.setState(Job.State.CANCELED);
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
    public final Job.State getStatus(Job job) {
        if(isShutdown) {
            throw new IllegalStateException("Queue is shutdown");
        }
        JobRunnable runnable = jobItemMap.get(job);
        if(runnable != null) {
            JobQueueItem jobQueueItem = runnable.getJob();
            if(jobQueueItem != null) {
                return jobQueueItem.getState();
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
        if(isShutdown) {
            throw new IllegalStateException("Queue is shutdown");
        }

        isShutdown = true;

        executor.shutdownNow();
        frozenJobExecutor.shutdownNow();

        if(!keepPersisted) {
            onPersistAllJobsCanceled();
        }

        activeQueues.remove(getName());

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
                if(job != null && job.getJob() != null) {
                    if(job.getJob().areMultipleInstancesAllowed() || !inFlightUIDs.contains(job.getJob().getUID())) {
                        JobRunnable runnable = new JobRunnable(job);
                        internalAddToQueueOrColdStorage(runnable);
                        onJobModified(job);
                        inFlightUIDs.add(job.getJob().getUID());
                        jobItemMap.put(job.getJob(), runnable);
                        if(job.isGroupMember()) {
                            LinkedList<JobQueueItem> groupQueue = groupMap.get(job.getGroup());
                            if(groupQueue == null) {
                                groupQueue = new LinkedList<>();
                            }
                            groupQueue.addLast(job);
                            groupMap.put(job.getGroup(), groupQueue);
                        }
                    }
                }
            }

            Set<String> groupKeys = groupMap.keySet();
            for(String group: groupKeys) {
                LinkedList<JobQueueItem> groupQueue = groupMap.get(group);
                if(groupQueue != null) {
                    Collections.sort(groupQueue, new Comparator<JobQueueItem>() {
                        @Override
                        public int compare(JobQueueItem lhs, JobQueueItem rhs) {
                            if(lhs == null) {
                                if(rhs == null) {
                                    return 0;
                                }
                                else {
                                    return -1;
                                }
                            }
                            else if(rhs == null) {
                                return 1;
                            }
                            else {
                                return lhs.getGroupIndex() - rhs.getGroupIndex();
                            }
                        }
                    });
                    groupMap.put(group, groupQueue);
                    groupIndexMap.put(group, groupQueue.size());
                }
            }
        }

        synchronized(isQueueReady) {
            for(JobQueueItem job : queueNotReadyList) {
                if(job.getJob().areMultipleInstancesAllowed() || !inFlightUIDs.contains(job.getJob().getUID())) {
                    add(job);
                }
            }
            isQueueReady.set(true);
        }

        if(externalEventListener != null) {
            if(jobs == null) {
                externalEventListener.onStarted(Collections.unmodifiableList(new ArrayList<Job>(0)));
            }
            else {
                List<Job> loadedPersistedJobs = new ArrayList<>(jobs.size());
                for(JobQueueItem job : jobs) {
                    loadedPersistedJobs.add(job.getJob());
                }
                externalEventListener.onStarted(Collections.unmodifiableList(loadedPersistedJobs));
            }
        }
    }

    /**
     * Called when this {@link BasicJobQueue} is created.
     * @see BasicJobQueue#addPersistedJobs(java.util.List)
     *
     */
    @SuppressWarnings("unused")
    protected void onLoadPersistedData() {
        addPersistedJobs(null);
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
     * @see JobQueue#cancel(Job)
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
            externalEventListener.onJobAdded(job.getJob());
        }
        if(job.getJob().isPersistent()) {
            onPersistJobAdded(job);
        }
    }

    private void onJobRemoved(JobQueueItem job) {
        inFlightUIDs.remove(job.getJob().getUID());
        if(externalEventListener != null) {
            externalEventListener.onJobRemoved(job.getJob());
        }
        if(job.getJob().isPersistent()) {
            onPersistJobRemoved(job);
        }
    }

    private void onJobModified(JobQueueItem job) {
        if(externalEventListener != null) {
            externalEventListener.onJobModified(job.getJob());
        }
        if(job.getJob().isPersistent()) {
            onPersistJobModified(job);
        }
    }

    protected final class JobQueueItem {
        private long validAtTime;
        private Job job;
        private Job.State state = Job.State.NOTHING;
        private int groupIndex = -1;

        private int networkRetryCount = 0;
        private BackoffPolicy networkBackoffPolicy = new BackoffPolicy.Step(1000, 3000);
        private int groupRetryCount = 0;
        private BackoffPolicy groupBackoffPolicy = new BackoffPolicy.Linear(1000);

        public JobQueueItem(Job job, long validAtTime) {
            if(job == null) {
                throw new IllegalArgumentException("Job cannot be null");
            }
            this.validAtTime = validAtTime;
        }

        public Job getJob() {
            return job;
        }

        public Job.State getState() {
            return state;
        }

        public void setState(Job.State state) {
            this.state = state;
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
            return groupIndex >= 0 && job.getGroup() != null;
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
            return job.getGroup();
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
            if(canceledJobs.contains(job.getJob())) {
                job.setState(Job.State.CANCELED);
                try {
                    job.getJob().onCanceled();
                } catch(Throwable ignore) {}
                canceledJobs.remove(job.getJob());
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
                    if(job != groupQueue.peekFirst()) {
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
                    job.setState(Job.State.ACTIVE);
                    job.getJob().performJob();
                    if(job.getJob() instanceof Waitable) {
                        ((Waitable) job.getJob()).await();
                    }
                    onJobRemoved(job);
                    if(job.isGroupMember()) {
                        popGroupQueue();
                    }
                    job.setState(Job.State.FINISHED);
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
