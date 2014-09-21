package com.staticbloc.jobs;

public class JobQueueInitializer {
    public final static int INFINITE = Integer.MAX_VALUE;
    public final static int DEFAULT_MIN_LIVE_CONSUMERS = 1;
    public final static int DEFAULT_MAX_LIVE_CONSUMERS = INFINITE;
    public final static int DEFAULT_CONSUMER_KEEP_ALIVE = INFINITE;
    public final static boolean SHOULD_DEBUG_LOG_DEFAULT = false;

    private String name;
    private int minLiveConsumers = DEFAULT_MIN_LIVE_CONSUMERS;
    private int maxLiveConsumers = DEFAULT_MAX_LIVE_CONSUMERS;
    private int consumerKeepAliveSeconds = DEFAULT_CONSUMER_KEEP_ALIVE;
    private boolean shouldDebugLog = SHOULD_DEBUG_LOG_DEFAULT;

    private JobQueueEventListener jobQueueEventListener;

    public JobQueueInitializer() {}

    public JobQueueInitializer name(String name) {
        this.name = name;
        return this;
    }

    public JobQueueInitializer minLiveConsumers(int minLiveConsumers) {
        this.minLiveConsumers = minLiveConsumers;
        return this;
    }

    public JobQueueInitializer maxLiveConsumers(int maxLiveConsumers) {
        this.maxLiveConsumers = maxLiveConsumers;
        return this;
    }

    public JobQueueInitializer consumerKeepAliveSeconds(int consumerKeepAliveSeconds) {
        this.consumerKeepAliveSeconds = consumerKeepAliveSeconds;
        return this;
    }

    public JobQueueInitializer jobQueueEventListener(JobQueueEventListener jobQueueEventListener) {
        this.jobQueueEventListener = jobQueueEventListener;
        return this;
    }

    public JobQueueInitializer shouldDebugLog(boolean shouldDebugLog) {
        this.shouldDebugLog = shouldDebugLog;
        return this;
    }

    /*package*/ String getName() {
        return name;
    }

    /*package*/ int getMinLiveConsumers() {
        return minLiveConsumers;
    }

    /*package*/ int getMaxLiveConsumers() {
        return maxLiveConsumers;
    }

    /*package*/ int getConsumerKeepAliveSeconds() {
        return consumerKeepAliveSeconds;
    }

    /*package*/ JobQueueEventListener getJobQueueEventListener() {
        return jobQueueEventListener;
    }

    /*package*/ boolean getShouldDebugLog() {
        return shouldDebugLog;
    }
}
