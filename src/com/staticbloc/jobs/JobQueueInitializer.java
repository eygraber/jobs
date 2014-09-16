package com.staticbloc.jobs;

public class JobQueueInitializer {
    public final static int INFINITE = Integer.MAX_VALUE;
    public final static int DEFAULT_MIN_LIVE_CONSUMERS = 1;
    public final static int DEFAULT_MAX_LIVE_CONSUMERS = INFINITE;
    public final static int DEFAULT_CONSUMER_KEEP_ALIVE = INFINITE;

    private String name;
    private int minLiveConsumers = DEFAULT_MIN_LIVE_CONSUMERS;
    private int maxLiveConsumers = DEFAULT_MAX_LIVE_CONSUMERS;
    private int consumerKeepAliveSeconds = DEFAULT_CONSUMER_KEEP_ALIVE;

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

    public String getName() {
        return name;
    }

    public int getMinLiveConsumers() {
        return minLiveConsumers;
    }

    public int getMaxLiveConsumers() {
        return maxLiveConsumers;
    }

    public int getConsumerKeepAliveSeconds() {
        return consumerKeepAliveSeconds;
    }

    public JobQueueEventListener getJobQueueEventListener() {
        return jobQueueEventListener;
    }
}
