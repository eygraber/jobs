package com.staticbloc.jobs;

public class JobInitializer {
    public final static int DEFAULT_PRIORITY = 0;
    public final static boolean DEFAULT_REQUIRES_NETWORK = false;
    public final static boolean DEFAULT_IS_PERSISTENT = false;
    public final static String DEFAULT_GROUP = null;
    public final static int DEFAULT_RETRY_LIMIT = 10;
    public final static BackoffPolicy DEFAULT_BACKOFF_POLICY = new BackoffPolicy.None();

    private int priority = DEFAULT_PRIORITY;
    private boolean requiresNetwork = DEFAULT_REQUIRES_NETWORK;
    private boolean isPersistent = DEFAULT_IS_PERSISTENT;
    private String group = DEFAULT_GROUP;
    private int retryLimit = DEFAULT_RETRY_LIMIT;
    private BackoffPolicy backoffPolicy = DEFAULT_BACKOFF_POLICY;

    public JobInitializer() {}

    public JobInitializer priority(int priority) {
        this.priority = priority;
        return this;
    }

    public JobInitializer requiresNetwork(boolean requiresNetwork) {
        this.requiresNetwork = requiresNetwork;
        return this;
    }

    public JobInitializer isPersistent(boolean isPersistent) {
        this.isPersistent = isPersistent;
        return this;
    }

    public JobInitializer group(String group) {
        this.group = group;
        return this;
    }

    public JobInitializer retryLimit(int retryLimit) {
        this.retryLimit = retryLimit;
        return this;
    }

    public JobInitializer backoffPolicy(BackoffPolicy backoffPolicy) {
        this.backoffPolicy = backoffPolicy;
        return this;
    }

    public int getPriority() {
        return priority;
    }

    public boolean requiresNetwork() {
        return requiresNetwork;
    }

    public boolean isPersistent() {
        return isPersistent;
    }

    public String getGroup() {
        return group;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    public BackoffPolicy getBackoffPolicy() {
        return backoffPolicy;
    }
}
