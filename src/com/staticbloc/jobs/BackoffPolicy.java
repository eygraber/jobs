package com.staticbloc.jobs;

import java.util.Random;

public interface BackoffPolicy {
    public static final int NO_PLATEAU = -1;

    /**
     * Return the relative amount of milliseconds to backoff.
     * @param backoffCount the amount of times a backoff was needed
     * @return the relative amount of milliseconds to backoff
     */
    public long getNextMillis(int backoffCount);

    /**
     * The {@code plateau} defines how many iterations of the {@code BackoffPolicy} we should attempt.
     * Once {@code backoffCount >= getPlateau()} {@code getNextMillis()} should use {@code getPlateau()}
     * instead of {@code backoffCount} for its calculations.
     * Use {@link com.staticbloc.jobs.BackoffPolicy#NO_PLATEAU} if you don't want there to be a plateau.
     * @return the plateau
     */
    public int getPlateau();

    /**
     * A convenience base class for {@link com.staticbloc.jobs.BackoffPolicy} implementations.
     * {@code getNextMillis()} always returns 0.
     */
    public static class None implements BackoffPolicy {
        @Override
        public long getNextMillis(int backoffCount) {
            return 0;
        }

        @Override
        public int getPlateau() {
            return NO_PLATEAU;
        }
    }

    /**
     * A convenience base class for {@link com.staticbloc.jobs.BackoffPolicy} implementations.
     */
    public static abstract class Base implements BackoffPolicy {
        private long baseWaitTimeMillis;
        private int plateau;

        public Base(long baseWaitTimeMillis) {
            this.baseWaitTimeMillis = baseWaitTimeMillis;
            plateau = NO_PLATEAU;
        }

        public Base(long baseWaitTimeMillis, int plateau) {
            this.baseWaitTimeMillis = baseWaitTimeMillis;
            this.plateau = plateau;
        }

        protected long getBaseWaitTimeMillis() {
            return baseWaitTimeMillis;
        }

        public int getPlateau() {
            return plateau;
        }
    }

    /**
     * A {@link com.staticbloc.jobs.BackoffPolicy} that backsoff linearly.
     * The algorithm is {@code getBaseWaitTimeMillis() * backoffCount}.
     */
    public static class Linear extends Base {
        public Linear(long baseWaitTimeMillis) {
            this(baseWaitTimeMillis, NO_PLATEAU);
        }

        public Linear(long baseWaitTimeMillis, int plateau) {
            super(baseWaitTimeMillis, plateau);
        }

        @Override
        public long getNextMillis(int backoffCount) {
            if(backoffCount >= getPlateau()) {
                return getBaseWaitTimeMillis() * getPlateau();
            }
            else {
                return getBaseWaitTimeMillis() * backoffCount;
            }
        }
    }

    /**
     * A {@link com.staticbloc.jobs.BackoffPolicy} that backsoff linearly using a step.
     * The algorithm is {@code getBaseWaitTimeMillis() + (step * backoffCount)}.
     */
    public static class Step extends Base {
        long step;

        public Step(long baseWaitTimeMillis, long step) {
            this(baseWaitTimeMillis, step, NO_PLATEAU);
        }

        public Step(long baseWaitTimeMillis, long step, int plateau) {
            super(baseWaitTimeMillis, plateau);
            this.step = step;
        }

        @Override
        public long getNextMillis(int backoffCount) {
            if(backoffCount >= getPlateau()) {
                return getBaseWaitTimeMillis() + (step * getPlateau());
            }
            else {
                return getBaseWaitTimeMillis() + (step * backoffCount);
            }
        }
    }

    /**
     * A {@link com.staticbloc.jobs.BackoffPolicy} that backsoff exponentially.
     * The algorithm is {@code (Math.pow(getBaseWaitTimeMillis(), backoffCount)) + rand(0, 1000)}.
     */
    public static class Exponential extends Base {
        private Random r = new Random();

        public Exponential(int baseWaitTimeMillis) {
            this(baseWaitTimeMillis, NO_PLATEAU);
        }

        public Exponential(long baseWaitTimeMillis, int plateau) {
            super(baseWaitTimeMillis, plateau);
            r.setSeed(System.currentTimeMillis());
        }

        @Override
        public long getNextMillis(int backoffCount) {
            if(backoffCount >= getPlateau()) {
                return ((long) Math.pow(getBaseWaitTimeMillis(), getPlateau())) + r.nextInt(1000);
            }
            else {
                return ((long) Math.pow(getBaseWaitTimeMillis(), backoffCount)) + r.nextInt(1000);
            }
        }
    }
}
