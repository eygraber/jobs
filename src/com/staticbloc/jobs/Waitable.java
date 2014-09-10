package com.staticbloc.jobs;

public interface Waitable {
    /**
     * Returns the amount of times {@link Waitable#unlock()} has to be called before
     * {@link Waitable#await()} will stop blocking.
     * @see Waitable#unlock()
     * @see Waitable#await()
     */
    public int getInitialLockCount();

    /**
     * The invoking {@link Thread} will wait until all locks are unlocked.
     * @throws InterruptedException if the {@code Thread} is interrupted while waiting
     * @see Waitable#unlock() ()
     */
    public void await() throws InterruptedException;

    /**
     * Unlocks one of the locked locks.
     */
    public void unlock();
}
