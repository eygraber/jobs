package com.staticbloc.jobs;

public interface Waitable {
    /**
     * Returns the amount of times {@link Waitable#notifyAsyncTaskDone()}
     * has to be called before {@link Waitable#waitForAsyncTasks()} will stop blocking.
     * @see Waitable#notifyAsyncTaskDone()
     * @see Waitable#waitForAsyncTasks()
     */
    public int getInitialLockCount();

    /**
     * The invoking {@link java.lang.Thread} will wait until all locks are unlocked.
     * @throws InterruptedException if the {@code Thread} is interrupted while waiting
     * @see Waitable#notifyAsyncTaskDone() ()
     */
    public void waitForAsyncTasks() throws Throwable;

    /**
     * Unlocks one of the locked locks.
     */
    public void notifyAsyncTaskDone();

    /**
     * Causes {@link Waitable#waitForAsyncTasks()} to stop waiting for any remaining async tasks,
     * and throws the provided {@link java.lang.Throwable}.
     * <b>Any other async tasks will continue to execute.</b>
     * @param t the {@code Throwable} to be thrown
     */
    public void raiseThrowableFromAsyncTask(Throwable t);
}
