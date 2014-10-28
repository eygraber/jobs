package com.staticbloc.jobs;

public interface Waitable {
    /**
     * Increments the amount of times {@link Waitable#notifyAsyncTaskDone()}
     * has to be called before {@link Waitable#waitForAsyncTasks()} will stop blocking.
     * @return the amount of registered async tasks including this one
     * @see Waitable#notifyAsyncTaskDone()
     * @see Waitable#waitForAsyncTasks()
     */
    public int registerAsyncTask();

    /**
     * Returns the amount of outstanding async tasks to wait for.
     */
    public int asyncTaskCount();

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
