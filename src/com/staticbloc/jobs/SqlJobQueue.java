package com.staticbloc.jobs;

import android.content.Context;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import com.google.gson.Gson;

/**
 * Created with IntelliJ IDEA.
 * User: eygraber
 * Date: 9/12/2014
 * Time: 6:33 PM
 * To change this template use File | Settings | File Templates.
 */
public final class SqlJobQueue extends BasicPersistentJobQueue {
    private Gson gson;

    public SqlJobQueue(Context context) {
        super(context);

        gson = new Gson();
    }

    @Override
    protected void onLoadPersistedJobs() {

    }

    @Override
    protected void onPersistJobAdded(JobQueueItem job) {
        String jobJson = gson.toJson(job);
    }

    @Override
    protected void onPersistJobRemoved(JobQueueItem job) {
        String jobJson = gson.toJson(job);
    }

    @Override
    protected void onPersistAllJobsCanceled() {

    }

    @Override
    protected void onPersistJobModified(JobQueueItem job) {
        String jobJson = gson.toJson(job);
    }

    private static class DbHelper extends SQLiteOpenHelper {
        private static final int DB_VERSION = 1;

        private static final String KEY_ROW_ID = "_id";
        private static final String KEY_JOB_ID = "job_id";
        private static final String KEY_JOB_JSON = "job_json";
        private static final String CREATE_TABLE_FOR_QUEUE = "CREATE TABLE IF NOT EXISTS %s (" +
                                                                KEY_ROW_ID + " INTEGER PRIMARY KEY AUTOINCREMENT, " +
                                                                KEY_JOB_ID + " TEXT NOT NULL UNIQUE, " +
                                                                KEY_JOB_JSON + " TEXT NOT NULL);";

        /*package*/ static final int COLUMN_COUNT = 9;

        public DbHelper(Context context, String name) {
            super(context, name, null, DB_VERSION);
        }

        @Override
        public void onCreate(SQLiteDatabase sqLiteDatabase) {
            // we create tables on a per-queue basis lazily
        }

        private void createTableForQueue(String name) {
            getWritableDatabase().execSQL(String.format(CREATE_TABLE_FOR_QUEUE, DatabaseUtils.sqlEscapeString(name)));
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

        }
    }
}
