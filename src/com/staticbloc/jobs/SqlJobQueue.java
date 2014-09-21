package com.staticbloc.jobs;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class SqlJobQueue extends BasicPersistentJobQueue {
    private final DbHelper db;

    public SqlJobQueue(Context context, JobQueueInitializer initializer) {
        super(context, initializer);
        db = DbHelper.getInstance(context.getApplicationContext(), "com.staticbloc.jobs.db");
    }

    @Override
    protected void onLoadPersistedData() {
        getPersistenceExecutor().execute(new Runnable() {
            @Override
            public void run() {
                addPersistedJobs(db.getAllJobsForQueue(getName()));
            }
        });
    }

    @Override
    protected void onPersistJobAdded(final JobQueueItem job) {
        getPersistenceExecutor().execute(new Runnable() {
            @Override
            public void run() {
                db.addJob(getName(), job);
            }
        });
    }

    @Override
    protected void onPersistJobRemoved(final JobQueueItem job) {
        getPersistenceExecutor().execute(new Runnable() {
            @Override
            public void run() {
                db.removeJob(getName(), job);
            }
        });
    }

    @Override
    protected void onPersistAllJobsCanceled() {
        getPersistenceExecutor().execute(new Runnable() {
            @Override
            public void run() {
                db.removeAllJobs(getName());
            }
        });
    }

    @Override
    protected void onPersistJobModified(final JobQueueItem job) {
        getPersistenceExecutor().execute(new Runnable() {
            @Override
            public void run() {
                db.updateJob(getName(), job);
            }
        });
    }

    private static class DbHelper extends SQLiteOpenHelper {
        private static final Map<JobQueueItem, Long> jobToSqlIdMap = new HashMap<>();

        private static volatile DbHelper instance = null;

        public static DbHelper getInstance(Context context, String name) {
            if (instance == null) {
                synchronized (DbHelper.class) {
                    if (instance == null) {
                        instance = new DbHelper(context, name);
                    }
                }
            }
            return instance;
        }
        private static final int DB_VERSION = 1;

        private static final String KEY_ROW_ID = "_id";
        private static final String KEY_QUEUE_NAME = "queue_name";

        private static final String JOB_TABLE_NAME = "jobs";
        private static final String KEY_JOB_JSON = "job_json";
        private static final String CREATE_JOB_TABLE = "CREATE TABLE IF NOT EXISTS " + JOB_TABLE_NAME + " (" +
                KEY_ROW_ID + " INTEGER PRIMARY KEY AUTOINCREMENT, " +
                KEY_QUEUE_NAME + " TEXT NOT NULL, " +
                KEY_JOB_JSON + " TEXT NOT NULL);";
        private static final String JOB_WHERE_CLAUSE = KEY_ROW_ID + "=%d";
        private static final String ALL_JOBS_IN_QUEUE_WHERE_CLAUSE = KEY_QUEUE_NAME + "=%s";

        private Gson gson;


        private DbHelper(Context context, String name) {
            super(context, name, null, DB_VERSION);

            gson = new Gson();
        }

        public List<JobQueueItem> getAllJobsForQueue(String queueName) {
            queueName = DatabaseUtils.sqlEscapeString(queueName);
            Cursor c = null;
            try {
                c = getWritableDatabase().query(JOB_TABLE_NAME,
                        new String[] {KEY_ROW_ID, KEY_JOB_JSON},
                        String.format(ALL_JOBS_IN_QUEUE_WHERE_CLAUSE, queueName),
                        null,
                        null,
                        null,
                        null);
                if(c.moveToFirst() && !c.isLast()) {
                    List<JobQueueItem> jobs = new ArrayList<>(c.getCount());
                    do {
                        JobQueueItem job = gson.fromJson(c.getString(c.getColumnIndex(KEY_JOB_JSON)), JobQueueItem.class);
                        jobs.add(job);
                        jobToSqlIdMap.put(job, c.getLong(c.getColumnIndex(KEY_ROW_ID)));
                    } while(c.moveToNext());
                    return jobs;
                }
                else {
                    Log.i("Jobs", "There were no persisted jobs to load");
                    return null;
                }
            }
            catch(Exception e) {
                Log.w("Jobs", "There was an exception trying to load persisted jobs for queue " + queueName, e);
                return null;
            }
            finally {
                if(c != null && !c.isClosed()) {
                    c.close();
                }
            }
        }

        public void addJob(String queueName, JobQueueItem job) {
            queueName = DatabaseUtils.sqlEscapeString(queueName);
            ContentValues values = new ContentValues();
            values.put(KEY_QUEUE_NAME, queueName);
            values.put(KEY_JOB_JSON, DatabaseUtils.sqlEscapeString(gson.toJson(job)));
            long id = getWritableDatabase().insert(JOB_TABLE_NAME, null, values);
            jobToSqlIdMap.put(job, id);
        }

        public void updateJob(String queueName, JobQueueItem job) {
            Long id = jobToSqlIdMap.get(job);
            if(id == null) {
                Log.w(queueName, "Tried to update job, but SQL id doesn't exist");
                return;
            }
            ContentValues values = new ContentValues();
            values.put(KEY_JOB_JSON, DatabaseUtils.sqlEscapeString(gson.toJson(job)));
            getWritableDatabase().update(JOB_TABLE_NAME,
                    values,
                    String.format(JOB_WHERE_CLAUSE, id),
                    null);
        }

        public void removeJob(String queueName, JobQueueItem job) {
            Long id = jobToSqlIdMap.get(job);
            if(id == null) {
                Log.w(queueName, "Tried to remove job, but SQL id doesn't exist");
                return;
            }
            getWritableDatabase().delete(JOB_TABLE_NAME,
                    String.format(JOB_WHERE_CLAUSE, id),
                    null);
            jobToSqlIdMap.remove(job);
        }

        public void removeAllJobs(String queueName) {
            queueName = DatabaseUtils.sqlEscapeString(queueName);
            getWritableDatabase().delete(JOB_TABLE_NAME,
                    String.format(ALL_JOBS_IN_QUEUE_WHERE_CLAUSE, queueName),
                    null);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(CREATE_JOB_TABLE);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            // TODO: drop tables or attempt to migrate?
        }
    }
}
