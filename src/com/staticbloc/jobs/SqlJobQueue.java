package com.staticbloc.jobs;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public final class SqlJobQueue extends BasicPersistentJobQueue {
    private final DbHelper db;

    SqlJobQueue(Context context, JobQueueInitializer initializer) {
        super(context, initializer);
        db = DbHelper.getInstance(context.getApplicationContext(), "com.staticbloc.jobs.db");
    }

    @Override
    protected void onLoadPersistedData() {
        getPersistenceExecutor().execute(new Runnable() {
            @Override
            public void run() {
                addPersistedJobs(db.getAllJobsForQueue(getName()));
                addPersistedGroups(db.getGroupsForQueue(getName()));
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
        private static final String KEY_JOB_ID = "job_id";
        private static final String KEY_JOB_JSON = "job_json";
        private static final String CREATE_JOB_TABLE = "CREATE TABLE IF NOT EXISTS " + JOB_TABLE_NAME + " (" +
                KEY_ROW_ID + " INTEGER PRIMARY KEY AUTOINCREMENT, " +
                KEY_QUEUE_NAME + " TEXT NOT NULL, " +
                KEY_JOB_ID + " INTEGER NOT NULL, " +
                KEY_JOB_JSON + " TEXT NOT NULL);";
        private static final String JOB_WHERE_CLAUSE = KEY_QUEUE_NAME + "='%s' AND " + KEY_JOB_ID + "=%d";
        private static final String ALL_JOBS_IN_QUEUE_WHERE_CLAUSE = KEY_QUEUE_NAME + "='%s'";

        private static final String GROUP_TABLE_NAME = "groups";
        private static final String KEY_GROUP_NAME = "group_name";
        private static final String KEY_GROUP_QUEUE = "group_queue";
        private static final String CREATE_GROUP_TABLE = "CREATE TABLE IF NOT EXISTS " + GROUP_TABLE_NAME + " (" +
                KEY_ROW_ID + " INTEGER PRIMARY KEY AUTOINCREMENT, " +
                KEY_QUEUE_NAME + " TEXT NOT NULL, " +
                KEY_GROUP_NAME + " TEXT NOT NULL UNIQUE, " +
                KEY_GROUP_QUEUE + " TEXT NOT NULL);";
        private static final String ALL_GROUPS_IN_QUEUE_WHERE_CLAUSE = KEY_QUEUE_NAME + "='%s'";

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
                        new String[] {KEY_JOB_JSON},
                        String.format(ALL_JOBS_IN_QUEUE_WHERE_CLAUSE, queueName),
                        null,
                        null,
                        null,
                        null);
                if(c.moveToFirst() && !c.isLast()) {
                    List<JobQueueItem> jobs = new ArrayList<>(c.getCount());
                    do {
                        jobs.add(gson.fromJson(c.getString(c.getColumnIndex(KEY_JOB_JSON)), JobQueueItem.class));
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
            values.put(KEY_JOB_ID, job.getJobId());
            values.put(KEY_JOB_JSON, DatabaseUtils.sqlEscapeString(gson.toJson(job)));
            getWritableDatabase().insert(JOB_TABLE_NAME, null, values);
        }

        public void updateJob(String queueName, JobQueueItem job) {
            queueName = DatabaseUtils.sqlEscapeString(queueName);
            ContentValues values = new ContentValues();
            values.put(KEY_JOB_JSON, DatabaseUtils.sqlEscapeString(gson.toJson(job)));
            getWritableDatabase().update(JOB_TABLE_NAME,
                    values,
                    String.format(JOB_WHERE_CLAUSE, queueName, job.getJobId()),
                    null);
        }

        public void removeJob(String queueName, JobQueueItem job) {
            queueName = DatabaseUtils.sqlEscapeString(queueName);
            getWritableDatabase().delete(JOB_TABLE_NAME,
                    String.format(JOB_WHERE_CLAUSE, queueName, job.getJobId()),
                    null);
        }

        public void removeAllJobs(String queueName) {
            queueName = DatabaseUtils.sqlEscapeString(queueName);
            getWritableDatabase().delete(JOB_TABLE_NAME,
                    String.format(ALL_JOBS_IN_QUEUE_WHERE_CLAUSE, queueName),
                    null);
        }

        @SuppressWarnings("unchecked")
        public Map<String, LinkedList<JobQueueItem>> getGroupsForQueue(String queueName) {
            queueName = DatabaseUtils.sqlEscapeString(queueName);
            Cursor c = null;
            try {
                c = getWritableDatabase().query(GROUP_TABLE_NAME,
                        new String[] {KEY_GROUP_NAME, KEY_GROUP_QUEUE},
                        String.format(ALL_GROUPS_IN_QUEUE_WHERE_CLAUSE, queueName),
                        null,
                        null,
                        null,
                        null);
                if(c.moveToFirst() && !c.isLast()) {
                    Map<String, LinkedList<JobQueueItem>> groups = new HashMap<>();
                    do {
                        String groupName = c.getString(c.getColumnIndex(KEY_GROUP_NAME));
                        LinkedList<JobQueueItem> groupQueue = gson.fromJson(c.getString(c.getColumnIndex(KEY_GROUP_QUEUE)),
                                new TypeToken<LinkedList<JobQueueItem>>(){}.getType());
                        groups.put(groupName, groupQueue);
                    } while(c.moveToNext());
                    return groups;
                }
                else {
                    Log.i("Jobs", "There were no persisted groups to load");
                    return null;
                }
            }
            catch(Exception e) {
                Log.w("Jobs", "There was an exception trying to load persisted groups for queue " + queueName, e);
                return null;
            }
            finally {
                if(c != null && !c.isClosed()) {
                    c.close();
                }
            }
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(CREATE_JOB_TABLE);
            db.execSQL(CREATE_GROUP_TABLE);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            // TODO: drop tables or attempt to migrate?
        }
    }
}
