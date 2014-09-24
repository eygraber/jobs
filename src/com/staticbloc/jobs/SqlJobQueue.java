package com.staticbloc.jobs;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;
import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class SqlJobQueue extends BasicPersistentJobQueue {
    private final DbHelper db;
    private final Gson gson;

    public SqlJobQueue(Context context, JobQueueInitializer initializer) {
        this(context, initializer, null);
    }

    public SqlJobQueue(Context context, JobQueueInitializer initializer, Map<Type, TypeAdapter> typeAdapters) {
        super(context, initializer);
        db = DbHelper.getInstance(context.getApplicationContext(), "com.staticbloc.jobs.db");
        if(typeAdapters != null) {
            GsonBuilder builder = new GsonBuilder();
            for(Type t : typeAdapters.keySet()) {
                builder.registerTypeAdapter(t, typeAdapters.get(t));
            }
            gson = builder.create();
        }
        else {
            gson = new Gson();
        }
    }

    @Override
    protected void onLoadPersistedData() {
        if(shouldDebugLog()) {
            Log.d(getName(), String.format("%s about to load persisted data", getName()));
        }
        getPersistenceExecutor().execute(new Runnable() {
            @Override
            public void run() {
                addPersistedJobs(db.getAllJobsForQueue(getName(), gson));
            }
        });
    }

    @Override
    protected void onPersistJobAdded(final JobQueueItem job) {
        if(shouldDebugLog()) {
            Log.d(getName(), String.format("%s persisting added %s", getName(), job.getJob().getClass().getSimpleName()));
        }
        getPersistenceExecutor().execute(new Runnable() {
            @Override
            public void run() {
                db.addJob(getName(), job, gson);
            }
        });
    }

    @Override
    protected void onPersistJobRemoved(final JobQueueItem job) {
        if(shouldDebugLog()) {
            Log.d(getName(), String.format("%s persisting removed %s", getName(), job.getJob().getClass().getSimpleName()));
        }
        getPersistenceExecutor().execute(new Runnable() {
            @Override
            public void run() {
                db.removeJob(getName(), job, shouldDebugLog());
            }
        });
    }

    @Override
    protected void onPersistAllJobsCanceled() {
        if(shouldDebugLog()) {
            Log.d(getName(), String.format("%s persisting removing all Jobs", getName()));
        }
        getPersistenceExecutor().execute(new Runnable() {
            @Override
            public void run() {
                db.removeAllJobs(getName());
            }
        });
    }

    @Override
    protected void onPersistJobModified(final JobQueueItem job) {
        if(shouldDebugLog()) {
            Log.d(getName(), String.format("%s persisting modified %s", getName(), job.getJob().getClass().getSimpleName()));
        }
        getPersistenceExecutor().execute(new Runnable() {
            @Override
            public void run() {
                db.updateJob(getName(), job, shouldDebugLog(), gson);
            }
        });
    }

    public static class TypeAdapter<T> implements JsonSerializer<T>, JsonDeserializer<T> {
        private static final String CLASSNAME = "CLASSNAME";
        private static final String INSTANCE  = "INSTANCE";

        @Override
        public JsonElement serialize(T src, Type typeOfSrc,
                                     JsonSerializationContext context) {

            JsonObject retValue = new JsonObject();
            String className = src.getClass().getCanonicalName();
            retValue.addProperty(CLASSNAME, className);
            JsonElement elem = context.serialize(src);
            retValue.add(INSTANCE, elem);
            return retValue;
        }

        @Override
        public T deserialize(JsonElement json, Type typeOfT,
                                 JsonDeserializationContext context) throws JsonParseException {
            JsonObject jsonObject =  json.getAsJsonObject();
            JsonPrimitive prim = (JsonPrimitive) jsonObject.get(CLASSNAME);
            String className = prim.getAsString();

            Class<?> klass = null;
            try {
                klass = Class.forName(className);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                throw new JsonParseException(e.getMessage());
            }
            return context.deserialize(jsonObject.get(INSTANCE), klass);
        }
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

        private DbHelper(Context context, String name) {
            super(context, name, null, DB_VERSION);
        }

        public List<JobQueueItem> getAllJobsForQueue(String queueName, Gson gson) {
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

        public void addJob(String queueName, JobQueueItem job, Gson gson) {
            queueName = DatabaseUtils.sqlEscapeString(queueName);
            ContentValues values = new ContentValues();
            values.put(KEY_QUEUE_NAME, queueName);
            values.put(KEY_JOB_JSON, DatabaseUtils.sqlEscapeString(gson.toJson(job)));
            long id = getWritableDatabase().insert(JOB_TABLE_NAME, null, values);
            jobToSqlIdMap.put(job, id);
        }

        public void updateJob(String queueName, JobQueueItem job, boolean shouldLog, Gson gson) {
            Long id = jobToSqlIdMap.get(job);
            if(id == null) {
                if(shouldLog) {
                    Log.w(queueName, String.format("Tried to update %s, but SQL id doesn't exist",
                                                    job.getJob().getClass().getSimpleName()));
                }
                return;
            }
            ContentValues values = new ContentValues();
            values.put(KEY_JOB_JSON, DatabaseUtils.sqlEscapeString(gson.toJson(job)));
            getWritableDatabase().update(JOB_TABLE_NAME,
                    values,
                    String.format(JOB_WHERE_CLAUSE, id),
                    null);
        }

        public void removeJob(String queueName, JobQueueItem job, boolean shouldLog) {
            Long id = jobToSqlIdMap.get(job);
            if(id == null) {
                if(shouldLog) {
                    Log.w(queueName, String.format("Tried to remove %s, but SQL id doesn't exist",
                            job.getJob().getClass().getSimpleName()));
                }
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
