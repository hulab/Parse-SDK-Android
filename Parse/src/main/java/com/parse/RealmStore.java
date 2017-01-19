package com.parse;

import java.util.ArrayList;

import bolts.Continuation;
import bolts.Task;
import io.realm.Realm;
import io.realm.RealmConfiguration;
import io.realm.RealmObject;

/**
 * Created by maxime on 10/01/2017.
 */

public class RealmStore {

    private RealmConfiguration config;
    protected final TaskQueue taskQueue = new TaskQueue();

    public RealmStore() {
        config = new RealmConfiguration.Builder().build();
    }

    public RealmStore(RealmConfiguration configuration) {
        config = configuration;
    }

    public RealmConfiguration getConfiguration() {
        return config;
    }

    public <T extends ParseObject> Task<Boolean> save(T object) {
        ArrayList<ParseObject> objects = new ArrayList<>();
        objects.add(object);
        return save(objects);
    }

    public <T extends ParseObject> Task<Boolean> save(ArrayList<T> objects) {

        final ArrayList<T> toSave = new ArrayList<>(objects);
        return taskQueue.enqueue(new Continuation<Void, Task<Boolean>>() {
            @Override
            public Task<Boolean> then(Task<Void> task) throws Exception {

            try {
                final Realm realm =  Realm.getInstance(config);
                realm.beginTransaction();

                for (ParseObject object : toSave) {

                    new ParseTraverser() {
                        @Override
                        protected boolean visit(Object object) {

                        if (object instanceof ParseObject) {
                            ParseRealmObject.encode(realm, (ParseObject)object);
                        }

                        return true;
                        }
                    }.setYieldRoot(true).setTraverseParseObjects(true).traverse(object);
                }

                realm.commitTransaction();

            } catch (Exception e) {
                return Task.forError(e);
            }
            return Task.forResult(true);
            }
        });
    }

    public <T extends ParseObject> Task<Boolean> remove(T object) {
        ArrayList<ParseObject> objects = new ArrayList<>();
        objects.add(object);
        return remove(objects, new CascadeDeletionCallback() {
            @Override
            public Boolean delete(ParseObject object) {
                return true;
            }
        });
    }

    public <T extends ParseObject> Task<Boolean> remove(ArrayList<T> objects) {
        return remove(objects, new CascadeDeletionCallback() {
            @Override
            public Boolean delete(ParseObject object) {
                return true;
            }
        });
    }

    public <T extends ParseObject> Task<Boolean> remove(T object, final CascadeDeletionCallback cascade) {
        ArrayList<ParseObject> objects = new ArrayList<>();
        objects.add(object);
        return remove(objects, cascade);
    }

    public <T extends ParseObject> Task<Boolean> remove(ArrayList<T> objects, final CascadeDeletionCallback cascade) {
        final ArrayList<T> toDelete = new ArrayList<>(objects);

        return taskQueue.enqueue(new Continuation<Void, Task<Boolean>>() {
            @Override
            public Task<Boolean> then(Task<Void> task) throws Exception {

            try {
                final Realm realm =  Realm.getInstance(config);
                realm.beginTransaction();

                for (ParseObject object : toDelete) {
                    new ParseTraverser() {
                        @Override
                        protected boolean visit(Object object) {

                    if (object instanceof ParseObject) {
                        ParseObject obj = (ParseObject)object;

                        Boolean delete = toDelete.contains(obj);
                        if (!delete) {
                            delete = cascade.delete(obj);
                        }

                        if (delete) {
                            RealmSchemaController.RealmSchema schema = RealmSchemaController.getSchema(obj.getClassName());
                            RealmObject realmObject = realm.where(schema.clazz).equalTo(schema.objectId.getName(), obj.getObjectId()).findFirst();
                            if (realmObject != null) {
                                realmObject.deleteFromRealm();
                            }
                        }
                    }
                    return true;
                        }
                    }.setYieldRoot(true).setTraverseParseObjects(true).traverse(object);
                }


            } catch (Exception e) {
                return Task.forError(e);
            }

            return Task.forResult(true);
            }
        });
    }

    public interface CascadeDeletionCallback {
        Boolean delete(ParseObject object);
    }

    public static void registerRealmSchema(Class<? extends RealmObject> subclass) {
        RealmSchemaController.registerSubclass(subclass);
    }

}
