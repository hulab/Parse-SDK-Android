package com.parse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import bolts.Continuation;
import bolts.Task;
import io.realm.Realm;
import io.realm.RealmConfiguration;

/**
 * Created by maxime on 10/01/2017.
 */

public class RealmStore {

    private RealmConfiguration config;
    protected final TaskQueue taskQueue = new TaskQueue();

    RealmStore() {
        config = new RealmConfiguration.Builder().build();
    }

    RealmStore(RealmConfiguration configuration) {
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
                                        Class<? extends ParseRealmObject> clazz = RealmSchemaController.getSubclass(obj.getClassName());
                                        ParseRealmObject realmObject = realm.createObject(clazz, obj.getObjectId());
                                        realmObject.deleteFromRealm();
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

    /* package */ static class RealmSchemaController {
        private static final Object mutex = new Object();
        private static final Map<String, Class<? extends ParseRealmObject>> registeredSubclasses = new HashMap<>();

        private static String getClassName(Class<? extends ParseRealmObject> clazz) {
            ParseClassName info = clazz.getAnnotation(ParseClassName.class);
            if (info == null) {
                throw new IllegalArgumentException("No ParseClassName annotation provided on " + clazz);
            }
            return info.value();
        }

        /* package */ static void registerSubclass(Class<? extends ParseRealmObject> clazz) {
            if (!ParseObject.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException("Cannot register a type that is not a subclass of ParseObject");
            }

            String className = getClassName(clazz);
            Class<? extends ParseRealmObject> previousClass;

            synchronized (mutex) {
                previousClass = registeredSubclasses.get(className);
                if (previousClass != null) {
                    if (clazz.isAssignableFrom(previousClass)) {
                        // Previous subclass is more specific or equal to the current type, do nothing.
                        return;
                    } else if (previousClass.isAssignableFrom(clazz)) {
                        // Previous subclass is parent of new child subclass, fallthrough and actually
                        // register this class.
                        /* Do nothing */
                    } else {
                        throw new IllegalArgumentException(
                                "Tried to register both " + previousClass.getName() + " and " + clazz.getName() +
                                        " as the ParseObject subclass of " + className + ". " + "Cannot determine the right " +
                                        "class to use because neither inherits from the other."
                        );
                    }
                }

                registeredSubclasses.put(className, clazz);
            }
        }

        /* package */ static void unregisterSubclass(Class<? extends ParseRealmObject> clazz) {
            String className = getClassName(clazz);

            synchronized (mutex) {
                registeredSubclasses.remove(className);
            }
        }

        /* package */ static Class<? extends ParseRealmObject> getSubclass(String className) {
            Class<? extends ParseRealmObject> clazz;

            synchronized (mutex) {
                clazz = registeredSubclasses.get(className);
                if (clazz != null) {
                    return clazz;
                }
                return ParseRealmObject.class;
            }
        }
    }

}
