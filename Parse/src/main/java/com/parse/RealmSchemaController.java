package com.parse;

import com.parse.realm.annotations.CreatedAt;
import com.parse.realm.annotations.EncodedData;
import com.parse.realm.annotations.Key;
import com.parse.realm.annotations.ObjectId;
import com.parse.realm.annotations.UpdatedAt;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import io.realm.RealmObject;

/**
 * Created by maxime on 16/01/2017.
 */

class RealmSchemaController {
    private static final Object mutex = new Object();
    private static final Map<String, RealmSchema> registeredSubclasses = new HashMap<>();

    static RealmSchema genericSchema = createSchema(ParseRealmObject.class);

    static class RealmSchema {
        Class<? extends RealmObject> clazz;
        Field objectId;
        Field encodedData;
        Field createAt;
        Field updatedAt;

        Map<String, Field> keys = new HashMap<>();

        RealmSchema(Class<? extends RealmObject> clazz) {
            this.clazz = clazz;
        }
    }

    private static String getClassName(Class<? extends RealmObject> clazz) {
        ParseClassName info = clazz.getAnnotation(ParseClassName.class);
        if (info == null) {
            throw new IllegalArgumentException("No ParseClassName annotation provided on " + clazz);
        }
        return info.value();
    }

    private static RealmSchema createSchema(Class<? extends RealmObject> clazz) {

        RealmSchema schema = new RealmSchema(clazz);

        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {

            if (field.isAnnotationPresent(ObjectId.class)) {
                if (schema.objectId != null) {
                    throw new IllegalArgumentException("ObjectId annotation must be unique");
                }
                if (field.getType() != String.class) {
                    throw new IllegalArgumentException("ObjectId annotation must associated to a String field");
                }
                schema.objectId = field;
                break;
            }

            if (field.isAnnotationPresent(EncodedData.class)) {
                if (schema.encodedData != null) {
                    throw new IllegalArgumentException("EncodedData annotation must be unique");
                }
                if (field.getType() != String.class) {
                    throw new IllegalArgumentException("EncodedData annotation must associated to a String field");
                }
                schema.encodedData = field;
                break;
            }

            if (field.isAnnotationPresent(CreatedAt.class)) {
                if (schema.createAt != null) {
                    throw new IllegalArgumentException("CreatedAt annotation must be unique");
                }
                if (field.getType() != Date.class) {
                    throw new IllegalArgumentException("CreatedAt annotation must associated to a Date field");
                }
                schema.createAt = field;
                break;
            }

            if (field.isAnnotationPresent(UpdatedAt.class)) {
                if (schema.updatedAt != null) {
                    throw new IllegalArgumentException("UpdatedAt annotation must be unique");
                }
                if (field.getType() != Date.class) {
                    throw new IllegalArgumentException("UpdatedAt annotation must associated to a Date field");
                }
                schema.updatedAt = field;
                break;
            }

            if (field.isAnnotationPresent(Key.class)) {
                String key = field.getAnnotation(Key.class).value();

                if (schema.keys.containsKey(key)) {
                    throw new IllegalArgumentException("Key annotation for " + key + " must be unique");
                }
                schema.keys.put(key, field);
            }
        }

        if (schema.objectId == null && schema.encodedData == null) {
            throw new IllegalArgumentException(clazz + " must provide ObjectId and EncodedData annotations");
        }

        return schema;
    }

    /* package */ static void registerSubclass(Class<? extends RealmObject> clazz) {
        if (!RealmObject.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("Cannot register a type that is not a subclass of RealmObject");
        }

        String className = getClassName(clazz);

        synchronized (mutex) {
            RealmSchema schema = registeredSubclasses.get(className);
            if (schema != null) {
                if (clazz.isAssignableFrom(schema.clazz)) {
                    // Previous subclass is more specific or equal to the current type, do nothing.
                    return;
                } else if (schema.clazz.isAssignableFrom(clazz)) {
                    // Previous subclass is parent of new child subclass, fallthrough and actually
                    // register this class.
                        /* Do nothing */
                } else {
                    throw new IllegalArgumentException(
                            "Tried to register both " + schema.clazz.getName() + " and " + clazz.getName() +
                                    " as the ParseObject subclass of " + className + ". " + "Cannot determine the right " +
                                    "class to use because neither inherits from the other."
                    );
                }
            }
            schema = createSchema(clazz);
            registeredSubclasses.put(className, schema);
        }
    }

    /* package */ static void unregisterSubclass(Class<? extends RealmObject> clazz) {
        String className = getClassName(clazz);

        synchronized (mutex) {
            registeredSubclasses.remove(className);
        }
    }

    /* package */ static RealmSchema getSchema(String className) {
        synchronized (mutex) {
            RealmSchema schema = registeredSubclasses.get(className);
            if (schema != null) {
                return schema;
            }
            return genericSchema;
        }
    }

}
