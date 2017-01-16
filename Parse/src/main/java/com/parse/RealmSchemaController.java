package com.parse;

import com.parse.realm.annotations.EncodedField;
import com.parse.realm.annotations.ObjectIdField;
import com.parse.realm.annotations.ParseClassNameField;

import java.lang.reflect.Field;
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
        Field objectIdField;
        Field parseClassNameField;
        Field encodedField;

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

            if (field.isAnnotationPresent(ObjectIdField.class)) {
                if (schema.objectIdField != null) {
                    throw new IllegalArgumentException("ObjectIdField annotation has to be unique");
                }
                if (field.getType() != String.class) {
                    throw new IllegalArgumentException("ObjectIdField annotation must associated to a String field");
                }
                schema.objectIdField = field;
                break;
            }

            if (field.isAnnotationPresent(ParseClassNameField.class)) {
                if (schema.parseClassNameField != null) {
                    throw new IllegalArgumentException("ParseClassNameField annotation has to be unique");
                }
                if (field.getType() != String.class) {
                    throw new IllegalArgumentException("ParseClassNameField annotation must associated to a String field");
                }
                schema.parseClassNameField = field;
                break;
            }

            if (field.isAnnotationPresent(EncodedField.class)) {
                if (schema.encodedField != null) {
                    throw new IllegalArgumentException("EncodedField annotation has to be unique");
                }
                if (field.getType() != String.class) {
                    throw new IllegalArgumentException("EncodedField annotation must associated to a String field");
                }
                schema.encodedField = field;
                break;
            }
        }

        if (schema.objectIdField == null && schema.parseClassNameField == null && schema.encodedField == null) {
            throw new IllegalArgumentException("ObjectIdField annotation has to be unique");
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
