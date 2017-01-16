package com.parse;

import android.util.Pair;

import com.parse.realm.annotations.EncodedField;
import com.parse.realm.annotations.ObjectIdField;
import com.parse.realm.annotations.ParseClassNameField;

import org.json.JSONObject;

import io.realm.Realm;
import io.realm.RealmObject;
import io.realm.annotations.PrimaryKey;
import io.realm.annotations.Required;

/**
 * Created by maxime on 10/01/2017.
 */

public class ParseRealmObject extends RealmObject {

    @PrimaryKey
    @ObjectIdField
    private String objectId;

    @Required
    @ParseClassNameField
    private String parseClassName;

    @Required
    @EncodedField
    private String encodedObject;

    /* package */ static <T extends ParseObject> RealmObject encode(Realm realm, T object) {

        try {

            RealmSchemaController.RealmSchema schema = RealmSchemaController.getSchema(object.getClassName());
            RealmObject realmObject = realm.createObject(schema.clazz);

            schema.objectIdField.set(realmObject, object.getObjectId());

            cacheObject(object);

            schema.objectIdField.set(realmObject, object.getObjectId());
            schema.parseClassNameField.set(realmObject, object.getClassName());

            ParseEncoder encoder = new PointerEncoder();
            schema.encodedField.set(realmObject, object.toRest(encoder).toString());

            if (realmObject instanceof Mapper) {
                ((Mapper<T>)realmObject).map(object);
            }

            return realmObject;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /* package */ static <T extends ParseObject> T decode(Realm realm, String parseClassName, RealmObject realmObject) {

        try {
            RealmSchemaController.RealmSchema schema = RealmSchemaController.getSchema(parseClassName);
            String objectId = (String) schema.objectIdField.get(realmObject);

            ParseObject object = getObjectFromCache(parseClassName, objectId);

            if(object == null) {
                object = ParseObject.createWithoutData(parseClassName, objectId);
                cacheObject(object);

                String json = (String) schema.encodedField.get(realmObject);
                if (json != null) {
                    RealmDecoder decoder = new RealmDecoder(realm);
                    object.build(new JSONObject(json), decoder);
                }
            }

            return (T)object;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private static final WeakValueHashMap<Pair<String, String>, ParseObject>
            objectMapCache = new WeakValueHashMap<>();

    private static <T extends ParseObject> void cacheObject(T object) {
        String objectId = object.getObjectId();
        if (objectId != null) {
            Pair<String, String> pair = Pair.create(object.getClassName(), objectId);
            objectMapCache.put(pair, object);
        }
    }

    private static void removeObjectFromCache(ParseObject object) {
        String objectId = object.getObjectId();
        if (objectId != null) {
            Pair<String, String> pair = Pair.create(object.getClassName(), objectId);
            objectMapCache.remove(pair);
        }
    }

    private static ParseObject getObjectFromCache(String parseClassName, String objectId) {
        if (objectId == null) {
            throw new IllegalStateException("objectId cannot be null.");
        }

        Pair<String, String> pair = Pair.create(parseClassName, objectId);
        return objectMapCache.get(pair);
    }

    /* package */ static ParseObject parseObject(String parseClassName, String objectId) {
        return parseObject(Realm.getDefaultInstance(), parseClassName, objectId);
    }

    /* package */ static ParseObject parseObject(Realm realm, String parseClassName, String objectId) {
        ParseObject object = getObjectFromCache(parseClassName, objectId);

        if(object == null) {
            object = ParseObject.createWithoutData(parseClassName, objectId);
            cacheObject(object);

            RealmSchemaController.RealmSchema schema = RealmSchemaController.getSchema(parseClassName);
            RealmObject realmObject = realm.where(schema.clazz).equalTo(schema.objectIdField.getName(), objectId).findFirst();

            if (realmObject != null) {
                try {

                    RealmDecoder decoder = new RealmDecoder(realm);
                    String json = (String) schema.encodedField.get(realmObject);
                    object.build(new JSONObject(json), decoder);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return object;
    }

    public interface Mapper<T extends ParseObject> {
        void map(T object);
    }

}
