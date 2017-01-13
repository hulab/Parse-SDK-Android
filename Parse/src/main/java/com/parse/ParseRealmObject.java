package com.parse;

import android.util.Pair;

import org.json.JSONException;
import org.json.JSONObject;

import java.security.InvalidParameterException;
import java.util.Date;

import io.realm.Realm;
import io.realm.RealmObject;
import io.realm.annotations.PrimaryKey;
import io.realm.annotations.Required;

/**
 * Created by maxime on 10/01/2017.
 */

public class ParseRealmObject<T extends ParseObject> extends RealmObject {

    @PrimaryKey
    private String objectId;

    @Required
    private String parseClassName;

    private Date createdAt;

    private Date updatedAt;

    @Required
    private String encodedObject;

    public String getObjectId() {
        return objectId;
    }

    /**
     * Accessor to the class name.
     */
    public String getParseClassName() {
        return parseClassName;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public Date getUpdatedAt() {
        return updatedAt;
    }

    /* package */ static <T extends ParseObject> ParseRealmObject<T> encode(Realm realm, T object) {

        Class<? extends ParseRealmObject> clazz = RealmStore.RealmSchemaController.getSubclass(object.getClassName());
        ParseRealmObject<T> realmObject = realm.createObject(clazz, object.getObjectId());

        cacheObject(object);

        realmObject.objectId = object.getObjectId();
        realmObject.parseClassName = object.getClassName();
        realmObject.createdAt  = object.getCreatedAt();
        realmObject.updatedAt = object.getUpdatedAt();

        ParseEncoder encoder = new PointerEncoder();
        realmObject.encodedObject = object.toRest(encoder).toString();

        realmObject.encode(object);

        return realmObject;
    }

    protected void encode(T object) {

    }

    /* package */ T decode(Realm realm) {
        ParseObject object = getObjectFromCache(parseClassName, objectId);

        if(object == null) {
            object = ParseObject.createWithoutData(parseClassName, objectId);
            cacheObject(object);

            try {
                RealmDecoder decoder = new RealmDecoder(realm);
                object.build(new JSONObject(encodedObject), decoder);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        return (T)object;
    }

    public static void registerRealmSchema(Class<? extends ParseRealmObject> subclass) {
        RealmStore.RealmSchemaController.registerSubclass(subclass);
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

            Class<? extends ParseRealmObject> clazz = RealmStore.RealmSchemaController.getSubclass(parseClassName);

            ParseRealmObject realmObject = realm.where(clazz).equalTo("objectId", objectId).findFirst();

            if (realmObject != null) {
                try {
                    RealmDecoder decoder = new RealmDecoder(realm);
                    object.build(new JSONObject(realmObject.encodedObject), decoder);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        }
        return object;
    }

}
