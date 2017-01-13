package com.parse;

import java.lang.ref.WeakReference;

import io.realm.Realm;

/**
 * Created by maxime on 10/01/2017.
 */

class RealmDecoder extends ParseDecoder {

    private WeakReference<Realm> realmRef;

    RealmDecoder() {
        realmRef = new WeakReference<>(Realm.getDefaultInstance());
    }

    RealmDecoder(Realm realm) {
        realmRef = new WeakReference<>(realm);
    }

    @Override
    protected ParseObject decodePointer(String className, String objectId) {
        return ParseRealmObject.parseObject(realmRef.get(), className, objectId);
    }
}
