package com.parse;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import bolts.Task;
import bolts.TaskCompletionSource;

import io.realm.Case;
import io.realm.Realm;
import io.realm.RealmChangeListener;
import io.realm.RealmConfiguration;
import io.realm.RealmObject;
import io.realm.RealmResults;
import io.realm.annotations.Required;

/**
 * Created by maxime on 11/01/2017.
 */

public final class RealmQuery<T extends ParseObject> {

    private RealmConfiguration config;
    private String className;
    private io.realm.RealmQuery<? extends RealmObject> query;

    private RealmQuery(RealmStore store, String parseClassName) {
        config = store.getConfiguration();
        className = parseClassName;
        Realm realm = Realm.getInstance(config);
        RealmSchemaController.RealmSchema schema = RealmSchemaController.getSchema(parseClassName);
        this.query = realm.where(schema.clazz);

        if (schema.clazz == ParseRealmObject.class) {
            this.query.equalTo("parseClassName", parseClassName);
        }
    }

    public static <T extends ParseObject> RealmQuery<T> createQuery(String parseClassName) {
        return new RealmQuery<>(new RealmStore(), parseClassName);
    }

    public static <T extends ParseObject> RealmQuery<T> createQuery(Class<T> clazz) {
        String parseClassName = ParseCorePlugins.getInstance().getSubclassingController().getClassName(clazz);
        return new RealmQuery<>(new RealmStore(), parseClassName);
    }

    public static <T extends ParseObject> RealmQuery<T> createQuery(RealmStore store, String parseClassName) {
        return new RealmQuery<>(store, parseClassName);
    }

    public static <T extends ParseObject> RealmQuery<T> createQuery(RealmStore store, Class<T> clazz) {
        String parseClassName = ParseCorePlugins.getInstance().getSubclassingController().getClassName(clazz);
        return new RealmQuery<>(store, parseClassName);
    }

    /**
     * Tests if a field is {@code null}. Only works for nullable fields.
     * <p>
     * For link queries, if any part of the link path is {@code null} the whole path is considered to be {@code null}
     * e.g., {@code isNull("linkField.stringField")} will be considered to be {@code null} if either {@code linkField} or
     * {@code linkField.stringField} is {@code null}.
     *
     * @param fieldName the field name.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if the field is not nullable.
     * @see Required for further infomation.
     */
    public RealmQuery<T> isNull(String fieldName) {
        this.query.isNull(fieldName);
        return this;
    }

    /**
     * Tests if a field is not {@code null}. Only works for nullable fields.
     *
     * @param fieldName the field name.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if the field is not nullable.
     * @see Required for further infomation.
     */
    public RealmQuery<T> isNotNull(String fieldName) {
        this.query.isNotNull(fieldName);
        return this;
    }

    // Equal

    /**
     * Equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> equalTo(String fieldName, String value) {
        return this.equalTo(fieldName, value, Case.SENSITIVE);
    }

    /**
     * Equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @param casing     how to handle casing. Setting this to {@link Case#INSENSITIVE} only works for Latin-1 characters.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> equalTo(String fieldName, String value, Case casing) {
        this.query.equalTo(fieldName, value, casing);
        return this;
    }

    /**
     * Equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> equalTo(String fieldName, Byte value) {
        this.query.equalTo(fieldName, value);
        return this;
    }

    /**
     * Equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> equalTo(String fieldName, byte[] value) {
        this.query.equalTo(fieldName, value);
        return this;
    }

    /**
     * Equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> equalTo(String fieldName, Short value) {
        this.query.equalTo(fieldName, value);
        return this;
    }

    /**
     * Equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> equalTo(String fieldName, Integer value) {
        this.query.equalTo(fieldName, value);
        return this;
    }

    /**
     * Equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> equalTo(String fieldName, Long value) {
        this.query.equalTo(fieldName, value);
        return this;
    }
    /**
     * Equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> equalTo(String fieldName, Double value) {
        this.query.equalTo(fieldName, value);
        return this;
    }

    /**
     * Equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return The query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> equalTo(String fieldName, Float value) {
        this.query.equalTo(fieldName, value);
        return this;
    }

    /**
     * Equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> equalTo(String fieldName, Boolean value) {
        this.query.equalTo(fieldName, value);
        return this;
    }

    /**
     * Equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> equalTo(String fieldName, Date value) {
        this.query.equalTo(fieldName, value);
        return this;
    }

    // In

    /**
     * In comparison. This allows you to test if objects match any value in an array of values.
     *
     * @param fieldName the field to compare.
     * @param values array of values to compare with and it cannot be null or empty.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if the field isn't a String field or {@code values} is {@code null} or empty.
     */
    public RealmQuery<T> in(String fieldName, String[] values) {
        return in(fieldName, values, Case.SENSITIVE);
    }

    /**
     * In comparison. This allows you to test if objects match any value in an array of values.
     *
     * @param fieldName the field to compare.
     * @param values array of values to compare with and it cannot be null or empty.
     * @param casing how casing is handled. {@link Case#INSENSITIVE} works only for the Latin-1 characters.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if the field isn't a String field or {@code values} is {@code null} or empty.
     */
    public RealmQuery<T> in(String fieldName, String[] values, Case casing) {
        this.query.in(fieldName, values, casing);
        return this;
    }

    /**
     * In comparison. This allows you to test if objects match any value in an array of values.
     *
     * @param fieldName the field to compare.
     * @param values array of values to compare with and it cannot be null or empty.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if the field isn't a Byte field or {@code values} is {@code null} or empty.
     */
    public RealmQuery<T> in(String fieldName, Byte[] values) {
        this.query.in(fieldName, values);
        return this;
    }

    /**
     * In comparison. This allows you to test if objects match any value in an array of values.
     *
     * @param fieldName the field to compare.
     * @param values array of values to compare with and it cannot be null or empty.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if the field isn't a Short field or {@code values} is {@code null} or empty.
     */
    public RealmQuery<T> in(String fieldName, Short[] values) {
        this.query.in(fieldName, values);
        return this;
    }

    /**
     * In comparison. This allows you to test if objects match any value in an array of values.
     *
     * @param fieldName the field to compare.
     * @param values array of values to compare with and it cannot be null or empty.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if the field isn't a Integer field or {@code values} is {@code null} or empty.
     */
    public RealmQuery<T> in(String fieldName, Integer[] values) {
        this.query.in(fieldName, values);
        return this;
    }

    /**
     * In comparison. This allows you to test if objects match any value in an array of values.
     *
     * @param fieldName the field to compare.
     * @param values array of values to compare with and it cannot be null or empty.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if the field isn't a Long field or {@code values} is {@code null} or empty.
     */
    public RealmQuery<T> in(String fieldName, Long[] values) {
        this.query.in(fieldName, values);
        return this;
    }

    /**
     * In comparison. This allows you to test if objects match any value in an array of values.
     *
     * @param fieldName the field to compare.
     * @param values array of values to compare with and it cannot be null or empty.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if the field isn't a Double field or {@code values} is {@code null} or empty.
     */
    public RealmQuery<T> in(String fieldName, Double[] values) {
        this.query.in(fieldName, values);
        return this;
    }

    /**
     * In comparison. This allows you to test if objects match any value in an array of values.
     *
     * @param fieldName the field to compare.
     * @param values array of values to compare with and it cannot be null or empty.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if the field isn't a Float field or {@code values} is {@code null} or empty.
     */
    public RealmQuery<T> in(String fieldName, Float[] values) {
        this.query.in(fieldName, values);
        return this;
    }

    /**
     * In comparison. This allows you to test if objects match any value in an array of values.
     *
     * @param fieldName the field to compare.
     * @param values array of values to compare with and it cannot be null or empty.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if the field isn't a Boolean field or {@code values} is {@code null} or empty.
     */
    public RealmQuery<T> in(String fieldName, Boolean[] values) {
        this.query.in(fieldName, values);
        return this;
    }

    /**
     * In comparison. This allows you to test if objects match any value in an array of values.
     *
     * @param fieldName the field to compare.
     * @param values array of values to compare with and it cannot be null or empty.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if the field isn't a Date field or {@code values} is {@code null} or empty.
     */
    public RealmQuery<T> in(String fieldName, Date[] values) {
        this.query.in(fieldName, values);
        return this;
    }

    // Not Equal

    /**
     * Not-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> notEqualTo(String fieldName, String value) {
        return notEqualTo(fieldName, value, Case.SENSITIVE);
    }

    /**
     * Not-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @param casing     how casing is handled. {@link Case#INSENSITIVE} works only for the Latin-1 characters.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> notEqualTo(String fieldName, String value, Case casing) {
        this.query.notEqualTo(fieldName, value, casing);
        return this;
    }

    /**
     * Not-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> notEqualTo(String fieldName, Byte value) {
        this.query.notEqualTo(fieldName, value);
        return this;
    }

    /**
     * Not-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> notEqualTo(String fieldName, byte[] value) {
        this.query.notEqualTo(fieldName, value);
        return this;
    }

    /**
     * Not-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> notEqualTo(String fieldName, Short value) {
        this.query.notEqualTo(fieldName, value);
        return this;
    }

    /**
     * Not-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> notEqualTo(String fieldName, Integer value) {
        this.query.notEqualTo(fieldName, value);
        return this;
    }

    /**
     * Not-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> notEqualTo(String fieldName, Long value) {
        this.query.notEqualTo(fieldName, value);
        return this;
    }

    /**
     * Not-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> notEqualTo(String fieldName, Double value) {
        this.query.notEqualTo(fieldName, value);
        return this;
    }

    /**
     * Not-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> notEqualTo(String fieldName, Float value) {
        this.query.notEqualTo(fieldName, value);
        return this;
    }

    /**
     * Not-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> notEqualTo(String fieldName, Boolean value) {
        this.query.notEqualTo(fieldName, value);
        return this;
    }

    /**
     * Not-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> notEqualTo(String fieldName, Date value) {
        this.query.notEqualTo(fieldName, value);
        return this;
    }

    // Greater Than

    /**
     * Greater-than comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> greaterThan(String fieldName, int value) {
        this.query.greaterThan(fieldName, value);
        return this;
    }

    /**
     * Greater-than comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> greaterThan(String fieldName, long value) {
        this.query.greaterThan(fieldName, value);
        return this;
    }

    /**
     * Greater-than comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> greaterThan(String fieldName, double value) {
        this.query.greaterThan(fieldName, value);
        return this;
    }

    /**
     * Greater-than comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> greaterThan(String fieldName, float value) {
        this.query.greaterThan(fieldName, value);
        return this;
    }

    /**
     * Greater-than comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> greaterThan(String fieldName, Date value) {
        this.query.greaterThan(fieldName, value);
        return this;
    }

    /**
     * Greater-than-or-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> greaterThanOrEqualTo(String fieldName, int value) {
        this.query.greaterThanOrEqualTo(fieldName, value);
        return this;
    }

    /**
     * Greater-than-or-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> greaterThanOrEqualTo(String fieldName, long value) {
        this.query.greaterThanOrEqualTo(fieldName, value);
        return this;
    }

    /**
     * Greater-than-or-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> greaterThanOrEqualTo(String fieldName, double value) {
        this.query.greaterThanOrEqualTo(fieldName, value);
        return this;
    }

    /**
     * Greater-than-or-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type
     */
    public RealmQuery<T> greaterThanOrEqualTo(String fieldName, float value) {
        this.query.greaterThanOrEqualTo(fieldName, value);
        return this;
    }

    /**
     * Greater-than-or-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> greaterThanOrEqualTo(String fieldName, Date value) {
        this.query.greaterThanOrEqualTo(fieldName, value);
        return this;
    }

    // Less Than

    /**
     * Less-than comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> lessThan(String fieldName, int value) {
        this.query.lessThan(fieldName, value);
        return this;
    }

    /**
     * Less-than comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> lessThan(String fieldName, long value) {
        this.query.lessThan(fieldName, value);
        return this;
    }

    /**
     * Less-than comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> lessThan(String fieldName, double value) {
        this.query.lessThan(fieldName, value);
        return this;
    }

    /**
     * Less-than comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> lessThan(String fieldName, float value) {
        this.query.lessThan(fieldName, value);
        return this;
    }

    /**
     * Less-than comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> lessThan(String fieldName, Date value) {
        this.query.lessThan(fieldName, value);
        return this;
    }

    /**
     * Less-than-or-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> lessThanOrEqualTo(String fieldName, int value) {
        this.query.lessThanOrEqualTo(fieldName, value);
        return this;
    }

    /**
     * Less-than-or-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> lessThanOrEqualTo(String fieldName, long value) {
        this.query.lessThanOrEqualTo(fieldName, value);
        return this;
    }

    /**
     * Less-than-or-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> lessThanOrEqualTo(String fieldName, double value) {
        this.query.lessThanOrEqualTo(fieldName, value);
        return this;
    }

    /**
     * Less-than-or-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> lessThanOrEqualTo(String fieldName, float value) {
        this.query.lessThanOrEqualTo(fieldName, value);
        return this;
    }

    /**
     * Less-than-or-equal-to comparison.
     *
     * @param fieldName the field to compare.
     * @param value the value to compare with.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> lessThanOrEqualTo(String fieldName, Date value) {
        this.query.lessThanOrEqualTo(fieldName, value);
        return this;
    }

    // Between

    /**
     * Between condition.
     *
     * @param fieldName the field to compare.
     * @param from lowest value (inclusive).
     * @param to highest value (inclusive).
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> between(String fieldName, int from, int to) {
        this.query.between(fieldName, from, to);
        return this;
    }

    /**
     * Between condition.
     *
     * @param fieldName the field to compare.
     * @param from lowest value (inclusive).
     * @param to highest value (inclusive).
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> between(String fieldName, long from, long to) {
        this.query.between(fieldName, from, to);
        return this;
    }

    /**
     * Between condition.
     *
     * @param fieldName the field to compare.
     * @param from lowest value (inclusive).
     * @param to highest value (inclusive).
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> between(String fieldName, double from, double to) {
        this.query.between(fieldName, from, to);
        return this;
    }

    /**
     * Between condition.
     *
     * @param fieldName the field to compare.
     * @param from lowest value (inclusive).
     * @param to highest value (inclusive).
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> between(String fieldName, float from, float to) {
        this.query.between(fieldName, from, to);
        return this;
    }

    /**
     * Between condition.
     *
     * @param fieldName the field to compare.
     * @param from lowest value (inclusive).
     * @param to highest value (inclusive).
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> between(String fieldName, Date from, Date to) {
        this.query.between(fieldName, from, to);
        return this;
    }


    // Contains

    /**
     * Condition that value of field contains the specified substring.
     *
     * @param fieldName the field to compare.
     * @param value the substring.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> contains(String fieldName, String value) {
        return contains(fieldName, value, Case.SENSITIVE);
    }

    /**
     * Condition that value of field contains the specified substring.
     *
     * @param fieldName the field to compare.
     * @param value the substring.
     * @param casing     how to handle casing. Setting this to {@link Case#INSENSITIVE} only works for Latin-1 characters.
     * @return The query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> contains(String fieldName, String value, Case casing) {
        this.query.contains(fieldName, value, casing);
        return this;
    }

    /**
     * Condition that the value of field begins with the specified string.
     *
     * @param fieldName the field to compare.
     * @param value the string.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> beginsWith(String fieldName, String value) {
        return beginsWith(fieldName, value, Case.SENSITIVE);
    }

    /**
     * Condition that the value of field begins with the specified substring.
     *
     * @param fieldName the field to compare.
     * @param value the substring.
     * @param casing     how to handle casing. Setting this to {@link Case#INSENSITIVE} only works for Latin-1 characters.
     * @return the query object
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> beginsWith(String fieldName, String value, Case casing) {
        this.query.beginsWith(fieldName, value, casing);
        return this;
    }

    /**
     * Condition that the value of field ends with the specified string.
     *
     * @param fieldName the field to compare.
     * @param value the string.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if one or more arguments do not match class or field type.
     */
    public RealmQuery<T> endsWith(String fieldName, String value) {
        return endsWith(fieldName, value, Case.SENSITIVE);
    }

    /**
     * Condition that the value of field ends with the specified substring.
     *
     * @param fieldName the field to compare.
     * @param value the substring.
     * @param casing     how to handle casing. Setting this to {@link Case#INSENSITIVE} only works for Latin-1 characters.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException One or more arguments do not match class or field type.
     */
    public RealmQuery<T> endsWith(String fieldName, String value, Case casing) {
        this.query.endsWith(fieldName, value, casing);
        return this;
    }

    // Grouping

    /**
     * Begin grouping of conditions ("left parenthesis"). A group must be closed with a call to {@code endGroup()}.
     *
     * @return the query object.
     * @see #endGroup()
     */
    public RealmQuery<T> beginGroup() {
        this.query.beginGroup();
        return this;
    }

    /**
     * End grouping of conditions ("right parenthesis") which was opened by a call to {@code beginGroup()}.
     *
     * @return the query object.
     * @see #beginGroup()
     */
    public RealmQuery<T> endGroup() {
        this.query.endGroup();
        return this;
    }

    /**
     * Logical-or two conditions.
     *
     * @return the query object.
     */
    public RealmQuery<T> or() {
        this.query.or();
        return this;
    }

    /**
     * Negate condition.
     *
     * @return the query object.
     */
    public RealmQuery<T> not() {
        this.query.not();
        return this;
    }

    /**
     * Condition that finds values that are considered "empty" i.e., an empty list, the 0-length string or byte array.
     *
     * @param fieldName the field to compare.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if the field name isn't valid or its type isn't either a RealmList,
     * String or byte array.
     */
    public RealmQuery<T> isEmpty(String fieldName) {
        this.query.isEmpty(fieldName);
        return this;
    }

    /**
     * Condition that finds values that are considered "Not-empty" i.e., a list, a string or a byte array with not-empty values.
     *
     * @param fieldName the field to compare.
     * @return the query object.
     * @throws java.lang.IllegalArgumentException if the field name isn't valid or its type isn't either a RealmList,
     * String or byte array.
     */
    public RealmQuery<T> isNotEmpty(String fieldName) {
        this.query.isNotEmpty(fieldName);
        return this;
    }

    public Task<T> findFirstAsync() {

        final TaskCompletionSource<T> source = new TaskCompletionSource<>();

        RealmChangeListener<RealmObject> listener = new RealmChangeListener<RealmObject>() {
            @Override
            public void onChange(RealmObject element) {

            try {
                Realm realm = Realm.getInstance(config);
                element.removeChangeListeners();

                T object = ParseRealmObject.decode(realm, className, element);

                source.setResult(object);
            } catch (Exception e) {
                source.setError(e);
            }
            }
        };

        RealmObject object = query.findFirstAsync();
        object.addChangeListener(listener);

        return source.getTask();
    }

    public Task<List<T>> findAllAsync() {

        final TaskCompletionSource<List<T>> source = new TaskCompletionSource<>();

        RealmChangeListener<RealmResults<RealmObject>> listener = new RealmChangeListener<RealmResults<RealmObject>>() {
            @Override
            public void onChange(RealmResults<RealmObject> results) {

            try {
                Realm realm = Realm.getInstance(config);
                ArrayList<T> objects = new ArrayList<>();

                for (RealmObject element : results) {

                    T object = ParseRealmObject.decode(realm, className, element);
                    objects.add(object);
                }
                results.removeChangeListeners();

                source.setResult(objects);
            } catch (Exception e) {
                source.setError(e);
            }
            }
        };

        RealmResults results = query.findAllAsync();
        results.addChangeListener(listener);

        return source.getTask();
    }
}
