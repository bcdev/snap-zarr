package org.esa.snap;

import java.lang.reflect.Field;

public class TestHelper {

    public static Object getPrivateFieldObject(Object fieldOwner, String fieldName) throws NoSuchFieldException, IllegalAccessException {
        Field field = findFieldRecursively(fieldOwner, fieldName);
        field.setAccessible(true);
        return field.get(fieldOwner);
    }

    public static void setPrivateFieldObject(Object fieldOwner, String fieldName, Object fieldValueToBeSet) throws NoSuchFieldException, IllegalAccessException {
        Field field = findFieldRecursively(fieldOwner, fieldName);
        field.setAccessible(true);
        field.set(fieldOwner, fieldValueToBeSet);
    }


    private static Field findFieldRecursively(Object fieldOwner, String fieldName) throws NoSuchFieldException {
        Class<?> aClass = fieldOwner.getClass();
        Field field = null;
        try {
            field = aClass.getDeclaredField(fieldName);
        } catch (NoSuchFieldException ignore) {
        }
        while (field == null) {
            aClass = aClass.getSuperclass();
            try {
                field = aClass.getDeclaredField(fieldName);
            } catch (NoSuchFieldException ignore) {
            }
        }
        if (field == null) {
            throw new NoSuchFieldException("Field with name '" + fieldName + "' not found.");
        }
        return field;
    }

}
