package com.github.ponkin.bloom;

import sun.misc.Unsafe;
import sun.misc.Cleaner;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;

final class Platform {

  private static final Unsafe _UNSAFE;

  public static final int BYTE_ARRAY_OFFSET;

  public static final int INT_ARRAY_OFFSET;

  public static final int LONG_ARRAY_OFFSET;

  public static final int DOUBLE_ARRAY_OFFSET;

  public static int getInt(Object object, long offset) {
    return _UNSAFE.getInt(object, offset);
  }

  public static void putInt(Object object, long offset, int value) {
    _UNSAFE.putInt(object, offset, value);
  }

  public static boolean getBoolean(Object object, long offset) {
    return _UNSAFE.getBoolean(object, offset);
  }

  public static void putBoolean(Object object, long offset, boolean value) {
    _UNSAFE.putBoolean(object, offset, value);
  }
  
  public static byte getByte(long address) {
    return _UNSAFE.getByte(address);
  }

  public static void putByte(long address, byte value) {
    _UNSAFE.putByte(address, value);
  }

  public static long getLong(long address) {
    return _UNSAFE.getLong(address);
  }

  public static void putLong(long address, long value) {
    _UNSAFE.putLong(address, value);
  }

  public static byte getByte(Object object, long offset) {
    return _UNSAFE.getByte(object, offset);
  }

  public static void putByte(Object object, long offset, byte value) {
    _UNSAFE.putByte(object, offset, value);
  }

  public static short getShort(Object object, long offset) {
    return _UNSAFE.getShort(object, offset);
  }

  public static void putShort(Object object, long offset, short value) {
    _UNSAFE.putShort(object, offset, value);
  }

  public static long getLong(Object object, long offset) {
    return _UNSAFE.getLong(object, offset);
  }

  public static void putLong(Object object, long offset, long value) {
    _UNSAFE.putLong(object, offset, value);
  }

  public static float getFloat(Object object, long offset) {
    return _UNSAFE.getFloat(object, offset);
  }

  public static void putFloat(Object object, long offset, float value) {
    _UNSAFE.putFloat(object, offset, value);
  }

  public static double getDouble(Object object, long offset) {
    return _UNSAFE.getDouble(object, offset);
  }

  public static void putDouble(Object object, long offset, double value) {
    _UNSAFE.putDouble(object, offset, value);
  }

  public static Object getObjectVolatile(Object object, long offset) {
    return _UNSAFE.getObjectVolatile(object, offset);
  }

  public static void putObjectVolatile(Object object, long offset, Object value) {
    _UNSAFE.putObjectVolatile(object, offset, value);
  }

  public static long allocateMemory(long size) {
    return _UNSAFE.allocateMemory(size);
  }

  public static long allocateRaw(long size) {
    long address = _UNSAFE.allocateMemory(size);
    _UNSAFE.setMemory(address, size, (byte) 0);
    return address;
  }

  public static void freeMemory(long address) {
    _UNSAFE.freeMemory(address);
  }

  public static void clear(long address, long size) {
    _UNSAFE.setMemory(address, size, (byte) 0);
  }

  public static void copyMemory(Object src, long srcOffset, Object dst, long dstOffset, long length) {
    // Check if dstOffset is before or after srcOffset to determine if we should copy
    // forward or backwards. This is necessary in case src and dst overlap.
    if (dstOffset < srcOffset) {
      while (length > 0) {
        long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
        _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
        length -= size;
        srcOffset += size;
        dstOffset += size;
      }
    } else {
      srcOffset += length;
      dstOffset += length;
      while (length > 0) {
        long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
        srcOffset -= size;
        dstOffset -= size;
        _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
        length -= size;
      }

    }
  }

  /**
   * Raises an exception bypassing compiler checks for checked exceptions.
   */
  public static void throwException(Throwable t) {
    _UNSAFE.throwException(t);
  }

  public static Field getField(Class cls, String name) {
    try {
      Field f = cls.getDeclaredField(name);
      f.setAccessible(true);
      return f;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static Method getMethod(Class cls, String name, Class... params) {
    try {
      Method m = cls.getDeclaredMethod(name, params);
      m.setAccessible(true);
      return m;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static long allocateMemory(long size, Object holder) {
    final long address = _UNSAFE.allocateMemory(size);
    Cleaner.create(holder, new Runnable() {
      @Override
      public void run() {
        _UNSAFE.freeMemory(address);
      }
    });
    return address;
  }

  public static long getByteBufferAddress(ByteBuffer buffer) {
    try {
      return getField(Buffer.class, "address").getLong(buffer);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }  

  /**
   * Limits the number of bytes to copy per {@link Unsafe#copyMemory(long, long, long)} to
   * allow safepoint polling during a large copy.
   */
  private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

  static {
    sun.misc.Unsafe unsafe;
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = (sun.misc.Unsafe) unsafeField.get(null);
    } catch (Throwable cause) {
      unsafe = null;
    }
    _UNSAFE = unsafe;

    if (_UNSAFE != null) {
      BYTE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(byte[].class);
      INT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(int[].class);
      LONG_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(long[].class);
      DOUBLE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(double[].class);
    } else {
      BYTE_ARRAY_OFFSET = 0;
      INT_ARRAY_OFFSET = 0;
      LONG_ARRAY_OFFSET = 0;
      DOUBLE_ARRAY_OFFSET = 0;
    }
  }
}
