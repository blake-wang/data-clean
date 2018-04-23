package com.ijunhai.common.serde.util;

public class ByteUtils {

    public static final int BYTE_SIZE = 1;
    public static final int SHORT_SIZE = 2;
    public static final int INT_SIZE = 4;
    public static final int LONG_SIZE = 8;
    public static final int FLOAT_SIZE = INT_SIZE;
    public static final int DOUBLE_SIZE = LONG_SIZE;


    public ByteUtils() {
    }

    public static int writeByte(int val, byte[] array, int offset) {
        return writeUnsignedByte((val ^ 0x80), array, offset);
    }

    public static int writeUnsignedByte(int val, byte[] array, int offset) {
        array[offset++] = (byte) val;
        return offset;
    }

    public static int write(final byte[] arrayFrom, byte[] arrayTo, int offsetTo) {
        return write(arrayFrom, 0, arrayTo, offsetTo, arrayFrom.length);
    }

    public static int write(final byte[] arrayFrom, int offsetFrom, byte[] arrayTo, int offsetTo, int len) {
        System.arraycopy(arrayFrom, offsetFrom, arrayTo, offsetTo, len);
        return offsetTo + len;
    }


    public static int writeBoolean(boolean val, byte[] arrayTo, int offsetTo) {
        arrayTo[offsetTo++] = (val ? (byte) 1 : (byte) 0);
        return offsetTo;
    }

    public static int writeShort(int val, byte[] arrayTo, int offsetTo) {
        return writeUnsignedShort(val ^ 0x8000, arrayTo, offsetTo);
    }

    public static int writeUnsignedShort(int val, byte[] arrayTo, int offsetTo) {
        arrayTo[offsetTo++] = (byte) (val >>> 8);
        arrayTo[offsetTo++] = (byte) val;
        return offsetTo;
    }

    public static int writeInt(int val, byte[] arrayTo, int offsetTo) {
        return writeUnsignedInt(val ^ 0x80000000, arrayTo, offsetTo);
    }

    public static byte[] writeInt(int val) {
        byte[] arrayTo = new byte[INT_SIZE];
        writeUnsignedInt(val ^ 0x80000000, arrayTo, 0);
        return arrayTo;
    }

    public static int writeUnsignedInt(long val, byte[] arrayTo, int offsetTo) {
        arrayTo[offsetTo++] = ((byte) (val >>> 24));
        arrayTo[offsetTo++] = ((byte) (val >>> 16));
        arrayTo[offsetTo++] = ((byte) (val >>> 8));
        arrayTo[offsetTo++] = ((byte) val);
        return offsetTo;
    }

    public static int writeLong(long val, byte[] arrayTo, int offsetTo) {
        return writeUnsignedLong(val ^ 0x8000000000000000L, arrayTo, offsetTo);
    }

    public static int writeUnsignedLong(long val, byte[] arrayTo, int offsetTo) {
        arrayTo[offsetTo++] = ((byte) (val >>> 56));
        arrayTo[offsetTo++] = ((byte) (val >>> 48));
        arrayTo[offsetTo++] = ((byte) (val >>> 40));
        arrayTo[offsetTo++] = ((byte) (val >>> 32));
        arrayTo[offsetTo++] = ((byte) (val >>> 24));
        arrayTo[offsetTo++] = ((byte) (val >>> 16));
        arrayTo[offsetTo++] = ((byte) (val >>> 8));
        arrayTo[offsetTo++] = ((byte) val);
        return offsetTo;
    }

    public static int writeFloat(float val, byte[] arrayTo, int offsetTo) {
        return writeUnsignedInt(Float.floatToIntBits(val), arrayTo, offsetTo);
    }

    public static int writeDouble(double val, byte[] arrayTo, int offsetTo) {
        return writeUnsignedLong(Double.doubleToLongBits(val), arrayTo, offsetTo);
    }

    /**
     * read Section
     */

    public static byte readByte(final byte[] array, int offset) {
        return (byte) (readUnsignedByte(array, offset) ^ 0x80);
    }

    public static int readUnsignedByte(final byte[] array, int offset) {
        return get(array, offset);
    }

    private static int get(final byte[] array, int offset) {
        return array[offset] & 0xff;
    }

    public static boolean readBoolean(final byte[] array, int offset) {
        return readByte(array, offset) != (byte) 0;
    }

    public static short readShort(final byte[] array, int offset) {
        return (short) (readUnsignedShort(array, offset) ^ 0x8000);
    }

    public static int readUnsignedShort(final byte[] array, int offset) {
        int c1 = get(array, offset++);
        int c2 = get(array, offset++);
        return ((c1 << 8) | c2);
    }

    public static int readInt(final byte[] array, int offset) {
        return (int) (readUnsignedInt(array, offset) ^ 0x80000000);
    }

    public static long readUnsignedInt(final byte[] array, int offset) {
        long c1 = get(array, offset++);
        long c2 = get(array, offset++);
        long c3 = get(array, offset++);
        long c4 = get(array, offset++);
        return ((c1 << 24) | (c2 << 16) | (c3 << 8) | c4);
    }

    public static long readLong(final byte[] array, int offset) {
        return readUnsignedLong(array, offset) ^ 0x8000000000000000L;
    }

    public static long readUnsignedLong(final byte[] array, int offset) {
        long c1 = get(array, offset++);
        long c2 = get(array, offset++);
        long c3 = get(array, offset++);
        long c4 = get(array, offset++);
        long c5 = get(array, offset++);
        long c6 = get(array, offset++);
        long c7 = get(array, offset++);
        long c8 = get(array, offset++);

        return ((c1 << 56) | (c2 << 48) | (c3 << 40) | (c4 << 32) |
                (c5 << 24) | (c6 << 16) | (c7 << 8) | c8);

    }

    public static float readFloat(final byte[] array, int offset) {
        return Float.intBitsToFloat((int) readUnsignedInt(array, offset));
    }

    public static double readDouble(final byte[] array, int offset) {
        return Double.longBitsToDouble(readUnsignedLong(array, offset));
    }

    public static byte[] readBytes(final byte[] array, int offset, int length) {
        byte[] bytes = new byte[length];
        System.arraycopy(array, offset, bytes, 0, length);
        return bytes;
    }

}
