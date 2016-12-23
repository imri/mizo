package mizo.hbase.patches;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;

import java.nio.ByteBuffer;

/**
 * Created by imrihecht on 12/23/16.
 */
public class MizoTitanStaticBuffer implements StaticBuffer {
    protected final MizoVirtualArray array;

    public MizoTitanStaticBuffer(byte[] array, int offsetKey, int limitKey, int offsetValue, int limitValue) {
        this.array = new MizoVirtualArray(array, offsetKey, limitKey, offsetValue, limitValue);
    }

    public MizoTitanStaticBuffer(MizoVirtualArray array) {
        this.array = array;
    }

    private int require(int position, int size) {
        return this.array.require(position, size);
    }

    public int length() {
        return this.array.length();
    }

    public StaticBuffer subrange(int position, int length) {
        return this.subrange(position, length, false);
    }

    public StaticBuffer subrange(int position, int length, boolean invert) {
        if(position >= 0 && length >= 0 && position + length <= this.array.length()) {
            if(!invert) {
                return new MizoTitanStaticBuffer(this.array.subrange(position, length));
            } else {
                return new MizoTitanStaticBuffer(new MizoInvertedVirtualArray(this.array.subrange(position, length)));
            }
        } else {
            throw new ArrayIndexOutOfBoundsException("Position [" + position + "] and or length [" + length + "] out of bounds");
        }
    }

    public ReadBuffer asReadBuffer() {
        return new MizoTitanReadBuffer(this.array);
    }

    public ByteBuffer asByteBuffer() {
        throw new RuntimeException("Not implemented");
    }

    public <T> T as(Factory<T> factory) {
        throw new RuntimeException("Not implemented");
    }


    public byte getByte(int position) {
        return this.array.get(this.require(position, 1));
    }

    public boolean getBoolean(int position) {
        return this.getByte(position) > 0;
    }

    public short getShort(int position) {
        int base = this.require(position, 2);
        return (short)((this.array.get(base++) & 255) << 8 | this.array.get(base++) & 255);
    }

    public int getInt(int position) {
        int base = this.require(position, 4);
        return getInt(this.array, base);
    }

    public static int getInt(MizoVirtualArray array, int offset) {
        return (array.get(offset++) & 255) << 24 | (array.get(offset++) & 255) << 16 | (array.get(offset++) & 255) << 8 | array.get(offset) & 255;
    }

    public long getLong(int position) {
        int offset = this.require(position, 8);
        return getLong(this.array, offset);
    }

    public static long getLong(MizoVirtualArray array, int offset) {
        return (long)array.get(offset++) << 56 | (long)(array.get(offset++) & 255) << 48 | (long)(array.get(offset++) & 255) << 40 | (long)(array.get(offset++) & 255) << 32 | (long)(array.get(offset++) & 255) << 24 | (long)((array.get(offset++) & 255) << 16) | (long)((array.get(offset++) & 255) << 8) | (long)(array.get(offset++) & 255);
    }

    public char getChar(int position) {
        return (char)this.getShort(position);
    }

    public float getFloat(int position) {
        return Float.intBitsToFloat(this.getInt(position));
    }

    public double getDouble(int position) {
        return Double.longBitsToDouble(this.getLong(position));
    }

    public byte[] getBytes(int position, int length) {
        byte[] result = new byte[length];

        for(int i = 0; i < length; ++i) {
            result[i] = this.getByte(position);
            ++position;
        }

        return result;
    }

    public short[] getShorts(int position, int length) {
        short[] result = new short[length];

        for(int i = 0; i < length; ++i) {
            result[i] = this.getShort(position);
            position += 2;
        }

        return result;
    }

    public int[] getInts(int position, int length) {
        int[] result = new int[length];

        for(int i = 0; i < length; ++i) {
            result[i] = this.getInt(position);
            position += 4;
        }

        return result;
    }

    public long[] getLongs(int position, int length) {
        long[] result = new long[length];

        for(int i = 0; i < length; ++i) {
            result[i] = this.getLong(position);
            position += 8;
        }

        return result;
    }

    public char[] getChars(int position, int length) {
        char[] result = new char[length];

        for(int i = 0; i < length; ++i) {
            result[i] = this.getChar(position);
            position += 2;
        }

        return result;
    }

    public float[] getFloats(int position, int length) {
        float[] result = new float[length];

        for(int i = 0; i < length; ++i) {
            result[i] = this.getFloat(position);
            position += 4;
        }

        return result;
    }

    public double[] getDoubles(int position, int length) {
        double[] result = new double[length];

        for(int i = 0; i < length; ++i) {
            result[i] = this.getDouble(position);
            position += 8;
        }

        return result;
    }

    public boolean equals(Object o) {
        if(this == o) {
            return true;
        } else if(o == null) {
            return false;
        } else if(!(o instanceof StaticBuffer)) {
            return false;
        } else {
            StaticBuffer b = (StaticBuffer)o;
            return this.length() != b.length()?false:this.compareTo(b) == 0;
        }
    }

    public int hashCode() {
        return this.hashCode(this.length());
    }

    protected int hashCode(int length) {
        Preconditions.checkArgument(length <= this.length());
        int result = 17;

        for(int i = 0; i < length; ++i) {
            result = 31 * result + this.array.get(i);
        }

        return result;
    }

    public String toString() {
        StringBuilder s = new StringBuilder();

        for(int i = 0; i < length(); ++i) {
            if(i > 0) {
                s.append("-");
            }

            s.append(toFixedWidthString(this.array.get(i)));
        }

        return s.toString();
    }

    private static String toString(byte b) {
        return String.valueOf(b >= 0?b:256 + b);
    }

    private static String toFixedWidthString(byte b) {
        String s = toString(b);

        assert s.length() <= 3 && s.length() > 0;

        if(s.length() == 1) {
            s = "  " + s;
        } else if(s.length() == 2) {
            s = " " + s;
        }

        return s;
    }

    public int compareTo(StaticBuffer other) {
        throw new RuntimeException("Not implemented");
    }

    public int valuePosition() {
        return this.array.valuePosition();
    }
}
