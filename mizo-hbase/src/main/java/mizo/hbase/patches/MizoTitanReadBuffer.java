package mizo.hbase.patches;

import com.thinkaurelius.titan.diskstorage.ReadBuffer;

/**
 * Created by imrihecht on 12/23/16.
 */
public class MizoTitanReadBuffer extends MizoTitanStaticBuffer implements ReadBuffer {
    private transient int position = 0;

    public MizoTitanReadBuffer(MizoVirtualArray array) {
        super(array);
    }

    private int updatePos(int update) {
        int pos = this.position;
        this.position += update;
        return pos;
    }

    public int getPosition() {
        return this.position;
    }

    public boolean hasRemaining() {
        return this.position < this.length();
    }

    public void movePositionTo(int newPosition) {
        assert newPosition >= 0 && newPosition <= this.length();

        this.position = newPosition;
    }

    public byte getByte() {
        return this.getByte(this.updatePos(1));
    }

    public boolean getBoolean() {
        return this.getBoolean(this.updatePos(1));
    }

    public short getShort() {
        return this.getShort(this.updatePos(2));
    }

    public int getInt() {
        return this.getInt(this.updatePos(4));
    }

    public long getLong() {
        return this.getLong(this.updatePos(8));
    }

    public char getChar() {
        return this.getChar(this.updatePos(2));
    }

    public float getFloat() {
        return this.getFloat(this.updatePos(4));
    }

    public double getDouble() {
        return this.getDouble(this.updatePos(8));
    }

    public byte[] getBytes(int length) {
        byte[] result = super.getBytes(this.position, length);
        this.position += length * 1;
        return result;
    }

    public short[] getShorts(int length) {
        short[] result = super.getShorts(this.position, length);
        this.position += length * 2;
        return result;
    }

    public int[] getInts(int length) {
        int[] result = super.getInts(this.position, length);
        this.position += length * 4;
        return result;
    }

    public long[] getLongs(int length) {
        long[] result = super.getLongs(this.position, length);
        this.position += length * 8;
        return result;
    }

    public char[] getChars(int length) {
        char[] result = super.getChars(this.position, length);
        this.position += length * 2;
        return result;
    }

    public float[] getFloats(int length) {
        float[] result = super.getFloats(this.position, length);
        this.position += length * 4;
        return result;
    }

    public double[] getDoubles(int length) {
        double[] result = super.getDoubles(this.position, length);
        this.position += length * 8;
        return result;
    }

    public <T> T asRelative(final Factory<T> factory) {
        throw new RuntimeException("Not implemented");
    }

    public ReadBuffer subrange(int length, boolean invert) {
        return super.subrange(this.position, length, invert).asReadBuffer();
    }
}
