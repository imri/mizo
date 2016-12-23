package mizo.hbase.patches;

/**
 * Created by imrihecht on 12/23/16.
 */
public class MizoVirtualArray {
    public byte[] array;
    private final int size;

    private int offsetKey;
    private int limitKey;
    private int offsetValue;
    private int limitValue;
    private final int sizeKey;
    private final int sizeValue;

    public MizoVirtualArray(byte[] array, int offsetKey, int limitKey, int offsetValue, int limitValue) {
        this.array = array;

        this.offsetKey = offsetKey;
        this.limitKey = limitKey;
        this.offsetValue = offsetValue;
        this.limitValue = limitValue;

        this.sizeKey = this.limitKey - this.offsetKey;
        this.sizeValue = this.limitValue - this.offsetValue;
        this.size = this.sizeKey + this.sizeValue;
    }

    public MizoVirtualArray(MizoVirtualArray virtualArray) {
        this(virtualArray.array, virtualArray.offsetKey, virtualArray.limitKey, virtualArray.offsetValue, virtualArray.limitValue);
    }

    public byte get(int position) {
        if (position < this.sizeKey)
            return array[this.offsetKey + position];

        if ((position - this.sizeKey) < this.sizeValue)
            return array[this.offsetValue + position - this.sizeKey];

        throw new ArrayIndexOutOfBoundsException("Position [" + position + "] is out of range");
    }

    public int require(int position, int size) {
        if(position < 0) {
            throw new ArrayIndexOutOfBoundsException("Position [" + position + "] must be non-negative");
        } else if(position + size > this.size) {
            throw new ArrayIndexOutOfBoundsException("Required size [" + size + "] " + " exceeds actual remaining size");
        } else {
            return position;
        }
    }

    public int length() {
        return this.size;
    }

    public int valuePosition() {
        return sizeKey;
    }

    public MizoVirtualArray subrange(int position, int length) {
        if (position > this.sizeKey) {
            return new MizoVirtualArray(this.array,
                    0,
                    0,
                    this.offsetValue + (position - this.sizeKey),
                    this.offsetValue + (position - this.sizeKey) + length);
        }

        if (position + length <= this.sizeKey) {
            return new MizoVirtualArray(this.array,
                    this.offsetKey + position,
                    this.offsetKey + position + length,
                    0,
                    0);
        }

        return new MizoVirtualArray(this.array,
                offsetKey + position,
                limitKey,
                offsetValue,
                offsetValue + length - sizeKey);
    }
}
