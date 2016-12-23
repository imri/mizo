package mizo.hbase.patches;

/**
 * Created by imrihecht on 12/23/16.
 */
public class MizoInvertedVirtualArray extends MizoVirtualArray {
    public MizoInvertedVirtualArray(MizoVirtualArray virtualArray) {
        super(virtualArray);
    }

    @Override
    public byte get(int position) {
        return (byte)~super.get(position);
    }
}
