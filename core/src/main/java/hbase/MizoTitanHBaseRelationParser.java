package hbase;

import com.thinkaurelius.titan.diskstorage.util.ReadArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.StringSerializer;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.types.system.BaseKey;
import com.thinkaurelius.titan.graphdb.types.system.BaseLabel;
import com.thinkaurelius.titan.hadoop.formats.util.input.SystemTypeInspector;
import com.thinkaurelius.titan.util.stats.NumberUtil;
import hbase.patches.MizoReadArrayBuffer;
import hbase.patches.MizoStringSerializer;
import input.IMizoRelationParser;
import org.apache.hadoop.hbase.Cell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Created by imrihecht on 12/9/16.
 */

public class MizoTitanHBaseRelationParser implements IMizoRelationParser {
    private static final Logger log =
            LoggerFactory.getLogger(MizoTitanHBaseRelationParser.class);

    /**
     * Raw value of this relation - property value, edge properties etc
     */
    protected final byte[] valueArray;

    /**
     * The type of this relation, in a Titan object
     */
    private InternalRelationType relationType;

    /**
     * The value array - as Titan buffer
     */
    private ReadArrayBuffer valueReader;

    /**
     * Offset of value in the given array
     */
    protected final int valueOffset;

    /**
     * Total length of the value
     */
    protected final int valueLength;

    /**
     * Id of the value we are parsing one of its relations
     */
    private final long vertexId;


    /**
     * Relation type ID
     */
    private long typeId;

    /**
     * Relation direction identifier
     */
    private IDHandler.DirectionID directionID;


    /**
     * Relation identifier - only if this relation is an edge
     */
    private long relationId = 0;

    /**
     * ID of the other vertex - only if this relation is an edge
     */
    private long otherVertexId = 0;

    /**
     * Titan internal object for ID reading and writing
     */

    private final static IDManager ID_MANAGER = new IDManager(NumberUtil.getPowerOf2(32));

    /**
     * Titan internal object used for reading Strings
     */
    private final static StringSerializer STRING_SERIALIZER = new StringSerializer();

    /**
     * Internal object for checking whether types are system-related
     */
    private final static SystemTypeInspector TYPES_INSPECTOR = new SystemTypeInspector() {
        @Override
        public boolean isSystemType(long typeId) {
            return IDManager.isSystemRelationTypeId(typeId);
        }

        @Override
        public boolean isVertexExistsSystemType(long typeId) {
            return typeId == BaseKey.VertexExists.longId();
        }

        @Override
        public boolean isVertexLabelSystemType(long typeId) {
            return typeId == BaseLabel.VertexLabelEdge.longId();
        }

        @Override
        public boolean isTypeSystemType(long typeId) {
            return typeId == BaseKey.SchemaCategory.longId() ||
                    typeId == BaseKey.SchemaDefinitionProperty.longId() ||
                    typeId == BaseKey.SchemaDefinitionDesc.longId() ||
                    typeId == BaseKey.SchemaName.longId() ||
                    typeId == BaseLabel.SchemaDefinitionEdge.longId();
        }
    };

    /**
     * Mapping between relation type IDs and relation names
     * If a property - its name, if an edge - its label
     */
    private final HashMap<Long, InternalRelationType> relationTypes;

    public MizoTitanHBaseRelationParser(HashMap<Long, InternalRelationType> relationTypes, Cell cell) {
        this.relationTypes = relationTypes;
        this.valueArray = cell.getValueArray();
        this.valueOffset = cell.getValueOffset();
        this.valueLength = cell.getValueLength();

        this.vertexId = parseVertexId(cell.getRowArray(), cell.getRowOffset(),
                cell.getRowLength());
        extractQualifier(cell.getQualifierArray(), cell.getQualifierOffset(),
                cell.getQualifierLength());

        if (!isSystemType() && !isKnownType()) {
            log.warn("Unknown relation type (vertex-id={}, type-id={})",
                    this.vertexId, this.typeId);
        }
    }

    /**
     * Extract data from the qualifier part of an HBase Cell
     *
     * @param qualifierArray  Cell array, containing the Qualifier part in an offset
     * @param qualifierOffset The offset of the Qualifier part within the given Cell array
     * @param qualifierLength The length of the qualifier within the Cell array
     */
    private void extractQualifier(byte[] qualifierArray, int qualifierOffset, int qualifierLength) {
        ReadArrayBuffer qualifierBuffer = new MizoReadArrayBuffer(qualifierArray,
                qualifierOffset, qualifierOffset + qualifierLength);

        IDHandler.RelationTypeParse typeAndDirection =
                IDHandler.readRelationType(qualifierBuffer);
        typeId = typeAndDirection.typeId;
        directionID = typeAndDirection.dirID;
        relationType = this.relationTypes.get(this.typeId);

        // The following code is a customized version
        // of Titan's EdgeSerializer.parseRelation
        if (!isSystemType() && isKnownType() && (isOutEdge() || isInEdge())) {
            if (relationType.multiplicity().isConstrained()) {
                if (relationType.multiplicity().isUnique(directionID.getDirection())) {
                    otherVertexId = VariableLong.readPositive(qualifierBuffer);
                } else {
                    qualifierBuffer.movePositionTo(qualifierBuffer.length());
                    otherVertexId = VariableLong.readPositiveBackward(qualifierBuffer);
                }
            } else {
                qualifierBuffer.movePositionTo(qualifierBuffer.length());

                relationId = VariableLong.readPositiveBackward(qualifierBuffer);
                otherVertexId = VariableLong.readPositiveBackward(qualifierBuffer);
            }
        }
    }

    /**
     * Parses the vertex id from the row array
     * This will be the same for cells that are under the same HBase row
     */
    private static long parseVertexId(byte[] rowArray, int rowOffset, int rowLength) {
        return ID_MANAGER.getKeyID(new StaticArrayBuffer(rowArray, rowOffset, rowOffset + rowLength));
    }

    private ReadArrayBuffer getValueReader() {
        if (valueReader == null) {
            valueReader = new MizoReadArrayBuffer(valueArray, valueOffset, valueOffset + valueLength);

            if (isRelatonIdInValue()) {
                relationId = VariableLong.readPositive(valueReader); // critical - skips the relation-id in the beginning of value buffer
            }
        }
        return valueReader;
    }


    private boolean isRelatonIdInValue() {
        return !isSystemType() && isKnownType() && (isOutEdge() || isInEdge()) &&
                relationType.multiplicity().isConstrained();
    }

    @Override
    public String readPropertyValue() {
        return STRING_SERIALIZER.read(getValueReader());
    }

    @Override
    public void skipPropertyValue() {
        ReadArrayBuffer buffer = getValueReader();
        long length = VariableLong.readPositive(buffer);

        if (length == 0) return;

        long compressionId = length & MizoStringSerializer.COMPRESSOR_BIT_MASK;
        assert compressionId < MizoStringSerializer.MAX_NUM_COMPRESSORS;
        MizoStringSerializer.CompressionType compression =
                MizoStringSerializer.CompressionType.getFromId((int) compressionId);
        length = (length >>> MizoStringSerializer.COMPRESSOR_BIT_LEN);

        if (compression == MizoStringSerializer.CompressionType.NO_COMPRESSION) {
            if ((length & 1) == 0) { //ASCII encoding
                length = length >>> 1;
                if (length == 1)
                    return;
                if (length == 2) {
                    while (((0xFF & buffer.getByte()) & 0x80) <= 0) ;
                } else throw new IllegalArgumentException("Invalid ASCII encoding offset: " + length);
            } else {
                length = length >>> 1;
                assert length > 0 && length <= Integer.MAX_VALUE;
                for (int i = 0; i < length; i++) {
                    int b = (buffer.getByte() & 0xFF) >> 4;

                    // fuck you titan
                    if (b >= 12 && b <= 14) {
                        buffer.movePositionTo(buffer.getPosition() + 1);

                        if (b == 14) {
                            buffer.movePositionTo(buffer.getPosition() + 1);
                        }
                    }
                }
            }
        } else {
            assert length <= Integer.MAX_VALUE;
            buffer.movePositionTo(buffer.getPosition() + (int) length);
        }
    }

    @Override
    public String readPropertyName() {
        long propertyTypeId = IDHandler.readInlineRelationType(getValueReader());
        String propertyName = this.relationTypes.get(propertyTypeId).name();

        if (propertyName == null) {
            log.warn("Unknown property relation type (vertex-id={}, property-type-id={}, relation-id={}, other-vertex-id={})",
                    this.vertexId, propertyTypeId, this.relationId, this.otherVertexId);
        }

        return propertyName;
    }

    @Override
    public boolean valueHasRemaining() {
        return getValueReader().hasRemaining();
    }


    /**
     * It is a type related to system?
     */
    @Override

    public boolean isSystemType() {
        return TYPES_INSPECTOR.isVertexLabelSystemType(typeId) || // we do not use vertex labels - can skip them
                TYPES_INSPECTOR.isSystemType(typeId) ||
                TYPES_INSPECTOR.isVertexExistsSystemType(typeId) ||
                TYPES_INSPECTOR.isTypeSystemType(typeId);
    }

    /**
     * Whether this relation type ID is not in the known relation-types mapping
     */
    @Override
    public boolean isKnownType() {
        return this.relationType != null;
    }

    @Override
    public long getTypeId() {
        return typeId;
    }

    @Override
    public String getTypeName() {
        return this.relationType.name();
    }

    @Override
    public long getRelationId() {
        // if multiplicity is constrainted, the relation-id is stored on the value buffer
        // so by calling getValueBuffer, the relation-id will be set
        if (relationId == 0 && isKnownType() && relationType.multiplicity().isConstrained()) {
            getValueReader();
        }

        return relationId;
    }

    @Override
    public long getVertexId() {
        return vertexId;
    }

    @Override
    public long getOtherVertexId() {
        return otherVertexId;
    }

    @Override
    public boolean isProperty() {
        return directionID.equals(IDHandler.DirectionID.PROPERTY_DIR);
    }

    @Override
    public boolean isOutEdge() {
        return directionID.equals(IDHandler.DirectionID.EDGE_OUT_DIR);
    }

    @Override
    public boolean isInEdge() {
        return directionID.equals(IDHandler.DirectionID.EDGE_IN_DIR);
    }


}

