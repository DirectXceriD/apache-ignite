package org.apache.ignite.examples.loader;

/**
 * ���� ��� �������� ����������� �� � IMDG
 *
 * Created by SBT-Danilyuk-YUS on 01.09.2016.
 */
public class AffinityParticleKey implements Comparable<AffinityParticleKey> {

    // ����� �����
    public static final String ROOT_ID_FIELD_NAME = "rootId";
    public static final String ID_FIELD_NAME = "id";
    public static final String PARTITION_ID_FIELD_NAME = "partitionId";

    /*
    ������������� ������������ ��
     */
    private long id;
    /*
    ������������� �������� ��
     */
    private long partitionId;
    /*
    ������������� ��������� �� �������� ���
     */
    private long rootId;

    /**
     * �������� ������������� ������������ ��
     *
     * @return ������������� ������������ ��
     */
    public long getId() {
        return id;
    }

    /**
     * �������� ������������� �������� ������������ ��
     *
     * @return ������������� �������� ������������ ��
     */
    public long getPartitionId() {
        return partitionId;
    }

    /**
     * �������� ������������� ��������� �� �������� ���
     *
     * @return ������������� ��������� �� �������� ���
     */
    public long getRootId() {
        return rootId;
    }

    public AffinityParticleKey(long id, long partitionId, long rootId) {
        this.id = id;
        this.partitionId = partitionId;
        this.rootId = rootId;
    }

    @Override
    public int compareTo(AffinityParticleKey o) {
        if (this.id == o.id) {
            return 0;
        }
        //TODO ������� ����� ������������ (������� partitionId) ParticleKey; ����� ������ ������� ������ �� ParticleKey
        if (o.id > this.id) {
            return 1;
        }
        else {
            return -1;
        }
    }

    @Override
    public String toString() {
        return String.format("AffinityParticleKey{id=%d, partitionId=%d, rootId=%d}",
            id, partitionId, rootId);
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        AffinityParticleKey key = (AffinityParticleKey)o;

        if (id != key.id)
            return false;
        if (partitionId != key.partitionId)
            return false;
        return rootId == key.rootId;

    }

    @Override public int hashCode() {
        int result = (int)(id ^ (id >>> 32));
        result = 31 * result + (int)(partitionId ^ (partitionId >>> 32));
        result = 31 * result + (int)(rootId ^ (rootId >>> 32));
        return result;
    }
}
