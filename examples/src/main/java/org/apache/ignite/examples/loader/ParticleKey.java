package org.apache.ignite.examples.loader;

/**
 * ������������� ����� � IMDG ��� ��
 * � ������ ���� ����������� ����� ����������
 *
 * @author Ը����� ����
 */
public class ParticleKey implements Comparable<ParticleKey> {

    // ����� �����
    public static final String PARTITION_FIELD_NAME = "partitionId";
    public static final String ID_FIELD_NAME = "id";

    /**
     * �������� ������������� ��
     */
    private long id;

    /**
     * �������� ������������� �������� ��
     */
    private long partitionId;

    /**
     * �����������
     * @param id ������������� ��
     * @param partitionId ������������� �������� ��
     */
    public ParticleKey(long id, long partitionId) {
        this.id = id;
        this.partitionId = partitionId;
    }

    /**
     * �������� ������������� ��
     * @return ������������� ��
     */
    public long getId(){
        return id;
    }

    /**
     * �������� ������������� �������� ��
     * @return ������������� �������� ��
     */
    public long getPartitionId() {
        return partitionId;
    }

    @Override
    public int compareTo(ParticleKey o) {
        if( (o.getId() == this.getId()) && (o.getPartitionId() == this.getPartitionId()) )
            return 0;
        else
        if(o.getId() > this.getId())
            return 1;
        else
            return -1;
    }

    @Override
    public String toString() {
        return "ParticleKey{" +
                "id=" + id +
                ", partitionId=" + partitionId +
                '}';
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ParticleKey key = (ParticleKey)o;

        if (id != key.id)
            return false;
        return partitionId == key.partitionId;

    }

    @Override public int hashCode() {
        int result = (int)(id ^ (id >>> 32));
        result = 31 * result + (int)(partitionId ^ (partitionId >>> 32));
        return result;
    }
}
