package udc.fic.tfg;

import org.apache.spark.Partitioner;

public class SeqPartitioner extends Partitioner {
    private double partitionDiv;
    private int partitions;

    public SeqPartitioner(int columns, int partitions) {
        this.partitionDiv = (double) columns / partitions;
        this.partitions = partitions;
    }

    @Override
    public int numPartitions() {
        return partitions;
    }

    @Override
    public int getPartition(Object key) {
        Integer intKey = (Integer) key;
        return (int) Math.floor(intKey / partitionDiv);
    }
}
