package com.kom.dsp.MachineOutlier;

import java.io.Serializable;

/**
 * JavaBean mirror of flink/dsp_flink_jobs/.../MachineOutlier/MachineUsage.java
 * (public no-arg constructor + get/set pairs, required for Spark's
 * Encoders.bean() to serialize it across the flatMapGroupsWithState shuffle).
 */
public class MachineUsage implements Serializable {

    private String machineId;
    private double timestamp;
    private double cpuUtilPercentage;
    private double memUtilPercentage;
    private double memBW;
    private double missPerThousandInstructions;
    private double netIn;
    private double netOut;
    private double diskIO;

    public MachineUsage() {
    }

    public MachineUsage(String machineId, double timestamp, double cpuUtilPercentage,
                         double memUtilPercentage, double memBW, double missPerThousandInstructions,
                         double netIn, double netOut, double diskIO) {
        this.machineId = machineId;
        this.timestamp = timestamp;
        this.cpuUtilPercentage = cpuUtilPercentage;
        this.memUtilPercentage = memUtilPercentage;
        this.memBW = memBW;
        this.missPerThousandInstructions = missPerThousandInstructions;
        this.netIn = netIn;
        this.netOut = netOut;
        this.diskIO = diskIO;
    }

    public String getMachineId() { return machineId; }
    public void setMachineId(String machineId) { this.machineId = machineId; }
    public double getTimestamp() { return timestamp; }
    public void setTimestamp(double timestamp) { this.timestamp = timestamp; }
    public double getCpuUtilPercentage() { return cpuUtilPercentage; }
    public void setCpuUtilPercentage(double v) { this.cpuUtilPercentage = v; }
    public double getMemUtilPercentage() { return memUtilPercentage; }
    public void setMemUtilPercentage(double v) { this.memUtilPercentage = v; }
    public double getMemBW() { return memBW; }
    public void setMemBW(double v) { this.memBW = v; }
    public double getMissPerThousandInstructions() { return missPerThousandInstructions; }
    public void setMissPerThousandInstructions(double v) { this.missPerThousandInstructions = v; }
    public double getNetIn() { return netIn; }
    public void setNetIn(double v) { this.netIn = v; }
    public double getNetOut() { return netOut; }
    public void setNetOut(double v) { this.netOut = v; }
    public double getDiskIO() { return diskIO; }
    public void setDiskIO(double v) { this.diskIO = v; }

    /** Mirrors MachineUsage.getEuclideanDistance() exactly. */
    public double getEuclideanDistance(MachineUsage other) {
        double cpuUtilDiff = this.cpuUtilPercentage - other.cpuUtilPercentage;
        double memUtilDiff = this.memUtilPercentage - other.memUtilPercentage;
        double memGpsDiff = this.memBW - other.memBW;
        double mkpiDiff = this.missPerThousandInstructions - other.missPerThousandInstructions;
        double netInDiff = this.netIn - other.netIn;
        double netOutDiff = this.netOut - other.netOut;
        double diskIoDiff = this.diskIO - other.diskIO;

        double sumOfSquaredDifferences = cpuUtilDiff * cpuUtilDiff +
                memUtilDiff * memUtilDiff +
                memGpsDiff * memGpsDiff +
                mkpiDiff * mkpiDiff +
                netInDiff * netInDiff +
                netOutDiff * netOutDiff +
                diskIoDiff * diskIoDiff;

        return Math.sqrt(sumOfSquaredDifferences);
    }
}
