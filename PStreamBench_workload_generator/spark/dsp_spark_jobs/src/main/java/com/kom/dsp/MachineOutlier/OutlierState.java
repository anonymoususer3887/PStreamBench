package com.kom.dsp.MachineOutlier;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Per-machineId state for MachineOutlier's flatMapGroupsWithState. */
public class OutlierState implements Serializable {
    public List<MachineUsage> usages = new ArrayList<>();
}
