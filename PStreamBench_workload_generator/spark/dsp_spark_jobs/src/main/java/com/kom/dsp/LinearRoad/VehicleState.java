package com.kom.dsp.LinearRoad;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VehicleState implements Serializable {
    public List<VehicleEvent> recentEvents = new ArrayList<>();
    public Map<Integer, List<VehicleEvent>> segmentVehicles = new HashMap<>();
    public long totalExpenditure = 0;
    public Long firstEventTime = null;
}
