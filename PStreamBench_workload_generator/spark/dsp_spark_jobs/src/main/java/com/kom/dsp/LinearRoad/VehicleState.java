package com.kom.dsp.LinearRoad;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-vehicleId state shared by the flatMapGroupsWithState implementations
 * of queries 1, 3, and 4 (toll notification, daily expenditure, vehicle
 * report), covering what each of TollNotificationMapper.java /
 * DailyExpenditureCalculator.java / VehicleReportsMapper.java tracks.
 *
 * Note on segmentVehicles: in the Flink source this is a MapState read
 * inside an operator that is *itself* keyed by vehicleId (via
 * `.keyBy(event -> event.vehicleId)`), so each vehicle's copy of this map
 * only ever observes that same vehicle's own events -- other vehicles'
 * entries are never visible to it, since Flink keyed state is strictly
 * per-key. In effect "average speed / vehicle count in the segment" always
 * resolves to that one vehicle's own speed and a count of 1. That looks
 * like an unintentional consequence of using per-key MapState to model
 * cross-vehicle segment occupancy, not a deliberate design -- but this
 * mirrors the actual (siloed) behavior rather than a hypothetical fixed
 * version, since the actual behavior is what would show up in a live
 * comparison against the Flink run.
 */
public class VehicleState implements Serializable {
    public List<VehicleEvent> recentEvents = new ArrayList<>();
    public Map<Integer, List<VehicleEvent>> segmentVehicles = new HashMap<>();
    public long totalExpenditure = 0;
    public Long firstEventTime = null;
}
