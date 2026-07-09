package com.kom.dsp.LinearRoad;

import java.io.Serializable;

public class VehicleEvent implements Serializable {
    private int type;
    private long time;
    private int vehicleId;
    private int speed;
    private int xway;
    private int lane;
    private int direction;
    private int segment;
    private int position;

    public VehicleEvent() {
    }

    public VehicleEvent(int type, long time, int vehicleId, int speed, int xway, int lane,
                         int direction, int segment, int position) {
        this.type = type;
        this.time = time;
        this.vehicleId = vehicleId;
        this.speed = speed;
        this.xway = xway;
        this.lane = lane;
        this.direction = direction;
        this.segment = segment;
        this.position = position;
    }

    public int getType() { return type; }
    public void setType(int type) { this.type = type; }
    public long getTime() { return time; }
    public void setTime(long time) { this.time = time; }
    public int getVehicleId() { return vehicleId; }
    public void setVehicleId(int vehicleId) { this.vehicleId = vehicleId; }
    public int getSpeed() { return speed; }
    public void setSpeed(int speed) { this.speed = speed; }
    public int getXway() { return xway; }
    public void setXway(int xway) { this.xway = xway; }
    public int getLane() { return lane; }
    public void setLane(int lane) { this.lane = lane; }
    public int getDirection() { return direction; }
    public void setDirection(int direction) { this.direction = direction; }
    public int getSegment() { return segment; }
    public void setSegment(int segment) { this.segment = segment; }
    public int getPosition() { return position; }
    public void setPosition(int position) { this.position = position; }
}
