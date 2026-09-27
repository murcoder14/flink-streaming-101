package org.muralis.flink.data.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

public class BusGPSEvent implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String busId;
    private String route;

    private double latitude;
    private double longitude;

    private double speedMph;
    private int passengerCount;

    private long timestamp;

    public BusGPSEvent() {}

    public BusGPSEvent(
            String busId,
            String route,
            double latitude,
            double longitude,
            double speedMph,
            int passengerCount,
            long timestamp) {

        this.busId = busId;
        this.route = route;
        this.latitude = latitude;
        this.longitude = longitude;
        this.speedMph = speedMph;
        this.passengerCount = passengerCount;
        this.timestamp = timestamp;
    }

    public String getBusId() {
        return busId;
    }

    public void setBusId(String busId) {
        this.busId = busId;
    }

    public String getRoute() {
        return route;
    }

    public void setRoute(String route) {
        this.route = route;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getSpeedMph() {
        return speedMph;
    }

    public void setSpeedMph(double speedMph) {
        this.speedMph = speedMph;
    }

    public int getPassengerCount() {
        return passengerCount;
    }

    public void setPassengerCount(int passengerCount) {
        this.passengerCount = passengerCount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BusGPSEvent that = (BusGPSEvent) o;
        return Double.compare(that.latitude, latitude) == 0
                && Double.compare(that.longitude, longitude) == 0
                && Double.compare(that.speedMph, speedMph) == 0
                && passengerCount == that.passengerCount
                && timestamp == that.timestamp
                && Objects.equals(busId, that.busId)
                && Objects.equals(route, that.route);
    }

    @Override
    public int hashCode() {
        return Objects.hash(busId, route, latitude, longitude, speedMph, passengerCount, timestamp);
    }

    @Override
    public String toString() {
        return String.format(
                "Bus %s | Route %s | Location (%.4f, %.4f) | " +
                        "Speed %.1f mph | Passengers %d",
                busId,
                route,
                latitude,
                longitude,
                speedMph,
                passengerCount
        );
    }
}