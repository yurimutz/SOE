package parte3;

import java.util.Random;

public class LocationSimulator {

    private final Random random = new Random();

    public void updateLocation(Student student) {

        double deltaLat = (random.nextDouble() - 0.5) / 1000;
        double deltaLon = (random.nextDouble() - 0.5) / 1000;

        student.setLatitude(student.getLatitude() + deltaLat);
        student.setLongitude(student.getLongitude() + deltaLon);
    }
}