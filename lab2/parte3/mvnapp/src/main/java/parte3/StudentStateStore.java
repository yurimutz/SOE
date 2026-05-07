package parte3;

import java.util.HashMap;
import java.util.Map;

public class StudentStateStore {

    private final Map<String, Student> currentState = new HashMap<>();
    private final Map<String, double[]> lastLocation = new HashMap<>();

    public void update(Student student) {

        String id = student.getId();

        Student previous = currentState.get(id);

        if (previous != null) {
            double prevLat = previous.getLatitude();
            double prevLon = previous.getLongitude();

            double distance = distance(prevLat, prevLon,
                    student.getLatitude(), student.getLongitude());

            System.out.println("Student " + id +
                    " moved " + String.format("%.5f", distance));
        }

        currentState.put(id, student);
    }

    // simple distance (not geo-accurate, but fine for simulation)
    //distância euclidiana (linha reta), sem considerar a curvatura da terra
    private double distance(double lat1, double lon1, double lat2, double lon2) {
        double dLat = lat2 - lat1;
        double dLon = lon2 - lon1;
        return Math.sqrt(dLat * dLat + dLon * dLon);
    }

    public void printState() {
        System.out.println("=== CURRENT STATE ===");
        for (Student s : currentState.values()) {
            System.out.println(s);
        }
    }
}