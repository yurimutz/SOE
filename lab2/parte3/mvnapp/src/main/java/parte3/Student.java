package parte3;

import java.io.Serializable;

public class Student implements Serializable {

    private String id;
    private String name;
    private int age;
    private String course;
    private double latitude;
    private double longitude;

    public Student() {}

    public Student(String id, String name, int age, String course, double latitude, double longitude) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.course = course;
        this.latitude = latitude;
        this.longitude = longitude;

    }

    // Getters and setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public int getAge() { return age; }
    public void setAge(int age) { this.age = age; }

    public String getCourse() { return course; }
    public void setCourse(String course) { this.course = course; }

    public void setLatitude(double latitude) { this.latitude = latitude; }
    public void setLongitude(double longitude) { this.longitude = longitude; }

    @Override
    public String toString() {
        return "Student{id='" + id + "', name='" + name +
                "', age=" + age + ", course='" + course + ", latitute='" + latitude + ", longitude='" + longitude +  "'}";
    }

    public double getLatitude() {
        return this.latitude;
    }

    public double getLongitude() {
        return this.longitude;
    }
}