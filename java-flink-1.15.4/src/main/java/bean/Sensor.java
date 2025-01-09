package bean;

/**
 * @Auther: wxf
 * @Date: 2024/6/7 11:04:46
 * @Description: Sensor
 * @Version 1.0.0
 */
public class Sensor {

    private Integer id;
    private String timeStamp;
    private Double temperature;

    public Sensor(Integer id, String timeStamp, Double temperature) {
        this.id = id;
        this.timeStamp = timeStamp;
        this.temperature = temperature;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "Sensor{" +
                "id=" + id +
                ", timeStamp='" + timeStamp + '\'' +
                ", temperature=" + temperature +
                '}';
    }
}