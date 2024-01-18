
import java.sql.Date;


/**
 * @Auther: wxf
 * @Date: 2023/12/14 16:45:57
 * @Description: Table
 * @Version 1.0.0
 */
public class Table {

    private Integer user_id;
    private String message;
    private Date timestamp;
    private Double metric;

    public Table(Integer user_id, String message, Date timestamp, Double metric) {
        this.user_id = user_id;
        this.message = message;
        this.timestamp = timestamp;
        this.metric = metric;
    }

    public Integer getUser_id() {
        return user_id;
    }

    public String getMessage() {
        return message;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public Double getMetric() {
        return metric;
    }

    @Override
    public String toString() {
        return "Table{" +
                "user_id=" + user_id +
                ", message='" + message + '\'' +
                ", timestamp=" + timestamp +
                ", metric=" + metric +
                '}';
    }
}