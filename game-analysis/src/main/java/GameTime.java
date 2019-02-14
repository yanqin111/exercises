import java.io.Serializable;

/**
 * @author page
 */
public class GameTime implements Serializable {

    private String qqId;

    private String playerId;

    private double timeValue;

    public GameTime() {
    }

    public GameTime(String qqId, String playerId, double timeValue) {
        this.qqId = qqId;
        this.playerId = playerId;
        this.timeValue = timeValue;
    }

    public String getQqId() {
        return qqId;
    }

    public void setQqId(String qqId) {
        this.qqId = qqId;
    }

    public String getPlayerId() {
        return playerId;
    }

    public void setPlayerId(String playerId) {
        this.playerId = playerId;
    }

    public double getTimeValue() {
        return timeValue;
    }

    public void setTimeValue(double timeValue) {
        this.timeValue = timeValue;
    }
}
