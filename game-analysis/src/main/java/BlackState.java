/**
 * 与队友开黑的状况
 * @author page
 */
public class BlackState {

    /**
     * 玩家号
     */
    private String playerId;

    /**
     * 队友id
     */
    private String teammateId;

    /**
     * 表现分权重得分
     */
    private double scoreWeight;

    /**
     * 对局时间表现得分
     */
    private double timeWeight;


    public String getPlayerId() {
        return playerId;
    }

    public void setPlayerId(String playerId) {
        this.playerId = playerId;
    }

    public String getTeammateId() {
        return teammateId;
    }

    public void setTeammateId(String teammateId) {
        this.teammateId = teammateId;
    }

    public double getScoreWeight() {
        return scoreWeight;
    }

    public void setScoreWeight(double scoreWeight) {
        this.scoreWeight = scoreWeight;
    }

    public double getTimeWeight() {
        return timeWeight;
    }

    public void setTimeWeight(double timeWeight) {
        this.timeWeight = timeWeight;
    }
}
