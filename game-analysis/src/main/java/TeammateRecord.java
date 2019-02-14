import java.io.Serializable;

/**
 * 队友游戏记录
 * @author page
 */
public class TeammateRecord implements Serializable {

    /**
     * 游戏id
     */
    private String playerId;

    /**
     * 队友id
     */
    private String teammateId;

    /**
     * 对局花费时间
     */
    private Long costTime;

    /**
     * 表现分
     */
    private double score;

    /**
     * 胜负
     */
    private int isVictory;


    public TeammateRecord( String playerId, String teammateId, Long costTime, double score, int isVictory) {
        this.playerId = playerId;
        this.teammateId = teammateId;
        this.costTime = costTime;
        this.score = score;
        this.isVictory = isVictory;
    }

    public int getIsVictory() {
        return isVictory;
    }

    public void setIsVictory(int isVictory) {
        this.isVictory = isVictory;
    }

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

    public Long getCostTime() {
        return costTime;
    }

    public void setCostTime(Long costTime) {
        this.costTime = costTime;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}
