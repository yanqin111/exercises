import java.io.Serializable;

/**
 * 玩家游戏次数
 * @author page
 */
public class PlayerTimes implements Serializable {
    /**
     * qqhao
     */
    private String qqId;

    /**
     * 游戏id
     */
    private String playerId;

    /**
     * 对局总次数
     */
    private Long count;

    public PlayerTimes() {
        this.count = 0L;
        this.qqId = "";
        this.playerId = "";
    }

    ;

    public PlayerTimes(String qqId, String playerId, Long count) {
        this.qqId = qqId;
        this.playerId = playerId;
        this.count = count;
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

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
