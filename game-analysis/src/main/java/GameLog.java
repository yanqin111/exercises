import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 游戏日志
 * @author page
 */
public class GameLog implements Serializable {

    //游戏开始时间
    private Long startTime;

    //游戏结束时间
    private Long endTime;

    //玩家QQ号
    private String qqId;

    //其余玩家玩家号
    private String players;

    //角色id
    private int roleId;

    //表现分
    private double score;

    //胜负
    private int isVictory;

    //游戏号
    private String playerId;

    //本局游戏花费时间
    private Long costTime;

    public GameLog() {

    }

    /**
     * 从玩家数据中取出前4个（队友数据）
     *
     * @return
     */
    public Iterator<TeammateRecord> getTeamList() {
        List<TeammateRecord> list = new ArrayList<>();
        String[] strs = players.split(",");
        for (int i = 0; i < 4; i++) {
            TeammateRecord t = new TeammateRecord(this.playerId, strs[i], this.costTime, this.score, this.isVictory);
            list.add(t);
        }
        return list.iterator();
    }


    public GameLog(String record) {
        String[] records = record.split("\\|");
        this.startTime = Long.valueOf(records[1]);
        this.endTime = Long.valueOf(records[2]);
        this.qqId = records[3];
        this.players = records[4];
        this.roleId = Integer.valueOf(records[5]);
        this.score = Double.valueOf(records[6]);
        this.isVictory = Integer.valueOf(records[7]);
        this.playerId = records[8];
        this.costTime = this.endTime = this.startTime;
    }

    @Override
    public String toString() {
        return "GameLog{" +
                "startTime=" + startTime +
                ", endTime=" + endTime +
                ", qqId='" + qqId + '\'' +
                ", players='" + players + '\'' +
                ", roleId=" + roleId +
                ", score=" + score +
                ", isVictory=" + isVictory +
                ", playerId='" + playerId + '\'' +
                ", costTime=" + costTime +
                '}';
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public String getQqId() {
        return qqId;
    }

    public void setQqId(String qqId) {
        this.qqId = qqId;
    }

    public String getPlayers() {
        return players;
    }

    public void setPlayers(String players) {
        this.players = players;
    }

    public int getRoleId() {
        return roleId;
    }

    public void setRoleId(int roleId) {
        this.roleId = roleId;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
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

    public Long getCostTime() {
        return costTime;
    }

    public void setCostTime(Long costTime) {
        this.costTime = costTime;
    }
}
