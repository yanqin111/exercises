import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Aggregator;

/**
 * 找到使用次数最多的游戏号
 * @author page
 */
public class CommonPlayerId extends Aggregator<Row, PlayerTimes, PlayerTimes> {

    @Override
    public PlayerTimes zero() {
        return new PlayerTimes("", "", 0L);
    }

    @Override
    public PlayerTimes reduce(PlayerTimes playerTimes, Row row) {
        playerTimes.setQqId(row.getString(0));
        playerTimes.setPlayerId(row.getString(1));
        playerTimes.setCount(row.getLong(2) + 1);
        return playerTimes;
    }

    @Override
    public PlayerTimes merge(PlayerTimes buf1, PlayerTimes buf2) {
        if (buf1.getCount() > buf2.getCount()) {
            return buf1;
        }
        return buf2;
    }

    @Override
    public PlayerTimes finish(PlayerTimes playerTimes) {
        return playerTimes;
    }

    @Override
    public Encoder<PlayerTimes> bufferEncoder() {
        return Encoders.bean(PlayerTimes.class);
    }

    @Override
    public Encoder<PlayerTimes> outputEncoder() {
        return Encoders.bean(PlayerTimes.class);
    }
}
