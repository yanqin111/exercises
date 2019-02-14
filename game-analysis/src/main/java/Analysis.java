import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * @author page
 */
public class Analysis {


    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Test")
                .master("local[1]")
                .getOrCreate();

        //读取并解析数据
        Encoder<GameLog> logEncoder = Encoders.bean(GameLog.class);
        Dataset<String> dataDS = spark.read().textFile("./src/main/resources/data.txt");
        Dataset<GameLog> logDS = dataDS.map((MapFunction<String, GameLog>) r -> new GameLog(r), logEncoder);


        //找出某个玩家擅长于使用哪些英雄(角色)---------------------------------------
        //玩家使用次数最多的游戏号中得分最高的英雄为该玩家最擅长的英雄（规避小号得分高的问题）

        //找出玩家使用次数最多的游戏号
        CommonPlayerId commonPlayerId = new CommonPlayerId();
        TypedColumn<Row, PlayerTimes> commonPlayerIdType = commonPlayerId.toColumn().name("commonPlayerId");
        Dataset<PlayerTimes> commonPlayerDS = logDS.groupBy(col("qqId"), col("playerId"))
                .count()
                .select(col("qqId"), col("playerId"), col("count").as("times"))
                .select(commonPlayerIdType);

        List<String> linkCol = new ArrayList<>();
        linkCol.add("qqId");
        linkCol.add("playerId");
        Seq<String> linkSeq = JavaConverters.asScalaIteratorConverter(linkCol.iterator()).asScala().toSeq();


        //玩家擅长英雄为平均得分最高的角色英雄
        Dataset<Row> goodAtRoleDS = logDS.join(commonPlayerDS, linkSeq)
                .select(col("qqId"), col("roleId"), col("score"))
                .groupBy(col("qqId"), col("roleId"))
                .avg("score").withColumnRenamed("avg(score)", "avgScore")
                .orderBy(desc("avgScore"))
                .select("qqId", "roleId", "avgScore");

        //玩家擅长英雄降序输出
        goodAtRoleDS.show();


        //擅长于玩哪个游戏------------------------------------------------------------
        //根据游戏胜率
        Dataset<Row> allRecordDS = logDS.select(col("qqId"), col("playerId"), col("isVictory"), col("costTime"));
        //找出玩家游戏号中得胜的次数
        Dataset<Row> victoryRecordDS = allRecordDS.select(col("qqId"), col("playerId"), col("isVictory"))
                .groupBy(col("qqId"), col("playerId"))
                .sum("isVictory")
                .withColumnRenamed("sum(isVictory)", "victory");

        //玩家游戏花费的总时间权重得分： 总时间到100小时为上限(40分)
        Encoder<GameTime> timeEncoder = Encoders.bean(GameTime.class);
        Dataset<Row> sumTimeWeightDS = allRecordDS.select(col("qqId"), col("playerId"), col("costTime"))
                .groupBy(col("qqId"), col("playerId"))
                .sum("costTime")
                .withColumnRenamed("sum(costTime)", "sumTime")
                .select(col("qqId"), col("playerId"), col("sumTime"))
                .map(row -> {
                    GameTime gt = new GameTime();
                    gt.setQqId(row.getString(0));
                    gt.setPlayerId(row.getString(1));
                    Long sumTime = row.getLong(2);
                    int maxTime = 100 * 60 * 60;
                    if (sumTime > maxTime) {
                        gt.setTimeValue(40.0);
                    } else {//小于100小时，计算权重系数分
                        double value = sumTime / maxTime * 40;
                        gt.setTimeValue(value);
                    }
                    return gt;
                }, timeEncoder)
                .withColumnRenamed("timeValue", "timeWeight");


        //游戏胜率权重得分： 100%胜率得分60
        Dataset<Row> allCountDS = allRecordDS.groupBy(col("qqId"), col("playerId")).count();
        Dataset<Row> winRateWeightDS = allCountDS.join(victoryRecordDS, linkSeq)
                .select(col("qqId"), col("playerId"),
                        (col("victory").divide(col("count")).multiply(60).as("winRateWeight"))
                );

        //合并时间权重和胜率权重为总参考系数value，并降序排序
        Dataset<Row> goodAtGameDS = winRateWeightDS.join(sumTimeWeightDS, linkSeq)
                .select(col("qqId"), col("playerId"), col("winRateWeight").plus(col("timeWeight")).as("value"))
                .orderBy(col("value").desc())
                .select(col("qqId"), col("playerId"), col("value"));

        //擅长游戏id降序输出
        goodAtGameDS.show();


        //与哪些队友开黑胜率高-----------------------------------------------------
        Encoder<TeammateRecord> TeammateEncoder = Encoders.bean(TeammateRecord.class);
        List<String> joinCol = new ArrayList<>();
        joinCol.add("playerId");
        joinCol.add("teammateId");
        Seq<String> joinSeq = JavaConverters.asScalaIteratorConverter(joinCol.iterator()).asScala().toSeq();

        //筛选常开合出队友数据，一起对局次数大于5次的可认为为常开黑队友
        Dataset<TeammateRecord> teamLogDS = logDS.flatMap(r -> r.getTeamList(), TeammateEncoder);
        Dataset<Row> teammateDS = teamLogDS.groupBy(col("playerId"), col("teammateId"))
                .count()
                .where(col("count").geq(5))
                .select(col("playerId"), col("teammateId"));


        //常开黑队友数据
        Dataset<Row> blackTeamDS = teamLogDS.join(teammateDS, joinSeq)
                .select(col("playerId"), col("teammateId"), col("score"), col("costTime"), col("isVictory"));

        //计算与队友的胜率,胜率占总参考值系数分60
        Dataset<Row> blackGameDS = blackTeamDS.select(col("playerId"), col("teammateId"), col("isVictory"));

        Dataset<Row> victoryTeamDS = blackGameDS
                .groupBy("playerId", "teammateId")
                .sum("isVictory")
                .withColumnRenamed("sum(isVictory)", "victoryCount");

        //计算胜率权重得分
        Dataset<Row> winRateDS = blackGameDS.groupBy("playerId", "teammateId").count()
                .join(victoryTeamDS, joinSeq)
                .select(col("playerId"), col("teammateId"),
                        col("victoryCount").divide(col("count")).multiply(60).as("winWeight")
                );


        //败局的最长时间(时间权重系数0分)
        Long maxCostTime = logDS.filter(col("isVictory").equalTo(1))
                .select(col("costTime"))
                .select(max("costTime"))
                .first()
                .getLong(0);
        //胜局的最短时间(时间权重系数满分)
        Long minCostTime = logDS.filter(col("isVictory").equalTo(0))
                .select(col("costTime"))
                .select(min("costTime"))
                .first()
                .getLong(0);

        Long windowTime = maxCostTime - minCostTime;


        //最高的表现分（由于不知道游戏具体的最高表现分，以全部数据的最高表现分为权重系数满分）
        Double maxScore = logDS
                .select(col("score"))
                .select(max("score"))
                .first()
                .getDouble(0);


        Encoder<BlackState> blackStateEncoder = Encoders.bean(BlackState.class);
        Dataset<BlackState> weightDS = teamLogDS.select(col("playerId"), col("teammateId"), col("costTime"), col("score"))
                .map(row -> {
                    BlackState weight = new BlackState();
                    weight.setPlayerId(row.getString(0));
                    weight.setTeammateId(row.getString(1));

                    //计算对局时间参考系数分，满分为15分
                    Long costTime = row.getLong(2);
                    if (costTime > windowTime) {//超过最大时间为0分
                        weight.setTimeWeight(0);
                    } else {//在时间范围之内，计算权重值
                        BigDecimal time = new BigDecimal(costTime);
                        BigDecimal windowTimeBig = new BigDecimal(windowTime);
                        BigDecimal divideTime = time.divide(windowTimeBig, 4, RoundingMode.HALF_UP);
                        double timeWeight = 15 - divideTime.doubleValue() * 15;
                        weight.setTimeWeight(timeWeight);
                    }


                    //表现分参考系数分，满分为25
                    BigDecimal score = new BigDecimal(row.getDouble(3));
                    BigDecimal windowScore = new BigDecimal(maxScore);
                    BigDecimal divideScore = score.divide(windowScore, 4, RoundingMode.HALF_UP);
                    double scoreWeight = divideScore.doubleValue() * 25;
                    weight.setScoreWeight(scoreWeight);

                    return weight;
                }, blackStateEncoder);


        //合并参考系数分：胜率权重+时间权重+表现分权重
        Dataset<Row> goodTeamDS = weightDS.join(winRateDS, joinSeq)
                .select(col("playerId"), col("teammateId"), col("winWeight").plus(col("scoreWeight")).plus(col("timeWeight")).as("value"))
                .orderBy(col("value").desc())
                .select(col("playerId"), col("teammateId"), col("value"));

        //适合的队友降序输出
        goodTeamDS.show();


        spark.stop();
    }


}
