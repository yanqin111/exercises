package com.yq.exercises.task.decompose.woker;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;

/**
 * Worker状态
 * @author page
 */
@SuppressWarnings("serial")
public class WorkerStatus implements Serializable, Comparable<WorkerStatus> {

    /**
     * 唯一标识
     */
    public String workerId;

    /**
     * 队列大小
     */
    public int queueSize;

    /**
     * 线程池大小
     */
    public int coreSize;

    /**
     * 空闲队列
     */
    public int freeQueue;

    /**
     * 空闲线程
     */
    public int freeCore;

    /**
     * 上报时间
     */
    public long time;

    /**
     * worker的空闲值
     * 线程空闲+队列空闲
     *
     * @return
     */
    public int getFreeValue() {
        return 0;
    }



    @Override
    public int compareTo(WorkerStatus o1) {
        return (o1.freeQueue / o1.coreSize) - (this.freeQueue / this.coreSize);
    }


    @Override
    public String toString() {
        return "WorkerStatus{" +
                "workerId='" + workerId + '\'' +
                ", queueSize=" + queueSize +
                ", coreSize=" + coreSize +
                ", freeQueue=" + freeQueue +
                ", freeCore=" + freeCore +
                ", time=" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time)) +
                '}';
    }
}
