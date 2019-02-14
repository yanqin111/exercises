package com.yq.exercises.task.decompose.task;

import com.yq.exercises.task.decompose.woker.WorkerStatus;

import java.io.Serializable;
import java.util.List;

/**
 * 用来产地消息，用于master 下发计算任务执行计划图时 的下发消息
 * @author page
 */
public class WorkerAndTask implements Serializable {

    /**
     * 最新worker状态
     */
    public List<WorkerStatus> workers;

    /**
     * 待分配和分解的任务
     */
    public Task task;

    @Override
    public String toString() {
        return "WorkerAndTask{" +
                "workers=" + workers +
                ", task=" + task +
                '}';
    }
}
