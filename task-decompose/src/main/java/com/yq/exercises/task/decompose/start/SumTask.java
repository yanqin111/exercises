package com.yq.exercises.task.decompose.start;

import com.yq.exercises.task.decompose.task.Task;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author page
 */
public class SumTask implements Task<Integer, Integer>, Serializable {

    private String clientId;

    private int errorFlag;

    private int begin;

    private int end;

    private int subTaskCount;

    public SumTask(String clientId, int errorFlag, int begin, int end, int subtTaskCount) {
        this.clientId = clientId;
        this.errorFlag = errorFlag;
        this.begin = begin;
        this.end = end;
        this.subTaskCount = subtTaskCount;
    }

    /**
     * 在worker中运行的方法
     *
     * @return 返回子任务的结果
     */
    @Override
    public Integer map() {
        int sum = 0;
        for (int i = begin; i < end; i++) {
            sum += begin;
        }
        return sum;
    }

    /**
     * 在master中汇集子任务结果的方法
     *
     * @param list
     * @return 返回最终结果
     */
    @Override
    public Integer reduce(List<Integer> list) {
        int sum = 0;
        for (int x : list) {
            if (x == getErrorFlag()) {
                break;
            } else {
                sum += x;
            }
        }
        return sum;
    }

    /**
     * @return 获取子任务个数
     */
    @Override
    public int getSubTaskCount() {
        return this.subTaskCount;
    }

    /**
     * @return 获取程序出错时的错误标志代码，其值不在子任务和任务的结果当中
     */
    @Override
    public Integer getErrorFlag() {
        return this.errorFlag;
    }

    /**
     * @return 返回分解后的任务列表
     */
    @Override
    public List<Task> decompose() {
        List<Task> subTasks = new ArrayList<>();
        int step = Math.abs(end - begin) / getSubTaskCount();
        int start = begin;
        for (int i = 0; i < getSubTaskCount() - 1; i++) {
            Task subTask = new SumTask(this.clientId, this.errorFlag, start, start + step, this.getSubTaskCount());
            subTasks.add(subTask);
            start += step;
            start += 1;
        }
        SumTask subTask = new SumTask(this.clientId, this.errorFlag, start, end, this.getSubTaskCount());
        subTasks.add(subTask);

        return subTasks;
    }

    /**
     * @return 返回提交任务客户端的id
     */
    @Override
    public String getClientId() {
        return clientId;
    }


    @Override
    public String toString() {
        return "SumTask{" +
                "clientId='" + clientId + '\'' +
                ", errorFlag=" + errorFlag +
                ", begin=" + begin +
                ", end=" + end +
                ", subTaskCount=" + subTaskCount +
                '}';
    }
}
