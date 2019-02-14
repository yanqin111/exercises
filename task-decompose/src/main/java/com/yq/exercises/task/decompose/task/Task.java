package com.yq.exercises.task.decompose.task;

import java.util.List;

/**
 * @author page
 */
public interface Task<M, R> {
    /**
     * 在worker中运行的方法
     *
     * @return 返回子任务的结果
     */
    M map();

    /**
     * 在master中汇集子任务结果的方法
     *
     * @param list
     * @return 返回最终结果
     */
    R reduce(List<M> list);

    /**
     * @return 获取子任务个数
     */
    int getSubTaskCount();

    /**
     * @return 获取程序出错时的错误标志代码，其值不在子任务和任务的结果当中
     */
    M getErrorFlag();

    /**
     * @return 返回分解后的任务列表
     */
    List<Task> decompose();


    /**
     * @return 返回提交任务客户端的id
     */
    String getClientId();

}
