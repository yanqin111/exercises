package com.yq.exercises.task.decompose.task;

import java.io.Serializable;

/**
 * worker 执行完计算任务，待合并的结果
 * @author page
 */
public class ShuffleData implements Serializable {

    /**
     * 客户端ID
     */
    public String clientId;

    /**
     * 计算结果
     */
    public Object value;

    public ShuffleData(String clientId, Object value) {
        super();
        this.clientId = clientId;
        this.value = value;
    }

    @Override
    public String toString() {
        return "ShuffleData{" +
                "clientId='" + clientId + '\'' +
                ", value=" + value +
                '}';
    }
}
