package com.yq.exercises.task.decompose.other;


/**
 * 配置信息类
 * @author page
 */
public class Config {

    public static final String MASTER_IP = "127.0.0.1";

    public static final int MASTER_PORT = 6060;

    /**
     * worker 消息头内容长度
     */
    public static final int WORKER_HEAD_LENGTH = 40;

    /**
     * worker 消息头workerId长度
     */
    public static final int WORKER_HEAD_ID_LENGTH = 5;

    /**
     * 消息头间隔字符
     */
    public static final char HEAD_INTERVAL_CHAR = '_';


    public static final String WORKER_HEAD_STATE = "state";

    public static final String WORKER_HEAD_HEART = "heart";

    public static final String WORKER_HEAD_TABLE = "table";

    public static final String WORKER_HEAD_VALUE = "value";

    public static final String WORKER_HEAD_TASK = "task_";

    public static final String WORKER_HEAD_CLOSE = "close";

    public static final String CLIENT_HEAD_VALUE = "value";

    public static final String CLIENT_HEAD_STATE = "state";

}
