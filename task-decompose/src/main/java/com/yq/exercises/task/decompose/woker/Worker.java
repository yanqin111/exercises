package com.yq.exercises.task.decompose.woker;

import com.yq.exercises.task.decompose.other.BaseProtocol;
import com.yq.exercises.task.decompose.other.Config;
import com.yq.exercises.task.decompose.task.Blueprint;
import com.yq.exercises.task.decompose.task.ShuffleData;
import com.yq.exercises.task.decompose.task.Task;
import com.yq.exercises.task.decompose.task.WorkerAndTask;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;

/**
 * 执行子任务的worker
 * @author page
 */
public class Worker extends BaseProtocol {

    private String masterIp;
    private int masterPort;

    private WorkerStatus status;

    private ExecutorService fixedThreadPool;
    private Queue<Task> taskQueue;

    private SocketChannel channel;

    private ByteBuffer readBuffer = ByteBuffer.allocate(1024);
    private ByteBuffer writeBuffer = ByteBuffer.allocate(1024);

    public Worker(int queueSize, int coreSize, String masterIp, int masterPort) {
        this.status = new WorkerStatus();
        this.status.queueSize = queueSize;
        this.status.coreSize = coreSize;
        this.status.freeCore = coreSize;
        this.status.freeQueue = queueSize;
        this.status.workerId = buildWorkId();
        this.masterIp = masterIp;
        this.masterPort = masterPort;
    }

    /**
     * 初始化资源，并上报master
     *
     * @throws IOException
     */
    public void init() {
        this.fixedThreadPool = Executors.newFixedThreadPool(status.coreSize);
        this.taskQueue = new LinkedBlockingQueue<>(status.queueSize);

        try {
            channel = SocketChannel.open();
            channel.connect(new InetSocketAddress(this.masterIp, this.masterPort));
            channel.configureBlocking(false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        printInfo("worker is start....");
        printInfo("init state:" + this.status.toString());
        //上报master
        heart();
    }

    /**
     * 停止任务，释放资源，上报master
     */
    public void dead() {
        send(Config.WORKER_HEAD_CLOSE, "close and exit");
    }


    public void run() {
        while (true) {
            //收到退出信号，直接退出
            if (!received()) {
                break;
            }
            heart();
            if (this.status.freeCore > 0 && !taskQueue.isEmpty()) {
                executeSubTask();// 执行计算任务
            } else {
                // 没有计算任务时，暂停5秒，防止消息爆炸
                try {
                    printInfo("没有计算任务，休息一会");
                    Thread.sleep(5000);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }

    }

    /**
     * 上报心跳信息
     */
    private void heart() {
        this.status.time = System.currentTimeMillis();
        send(Config.WORKER_HEAD_STATE, this.status);
    }


    /**
     *
     * @return
     */
    private boolean received() {
        try {
            int count = channel.read(readBuffer);
            while (count > 0) {
                readBuffer.flip();

                handleOneMsg();

                readBuffer.compact();
                count = channel.read(readBuffer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }


    /**
     * @return 关闭链接信号
     */
    private boolean handleOneMsg() {
        byte[] head = new byte[Config.WORKER_HEAD_LENGTH];
        readBuffer.get(head);
        String msgHead = new String(head);
        printInfo("head:" + msgHead);
        String flag = getFlag(msgHead);
        int contextLength = getContextLength(msgHead);
        byte[] context = new byte[contextLength];
        readBuffer.get(context);
        if (flag.equals(Config.WORKER_HEAD_TASK)) {// 计算任务
            List<Task> taskList = (List<Task>) toObject(context);
            System.err.println("收到计算任务，taskList=" + taskList);
            for (Task task : taskList) {
                // 任务加入队列
                taskQueue.add(task);
                this.status.freeQueue -= 1;
            }
            heart();
        } else if (flag.equals(Config.WORKER_HEAD_TABLE)) {// 分配任务
            this.status.freeCore -= 1;
            // 上报心跳
            heart();
            WorkerAndTask workerAndTask = (WorkerAndTask) toObject(context);
            System.err.println("workerAndTask=" + workerAndTask);
            Blueprint blueprint = buildBlueprint(workerAndTask);
            // 返回计算结果
            send(Config.WORKER_HEAD_TABLE, blueprint);
            this.status.freeCore += 1;
            heart();// 上报心跳
//        } else if (flag.equals(Config.WORKER_HEAD_CLOSE)) {
//            exit();
//            return  false;
        } else if (flag.equals(Config.WORKER_HEAD_HEART)) {
            printInfo((String) toObject(context));
        } else {
            System.err.println("received message =  " + msgHead + new String(context));
        }
        return true;
    }


    /**
     * 释放资源
     */
    private void exit() {
        try {
            channel.socket().close();
            channel.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.fixedThreadPool.shutdown();
        this.taskQueue.clear();

    }

    /**
     * 执行任务
     *
     * @return
     */
    private void executeSubTask() {
        /**
         * 有待计算的任务且线程有空闲
         */
        // 获取任务
        final Task subTask = taskQueue.poll();
        printInfo("executeSubTask:" + subTask);
        status.freeQueue += 1;

        // 执行等待结果
        Future<Object> future = fixedThreadPool.submit(new Callable<Object>() {
            public Object call() {
                return subTask.map();
            }
        });

        // 提交任务，core -1
        status.freeCore -= 1;
        heart();//更新状态
        // 执行错误，返回异常编码
        Object value = subTask.getErrorFlag();
        try {
            value = future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        // 完成任务，core +1
        status.freeCore += 1;
        heart();
        ShuffleData shuffle = new ShuffleData(subTask.getClientId(), value);
        send(Config.WORKER_HEAD_VALUE, shuffle);
        System.err.println("子任务计算完成：返回结果，shuffle=" + shuffle);
    }


    /**
     * 消息格式： 消息头+消息内容 消息头 40个字节：格式为flag_workerId_contextLength
     *
     * @param flag
     * @param context
     */
    private void send(String flag, Object context) {
        byte[] contextBytes = toByteArray(context);
        String head = buildHead(flag, this.status.workerId, contextBytes.length);
        byte[] bytes = combineBytes(head.getBytes(), contextBytes);
        writeBuffer.put(bytes, 0, bytes.length);
        writeBuffer.flip();
        try {
            channel.write(writeBuffer);
            writeBuffer.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 将任务加入到map中
     *
     * @param map
     * @param workerId
     * @param task
     */
    private void putTaskToMap(Map<String, List<Task>> map, String workerId, Task task) {
        List<Task> taskList = map.get(workerId);
        if (null == taskList) {
            taskList = new ArrayList<>();
        }
        taskList.add(task);
        map.put(workerId, taskList);
    }

    /**
     * 分配任务，并返回任务执行计划图
     *
     * @param workerAndTask
     * @return
     */
    private Blueprint buildBlueprint(WorkerAndTask workerAndTask) {
        Blueprint blueprint = new Blueprint();
        blueprint.clientId = workerAndTask.task.getClientId();
        List<Task> tasks = workerAndTask.task.decompose();

        Map<String, List<Task>> map = new HashMap();

        int index = tasks.size() - 1;
        int sumFreeCore = 0;
        for (WorkerStatus worker : workerAndTask.workers) {
            sumFreeCore += worker.freeCore;
        }
        //空闲线程足够，那就直接分配到空闲的线程当中
        if (sumFreeCore >= workerAndTask.task.getSubTaskCount()) {
            for (WorkerStatus worker : workerAndTask.workers) {
                if (index < 0) {
                    break;
                }
                if (worker.freeCore > 0) {
                    for (int i = 0; i < worker.freeCore; i++) {
                        if (index < 0) {
                            break;
                        }
                        putTaskToMap(map, worker.workerId, tasks.get(index));

                        index -= 1;
                    }
                    //更新状况
                    worker.freeCore = 0;
                }
            }

        } else {
            //存在空闲线程,优先分配给空闲线程，剩下的分配给速度最快的队列
            if (sumFreeCore > 0) {
                for (WorkerStatus worker : workerAndTask.workers) {
                    if (worker.freeCore > 0) {
                        for (int i = 0; i < worker.freeCore; i++) {
                            putTaskToMap(map, worker.workerId, tasks.get(index));
                            index -= 1;
                        }
                        //更新状况
                        worker.freeCore = 0;
                    }
                    for (int i = index; i >= 0; i--) {
                        Collections.sort(workerAndTask.workers);
                        WorkerStatus firstWorker = workerAndTask.workers.get(0);
                        putTaskToMap(map, firstWorker.workerId, tasks.get(index));
                        firstWorker.freeQueue -= 1;
                    }
                }

            } else {//不存在空闲线程
                int sumFreeQueue = 0;
                for (WorkerStatus worker : workerAndTask.workers) {
                    sumFreeQueue += worker.freeQueue;
                }
                //空闲队列等于 任务数，直接分配
                if (tasks.size() == sumFreeQueue) {
                    for (WorkerStatus worker : workerAndTask.workers) {
                        if (worker.freeQueue > 0) {
                            for (int i = 0; i < worker.freeQueue; i++) {
                                putTaskToMap(map, worker.workerId, tasks.get(index));
                                index -= 1;
                            }
                            //更新状况
                            worker.freeQueue = 0;
                        }
                    }
                } else {
                    //优先将任务分配给 等待速度最少的 worker
                    for (Task task : tasks) {
                        Collections.sort(workerAndTask.workers);

                        WorkerStatus worker = workerAndTask.workers.get(0);
                        putTaskToMap(map, worker.workerId, tasks.get(index));
                        worker.freeQueue -= 1;
                    }

                }
            }
        }


        blueprint.planTable = map;
        System.out.println(blueprint.getBlueprint());
        return blueprint;
    }

    /**
     * 获取本机ip
     *
     * @return
     */
    private String getIp() {
        String ip = "";
        try {
            // 获取IP地址
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ip;
    }

    // 生成主键
    public String buildWorkId() {
        Random ra = new Random();
        String ip = getIp();
        if (ip.length() > 15) {
            ip = ip.substring(0, 15);
        }
        return ip + ":" + ra.nextInt(10000);
    }

}
