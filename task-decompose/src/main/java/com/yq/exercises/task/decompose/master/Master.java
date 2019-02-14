package com.yq.exercises.task.decompose.master;

import com.yq.exercises.task.decompose.other.BaseProtocol;
import com.yq.exercises.task.decompose.other.Config;
import com.yq.exercises.task.decompose.task.Blueprint;
import com.yq.exercises.task.decompose.task.ShuffleData;
import com.yq.exercises.task.decompose.task.Task;
import com.yq.exercises.task.decompose.task.WorkerAndTask;
import com.yq.exercises.task.decompose.woker.WorkerStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;

/**
 * @author page
 */
public class Master extends BaseProtocol {

    private String ip;

    private int port;

    private Selector selector;

    private ServerSocketChannel serverSocketChannel;

    private ByteBuffer readBuffer = ByteBuffer.allocate(2048);
    private ByteBuffer writeBuffer = ByteBuffer.allocate(2048);

    /**
     * 收到的新任务的 clientId, 先到先分配，找到执行分配任务的worker之后从列表中删除
     */
    private LinkedList<String> receivedTaskClientList = new LinkedList<>();

    /**
     * 存放worker状态的map，并实时更新
     */
    private Map<String, WorkerStatus> workerStatusMap = new HashMap<String, WorkerStatus>();

    /**
     * 等待下发给worker的任务列表
     */
    private Map<String, Task> baseTaskForBlueprintMap = new HashMap<>();

    /**
     * 等待下发的任务列表，(worker执行完分配任务后的结果)
     */
    private List<Map<String, List<Task>>> subTaskPlanTableMap = new LinkedList<>();

    /**
     * 客户端上传还没有返回结果的任务列表
     */
    private Map<String, Task> baseTaskExecutingMap = new HashMap<>();

    /**
     * 返回给客户端的信息缓存map
     */
    private Map<String, String> echoClientMsgMap = new HashMap<>();

    /**
     * worker计算完成后，子任务结果存在在该处，等待合并返回客户端
     */
    private Map<String, List<Object>> shuffleDataListMap = new HashMap<>();

    public Master(String ip, int port) {
        this.ip = ip;
        this.port = port;

    }

    public void init() throws IOException {
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.socket().bind(new InetSocketAddress(this.ip, this.port));
        this.selector = Selector.open();
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        printInfo("master starting.......");

    }

    public void listen() {
        try {
            while (true) {
                // 获取就绪channel
                int count = selector.select();
                if (count > 0) {
                    Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                    while (iterator.hasNext()) {
                        SelectionKey key = iterator.next();

                        // 若此key的通道是等待接受新的套接字连接
                        if (key.isAcceptable()) {
//                            System.out.println(key.toString() + " : 接收");
                            // 一定要把这个accpet状态的服务器key去掉，否则会出错
                            iterator.remove();
                            ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
                            // 接受socket
                            SocketChannel socket = serverChannel.accept();
                            socket.configureBlocking(false);
                            // 将channel加入到selector中，并一开始读取数据
                            socket.register(selector, SelectionKey.OP_READ);
                        }
                        // 若此key的通道是有数据可读状态
                        if (key.isValid() && key.isReadable()) {
//                            System.out.println(key.toString() + " : 读");
                            readMsg(key);

                        }
                        // 若此key的通道是写数据状态
                        if (key.isValid() && key.isWritable()) {
//                            System.out.println(key.toString() + " : 写");
                            //client 没有新消息，不需要转化为可读
                            writeMsg(key);
                        }
                    }
                }
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void send(SocketChannel channel, String flag, String workerId, Object msg) {
        byte[] msgBytes = toByteArray(msg);
        String head = buildHead(flag, workerId, msgBytes.length);
        try {
            byte[] bytes = combineBytes(head.getBytes(), msgBytes);
            writeBuffer.put(bytes, 0, bytes.length);
            writeBuffer.flip();
            channel.write(writeBuffer);
            writeBuffer.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String handleOneMsg(SelectionKey key, SocketChannel channel) {
        byte[] headBytes = new byte[Config.WORKER_HEAD_LENGTH];
        readBuffer.get(headBytes);
        String head = new String(headBytes);
        String flag = getFlag(head);
        String workId = getWorkId(head);

        int contextLength = getContextLength(head);
        byte[] contextBytes = new byte[contextLength];
        readBuffer.get(contextBytes);

        System.out.println("----received: flag=" + flag + ",workerId:" + workId + ",length:" + contextLength);

        if (Config.WORKER_HEAD_STATE.equals(flag)) {
            // 心跳状态数据，更新到workerStatusMap
            WorkerStatus worker = (WorkerStatus) toObject(contextBytes);
            workerStatusMap.put(worker.workerId, worker);
            send(channel, Config.WORKER_HEAD_HEART, workId, "received state msg data");
        } else if (Config.WORKER_HEAD_CLOSE.equals(flag)) {
            // worker或客户端请求断开连接，发送信息，让其主动断开连接,并清理掉相关数据
            // worker 已上线且可发送消息
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            send(channel, Config.WORKER_HEAD_CLOSE, workId, "close and shutdown");
            // 清理掉worker的状态信息
            workerStatusMap.remove(workId);
            // 清理掉map中客户端任务信息
            baseTaskExecutingMap.remove(workId);
        } else if (Config.WORKER_HEAD_VALUE.equals(flag)) {
            // worker回传子任务执行结果
            ShuffleData shuffleData = (ShuffleData) toObject(contextBytes);
            System.out.println("收到计算结果，shuffleData=" + shuffleData);
            // 将结果加入待合并列表
            String clientId = shuffleData.clientId;
            List<Object> shuffleList = shuffleDataListMap.get(clientId);
            if (null == shuffleList) {
                shuffleList = new ArrayList<>();
            }
            shuffleList.add(shuffleData.value);
            shuffleDataListMap.put(clientId, shuffleList);

            // 反馈进度信息给客户端
            String stateMsg = workId + " map success and the speed is " + getTaskSpeed(clientId) + "%";
            echoClient(clientId, stateMsg);
        } else if (Config.WORKER_HEAD_TABLE.equals(flag)) {
            // worker执行分配任务后并返回分配好的任务列表
            Blueprint blueprint = (Blueprint) toObject(contextBytes);
            System.out.println("收到任务分配结果，blueprint=" + blueprint);
            subTaskPlanTableMap.add(blueprint.planTable);
            // 反馈进度
            echoClientMsgMap.put(blueprint.clientId, blueprint.toString());
        } else if (Config.WORKER_HEAD_TASK.equals(flag)) {
            // 客户端请求的新任务，下发给指定worker进行分配，并返回分配列表
            Task task = (Task) toObject(contextBytes);
            System.out.println("read task:" + task);

            // 反馈进度
            String clientId = workId;
            //押入链表，准备分配worker计算任务计划表
            receivedTaskClientList.add(clientId);
            //放入map
            baseTaskExecutingMap.put(clientId, task);
            echoClient(clientId, "task is begin and planning for implementation");
            //执行分配算法，计算分配给那个worker执行分配算法，准备下发给worker执行
            findWorkerForBlueprint();
        }

        return workId;
    }

    private void echoClient(String clientId, String msg) {
        String oldMsg = echoClientMsgMap.get(clientId);
        if (null == oldMsg) {
            echoClientMsgMap.put(clientId, msg);
        } else {
            echoClientMsgMap.put(clientId, oldMsg + "\n" + msg);
        }
    }


    private void readMsg(SelectionKey key) {
        SocketChannel channel = null;
        try {
            System.out.println("\n");
            Object id = key.attachment();
            printInfo("readMsg and id=" + id);
            channel = (SocketChannel) key.channel();
            // 设置buffer缓冲区
            // 假如客户端关闭了通道，这里在对该通道read数据，会发生IOException，捕获到Exception后，关闭掉该channel，取消掉该key
            int count = channel.read(readBuffer);
            String workId = null;
            // 如果读取到了数据
            while (count > 0) {
                //有信息更新，检查列表并尝试分配 分配任务
                findWorkerForBlueprint();
                // 让buffer翻转，把buffer中的数据读取出来
                readBuffer.flip();

                workId = handleOneMsg(key, channel);

                readBuffer.compact();//踢出已经处理掉的数据,接着读，直到处理完数据
                count = channel.read(readBuffer);
            }
            // 打上标记，让写入消息时知道往哪个worker写
            key.attach(workId);
            //读完消息转为可写模式
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        } catch (Exception e) {
            e.printStackTrace();
            // 当客户端关闭channel时，服务端再往通道缓冲区中写或读数据，都会报IOException，解决方法是：在服务端这里捕获掉这个异常，并且关闭掉服务端这边的Channel通道
            closeChannel(key, channel);
        }
    }

    private void closeChannel(SelectionKey key, SocketChannel channel) {
        if (null != channel && channel.isConnected()) {
            try {
                key.cancel();
                key.attach(null);
                channel.socket().close();
                channel.close();
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }

    }

    /**
     * 找到合适的worker，计算子任务分配列表
     */
    private void findWorkerForBlueprint() {
        //有新任务没有分配
        if (!receivedTaskClientList.isEmpty()) {
            printInfo("findWorkerForBlueprint");
            String clientId = receivedTaskClientList.getFirst();
            Task task = baseTaskExecutingMap.get(clientId);

            System.out.println("clientId:" + clientId + ",task=" + task);

            boolean isSuccess = false;
            //如果有空闲的线程，直接下发任务
            for (String workerId : workerStatusMap.keySet()) {
                WorkerStatus status = workerStatusMap.get(workerId);
                if (status.freeCore > 0) {
                    //在workerId中计算子任务执行计划
                    baseTaskForBlueprintMap.put(workerId, task);
                    isSuccess = true;
                    break;
                }
            }
            //没有空闲线程，下发给排队最快的队列
            if (!isSuccess) {
                int freeQueue = 0;
                List<WorkerStatus> workerStatuses = new ArrayList<>();
                for (String workerId : workerStatusMap.keySet()) {
                    WorkerStatus status = workerStatusMap.get(workerId);
                    freeQueue += status.freeQueue;
                    workerStatuses.add(status);
                }
                if (freeQueue > 0) {
                    Collections.sort(workerStatuses);
                    WorkerStatus status = workerStatuses.get(0);
                    baseTaskForBlueprintMap.put(status.workerId, task);
                    isSuccess = true;
                }
            }

            //下发  分配任务成功之后， 在表中删除
            if (isSuccess) {
                receivedTaskClientList.removeFirst();
                echoClient(clientId, "findWorkerForBlueprint is success!");
                printInfo("findWorkerForBlueprint is ok ");
                for (String key : baseTaskForBlueprintMap.keySet()) {
                    System.out.println(key + "," + baseTaskForBlueprintMap.get(key));
                }
            } else {
                //没有空闲线程和空闲队列，分配失败
                echoClient(clientId, "lack of resource, please waiting......\n");
                printInfo("findWorkerForBlueprint is fail");
            }

        }
    }


    private void writeMsg(SelectionKey key) {
        try {
            SocketChannel channel = (SocketChannel) key.channel();
            Object attachment = key.attachment();
            if (null != attachment) {
                String workerId = attachment.toString();

                // 计算任务下发
                sendSubTaskForExecute(channel, workerId);
                //计算执行计划 下发
                sendWorkerAndTaskForBlueprint(channel, workerId);
                // 计算进度反馈
                echoClientForSpeed(key, channel, workerId);

                //client 不需要转化为可读模式，不会提交新信息
                if (null != baseTaskExecutingMap.get(workerId)) {
                    key.interestOps(SelectionKey.OP_READ);
                }
            } else {
                key.cancel();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 分发执行子任务
     *
     * @param channel
     * @param workerId
     * @throws IOException
     */
    private void sendSubTaskForExecute(SocketChannel channel, String workerId) {
        printInfo("sendSubTaskForExecute,workerId=" + workerId);
        for (Map<String, List<Task>> table : subTaskPlanTableMap) {
            List<Task> taskList = table.get(workerId);
            if (null != taskList) {
                send(channel, Config.WORKER_HEAD_TASK, workerId, taskList);
                System.out.println("sendSubTaskForExecute,taskList=" + taskList);
                // 下发之后删除
                table.remove(workerId);
            }
        }
    }


    /**
     * 分发计算任务
     *
     * @param channel
     * @param workerId
     * @throws IOException
     */
    private void workerHeart(SocketChannel channel, String workerId) throws IOException {
        for (Map<String, List<Task>> table : subTaskPlanTableMap) {
            List<Task> taskList = table.get(workerId);
            if (null != table) {
                byte[] msgBytes = toByteArray(taskList);
                String head = Config.WORKER_HEAD_TASK + msgBytes.length;
                channel.write(ByteBuffer.wrap(combineBytes(head.getBytes(), msgBytes)));
                // 下发之后删除
                table.remove(workerId);
            }

            // 清理掉已成功下发计划任务的列表
            if (table.isEmpty()) {
                subTaskPlanTableMap.remove(table);
            }

        }
    }


    /**
     * 将计算计划图的任务下发到 指定worker中 计算子任务分配列表
     *
     * @param channel
     * @param workerId
     * @throws IOException
     */
    private void sendWorkerAndTaskForBlueprint(SocketChannel channel, String workerId) {
        printInfo("sendWorkerAndTaskForBlueprint," + baseTaskForBlueprintMap.size());
        Task task = baseTaskForBlueprintMap.get(workerId);
        if (null != task) {
            System.out.println("sendWorkerAndTaskForBlueprint=" + workerId);


            List<WorkerStatus> workers = new ArrayList<>();
            for (String id : workerStatusMap.keySet()) {
                workers.add(workerStatusMap.get(id));
            }

            WorkerAndTask workerAndTask = new WorkerAndTask();
            workerAndTask.task = task;
            workerAndTask.workers = workers;

            send(channel, Config.WORKER_HEAD_TABLE, workerId, workerAndTask);
            System.out.println("send workerAndTask is ok and workerAndTask=" + workerAndTask);
            //下发成功
            baseTaskForBlueprintMap.remove(workerId);
            echoClient(task.getClientId(), "send worker " + workerId + "  for blueprint");
        }

    }

    /**
     * 客户端信息 任务执行状态和进度
     * 如果关闭链接则返回false；
     *
     * @param channel
     */
    private void echoClientForSpeed(SelectionKey key, SocketChannel channel, String clientId) {
        printInfo("echoClientForSpeed");
        String msg = echoClientMsgMap.get(clientId);
        if (null != msg) {
            send(channel, Config.CLIENT_HEAD_STATE, clientId, msg);
            // 删除已发送内容
            echoClientMsgMap.remove(clientId);
            // 检查任务进度,如果全部完成则关闭连接
            Task task = baseTaskExecutingMap.get(clientId);
            List<Object> reduceList = shuffleDataListMap.get(clientId);
            // worker计算完成，合并结果，返回给客户端并发送断开信息
            if (null != reduceList && task.getSubTaskCount() == reduceList.size()) {
                Object result = task.reduce(reduceList);
                System.out.println("计算完成，合并结果给client,value=" + result);
                send(channel, Config.CLIENT_HEAD_VALUE, clientId, result);
                // 清理缓存内容
                baseTaskExecutingMap.remove(clientId);
                shuffleDataListMap.remove(clientId);
                //清理掉标志信息
                key.attach(null);
            }
        }
    }

    private int getTaskSpeed(String clientId) {
        Task task = baseTaskExecutingMap.get(clientId);
        List<Object> reduceList = shuffleDataListMap.get(clientId);
        System.out.println("reduce size=" + reduceList.size() + ",taskcount:" + task.getSubTaskCount());
        return (reduceList.size() * 100) / task.getSubTaskCount();
    }

}
