package com.yq.exercises.task.decompose.start;

import com.yq.exercises.task.decompose.other.BaseProtocol;
import com.yq.exercises.task.decompose.other.Config;
import com.yq.exercises.task.decompose.task.Task;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * 将计算任务提交到master
 * @author page
 */
public class Client extends BaseProtocol {

    private Task task;

    private  SocketChannel socket;
    ByteBuffer readBuffer = ByteBuffer.allocate(1024);

    public Client() {
        try {
            socket = SocketChannel.open();
            socket.connect(new InetSocketAddress(Config.MASTER_IP, Config.MASTER_PORT));
            socket.configureBlocking(false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 提交任务
    public void submit(Task task) {
        this.task = task;
        try {
            byte[] msgBytes = toByteArray(task);
            // 任务号
            String head = buildHead(Config.WORKER_HEAD_TASK, task.getClientId(), msgBytes.length);
            socket.write(ByteBuffer.wrap(combineBytes(head.getBytes(), msgBytes)));
            printInfo("getClientId=" + task.getClientId());
            printInfo("submit is ok");
            while (true) {
                if (readMsg()) {
                    break;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean handleOneMsg() {
        boolean isClose = false;
        byte[] head = new byte[Config.WORKER_HEAD_LENGTH];
        readBuffer.get(head);
        String msgHead = new String(head);
//        printInfo("head:" + msgHead);
        String flag = getFlag(msgHead);
        int contextLength = getContextLength(msgHead);
        byte[] context = new byte[contextLength];
        readBuffer.get(context);

        // 收到处理结果，打印并断开连接
        if (Config.CLIENT_HEAD_VALUE.equals(flag)) {
            Integer rs = (Integer) toObject(context);
            if (checkResult(rs)){
                printInfo("-----------------------------\nthe task is success and  value is:" + rs + "\n-----------------------------");
            }else{
                printInfo("the task is failed ");
            }
            exit();
            isClose = true;
        } else if (Config.CLIENT_HEAD_STATE.equals(flag)) {
            String msg = (String) toObject(context);
            printInfo("read:\n" + msg);
        } else {
            Object msg = toObject(context);
            System.out.println(msg);
        }

        return isClose;
    }

    private void exit() {
        try {
            socket.socket().close();
            socket.close();
            printInfo("client is close!");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * 处理信息，收到close标识就断开连接
     *
     * @return
     */
    private boolean readMsg() {
        boolean isClose = false;
        try {
            int count = socket.read(readBuffer);
            while (count > 0 && !isClose) {
                readBuffer.flip();

                isClose = handleOneMsg();
                //连接关闭后直接退出
                if (isClose) {
                    break;
                }
                readBuffer.compact();
                count = socket.read(readBuffer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isClose;
    }


    /**
     * 校验返回结果
     * @param object
     * @return
     */
    private boolean checkResult(Object object){
        if(task.getErrorFlag().equals(object)){
           return false;
        }
        return true;
    }

}
