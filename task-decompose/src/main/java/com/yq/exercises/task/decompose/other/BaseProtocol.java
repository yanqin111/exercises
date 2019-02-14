package com.yq.exercises.task.decompose.other;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 通用方法类
 * @author page
 */
public class BaseProtocol {
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void printInfo(String str) {
        System.out.println("[" + sdf.format(new Date()) + "] -> " + str);
    }

    /**
     * 消息头 40个字符
     *
     * @param flag
     * @param workId
     * @param length
     * @return
     */
    public String buildHead(String flag, String workId, int length) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(flag);
        buffer.append(Config.HEAD_INTERVAL_CHAR);
        buffer.append(workId);
        buffer.append(Config.HEAD_INTERVAL_CHAR);
        buffer.append(length);

        int count = Config.WORKER_HEAD_LENGTH - buffer.toString().length();
        for (int i = 0; i < count; i++) {
            buffer.append(Config.HEAD_INTERVAL_CHAR);
        }
        return buffer.toString();
    }

    /**
     * 从头信息中取出 消息分类标志
     *
     * @param head
     * @return
     */
    public String getFlag(String head) {
        return head.substring(0, Config.WORKER_HEAD_ID_LENGTH);
    }

    /**
     * 从头信息中取出消息内容长度
     *
     * @param head
     * @return
     */
    public int getContextLength(String head) {
        String str = head.substring(Config.WORKER_HEAD_ID_LENGTH+1);
        String str2 = str.substring(str.indexOf(Config.HEAD_INTERVAL_CHAR));
        StringBuffer buffer = new StringBuffer();
        buffer.append(Config.HEAD_INTERVAL_CHAR);
        String str3 = str2.replaceAll(buffer.toString(), "").trim();
        return Integer.valueOf(str3);
    }

    /**
     * 从头信息中获取 workerId信息
     *
     * @param head
     * @return
     */
    public String getWorkId(String head) {
        String str = head.substring(Config.WORKER_HEAD_ID_LENGTH + 1);
        // 取出workId
        String workerId = str.substring(0, str.indexOf(Config.HEAD_INTERVAL_CHAR));
        return workerId;
    }

    /**
     * 合并两个 bytes数组为一个
     *
     * @param bytes1
     * @param bytes2
     * @return
     */
    public byte[] combineBytes(byte[] bytes1, byte[] bytes2) {
        byte[] bytes3 = new byte[bytes1.length + bytes2.length];
        System.arraycopy(bytes1, 0, bytes3, 0, bytes1.length);
        System.arraycopy(bytes2, 0, bytes3, bytes1.length, bytes2.length);
        return bytes3;
    }

    /**
     * 对象转字节数组
     *
     * @param obj
     * @return
     */
    public byte[] toByteArray(Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return bytes;
    }

    /**
     * 字节数组转对象
     *
     * @param bytes
     * @return
     */
    public Object toObject(byte[] bytes) {
        Object obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
        return obj;
    }
}
