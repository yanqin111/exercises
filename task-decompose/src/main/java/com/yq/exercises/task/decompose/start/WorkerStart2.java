package com.yq.exercises.task.decompose.start;

import com.yq.exercises.task.decompose.other.Config;
import com.yq.exercises.task.decompose.woker.Worker;

/**
 * @author page
 */
public class WorkerStart2 {

    public static void main(String[] args) {
        Worker worker_2 = new Worker(5, 2, Config.MASTER_IP, Config.MASTER_PORT);
        worker_2.init();
        worker_2.run();
    }

}
