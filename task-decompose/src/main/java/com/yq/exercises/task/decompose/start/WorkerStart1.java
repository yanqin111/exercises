package com.yq.exercises.task.decompose.start;

import com.yq.exercises.task.decompose.other.Config;
import com.yq.exercises.task.decompose.woker.Worker;

/**
 * @author page
 */
public class WorkerStart1 {

    public static void main(String[] args) {
        Worker worker_1 = new Worker(10, 3, Config.MASTER_IP, Config.MASTER_PORT);
        worker_1.init();
        worker_1.run();
        
    }

}
