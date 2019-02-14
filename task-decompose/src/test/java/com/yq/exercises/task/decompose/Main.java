package com.yq.exercises.task.decompose;

import com.yq.exercises.task.decompose.start.SumTask;
import com.yq.exercises.task.decompose.task.Task;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        Task<Integer, Integer> task = new SumTask("finnxiang:0001", -1, 1, 1001, 5);
        List<Task> tasks = task.decompose();
        List<Integer> reduce = new ArrayList<>();
        for (Task t : tasks) {
            int m = (Integer) t.map();
            reduce.add(m);
            System.out.println(t);
        }
        int r = task.reduce(reduce);
        System.out.println(r);

    }

}
