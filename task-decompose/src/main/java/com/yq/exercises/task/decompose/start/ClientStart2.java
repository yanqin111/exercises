package com.yq.exercises.task.decompose.start;

import com.yq.exercises.task.decompose.task.Task;

/**
 * @author page
 */
public class ClientStart2 {

	public static void main(String[] args) {
		Client client = new Client();
		Task task = new SumTask("page:0002",-1,1, 1001,5);
		client.submit(task);
	}

}
