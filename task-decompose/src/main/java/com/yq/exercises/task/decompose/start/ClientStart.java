package com.yq.exercises.task.decompose.start;

import com.yq.exercises.task.decompose.task.Task;

/**
 * @author page
 */
public class ClientStart {

	public static void main(String[] args) {
		Client client = new Client();
		Task task = new SumTask("page:0001",-1,1, 100,5);
		client.submit(task);
	}

}
