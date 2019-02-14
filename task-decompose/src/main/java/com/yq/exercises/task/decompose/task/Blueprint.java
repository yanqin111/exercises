package com.yq.exercises.task.decompose.task;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 
 * 执行计划图
 * @author page
 */
public class Blueprint implements Serializable {

	public String clientId;

	public Map<String, List<Task>> planTable;

	public String getBlueprint() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("----------------------------\n");
		buffer.append("the task clientId is :" + clientId + "\n");
		for (String key : planTable.keySet()) {
			buffer.append(key + "  ->  " + planTable.get(key).toString() + "\n");
		}
		buffer.append("----------------------------\n");
		return buffer.toString();
	}

	@Override
	public String toString() {
		return "Blueprint{" +
				"clientId='" + clientId + '\'' +
				", planTable:\n" + getBlueprint() +
				'}';
	}
}
