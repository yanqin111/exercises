package com.yq.exercises.task.decompose.start;

import java.io.IOException;

import com.yq.exercises.task.decompose.master.Master;
import com.yq.exercises.task.decompose.other.Config;

/**
 * @author page
 */
public class MasterStart {
	public static void main(String[] args) throws IOException {
		Master server = new Master(Config.MASTER_IP, Config.MASTER_PORT);
		server.init();
		server.listen();
	}

}
