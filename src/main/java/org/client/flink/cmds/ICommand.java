package org.client.flink.cmds;

import java.util.ArrayList;

public interface ICommand {
    public void run(ArrayList<String> cmd) throws Exception;
}
