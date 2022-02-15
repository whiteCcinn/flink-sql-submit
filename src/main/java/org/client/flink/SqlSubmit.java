package org.client.flink;

import org.client.flink.cmds.AbstractCommand;

public class SqlSubmit extends Bootstrap {
    public static void main(String[] args) throws Exception {
        registerCommands(args);
        AbstractCommand handler = getCommand();
        handler.run(cmd);
    }
}
