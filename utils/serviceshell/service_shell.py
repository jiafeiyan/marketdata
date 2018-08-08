#-*- coding: UTF-8 -*-

import os
import json
from utils import log, rshell


def execute_command(context, conf):
    logger = log.get_logger(category="ServiceShell")

    logger.info("[service shell with %s] begin" % (json.dumps(conf, encoding="UTF-8", ensure_ascii=False),))

    hosts_config = context.get("hosts")
    host_id = conf.get("host")
    command = conf.get("command")

    parameters = conf.get("parameters", None)
    if parameters is not None:
        for parameter in parameters:
            command = command.replace("@%s@" % parameter, parameters.get(parameter))

    host_config = hosts_config.get(host_id)

    rsh = rshell(host_config)
    rsh.connect()
    stdin, stdout, stderr = rsh.execute(command)

    for line in stdout.readlines():
        line = line.replace("\n", "")
        print("\033[1;32m %s \033[0m" % line)

    for line in stderr.readlines():
        os.sys.stderr.write(line)

    rsh.disconnect()

    logger.info("[service shell with %s] end" % (json.dumps(conf, encoding="UTF-8", ensure_ascii=False),))
