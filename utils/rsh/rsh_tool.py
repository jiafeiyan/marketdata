# -*- coding: UTF-8 -*-

import os
import stat
import json
import paramiko

from utils.logger.log import log
from utils.config.path_tool import path


class rshell:
    def __init__(self, conf):
        self.config = conf
        self.logger = log.get_logger(category="rsh")
        self.ssh = None

    def connect(self):
        self.__ssh_connect()

    def disconnect(self):
        self.__ssh_disconnect()

    def __ssh_connect(self):
        host = self.config.get("host")
        port = self.config.get("port", 22)
        user = self.config.get("user")
        password = self.config.get("password")

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self.logger.info("[rsh-ssh is connecting: %s]" %(json.dumps(self.config),))
        ssh.connect(hostname=host, port=port, username=user, password=password)
        self.logger.info("[rsh-ssh is connected: %s]" %(json.dumps(self.config),))
        self.ssh = ssh

    def __ssh_disconnect(self):
        self.ssh.close()
        self.logger.info("[rsh-ssh is disconnected: %s]" %(json.dumps(self.config),))

    def parse_remote_path(self, path_str):
        stdin, stdout, stderr = self.execute("echo %s" % (path_str,))
        remote_path = stdout.read()
        if remote_path is not None:
            remote_path = remote_path.replace("\n", "")
        return remote_path

    def execute(self, command):
        self.logger.info("[rsh-ssh is executing: %s] [%s]" %(json.dumps(self.config), command))
        return self.ssh.exec_command(command)

    def put(self, source, target_dir):
        self.logger.info("[rsh-sftp is copying: %s] [(local)%s -> (remote)%s]" %(json.dumps(self.config), source, target_dir))
        sftp = self.ssh.open_sftp()

        source = os.path.abspath(path.convert(source))

        target_dir = self.parse_remote_path(target_dir)

        if target_dir[-1] == "/":
            target_dir = target_dir[0:-1]

        try:
            sftp.stat(target_dir)
        except:
            stdin, stdout, stderr = self.execute("mkdir -p %s" % target_dir)
            stdout.readline()

        if os.path.isfile(source):
            source_file_name = source
            target_file_name = target_dir + "/" + os.path.basename(source)

            sftp.put(source_file_name, target_file_name)
        else:
            files = os.listdir(source)

            for file_name in files:
                source_file_name = os.path.join(source, file_name)
                if not stat.S_ISDIR(os.stat(source_file_name).st_mode):
                    target_file_name = target_dir + "/" + file_name

                    self.logger.info("[rsh-sftp is copying: %s] [(local)%s -> (remote)%s]" % (json.dumps(self.config), source_file_name, target_file_name))
                    sftp.put(source_file_name, target_file_name)

        self.logger.info("[rsh-sftp has copyed: %s] [(local)%s -> (remote)%s]" %(json.dumps(self.config), source, target_dir))

    def get(self, source, target_dir):
        self.logger.info("[rsh-sftp is copying: %s] [(remote)%s -> (local)%s]" %(json.dumps(self.config), source, target_dir))

        target_dir = os.path.abspath(path.convert(target_dir))

        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        sftp = self.ssh.open_sftp()

        source = self.parse_remote_path(source)

        if stat.S_ISDIR(sftp.stat(source).st_mode):
            files = sftp.listdir(source)

            if source[-1] == "/":
                source = source[0:-1]

            for file_name in files:
                source_file_name = source + "/" + file_name
                target_file_name = os.path.join(target_dir, file_name)

                self.logger.info("[rsh-sftp is copying: %s] [(remote)%s -> (local)%s]" % (json.dumps(self.config), source_file_name, target_file_name))
                sftp.get(source_file_name, target_file_name)
        else:
            source_file_name = source
            target_file_name = os.path.join(target_dir, os.path.basename(source))

            sftp.get(source_file_name, target_file_name)

        self.logger.info("[rsh-sftp has copyed: %s] [(remote)%s -> (local)%s]" %(json.dumps(self.config), source, target_dir))
