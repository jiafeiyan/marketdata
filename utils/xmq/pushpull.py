# -*- coding: UTF-8 -*-

import json
import zmq
import thread
import Queue

from utils import log


class Pusher:
    def __init__(self, addr, topic):
        logger = log.get_logger(category="XmqPusher")
        logger.info("[init xmq puber with %s] begin" % (
            json.dumps({"addr": addr, "topic": topic}, encoding="UTF-8", ensure_ascii=False)))

        self.addr = addr
        self.topic = topic

        self.__connect()

    def __connect(self):
        self.context = zmq.Context()
        self.my_xmq = self.context.socket(zmq.PUSH)
        self.my_xmq.connect(self.addr)

    def send(self, msg):
        msg_text = self.topic + json.dumps(msg, encoding="UTF-8", ensure_ascii=False)
        self.my_xmq.send_string(msg_text)


class QueuePusher:
    def __init__(self, addr, topic):
        self.addr = addr
        self.topic = topic
        self.msg_queue = Queue.Queue()
        self.__start_daemon()

    def send(self, msg):
        self.msg_queue.put(msg)

    def __start_daemon(self):
        def loop_msg():
            mqpusher = Pusher(self.addr, self.topic)
            while True:
                msg = self.msg_queue.get()
                mqpusher.send(msg)

        thread.start_new(loop_msg, ())


class Puller:
    def __init__(self, addr, topic):
        logger = log.get_logger(category="XmqPuller")
        logger.info("[init xmq suber with %s] begin" % (
            json.dumps({"addr": addr, "topic": topic}, encoding="UTF-8", ensure_ascii=False)))

        self.addr = addr
        self.topic = topic
        self.topic_length = len(self.topic)

        self.__connect()

    def __connect(self):
        self.context = zmq.Context()
        self.my_xmq = self.context.socket(zmq.PULL)
        self.my_xmq.connect(self.addr)

    def pull(self):
        data = self.my_xmq.recv()
        data = data[self.topic_length:]
        return data


class QueuePuller:
    def __init__(self, addr, topic):
        self.addr = addr
        self.topic = topic

        self.msg_queue = Queue.Queue()
        self.__start_daemon()

    def pull(self):
        return self.msg_queue.get()

    def __start_daemon(self):
        def loop_msg():
            mqpuller = Puller(self.addr, self.topic)
            while True:
                msg_text = mqpuller.pull()
                msg = json.loads(msg_text, encoding="UTF-8")
                self.msg_queue.put(msg)

        thread.start_new(loop_msg(), ())


class PullMsgResolver:
    def __init__(self):
        pass

    def resolve_msg(self, msg):
        return -1


class resolving_puller:
    def __init__(self, addr, topic):
        self.addr = addr
        self.topic = topic
        self.resolvers = []
        self.__start_daemon()

    def add_resolver(self, resolver):
        self.resolvers.append(resolver)

    def __start_daemon(self):
        def loop_msg():
            mqpuller = Puller(self.addr, self.topic)
            while True:
                msg_text = mqpuller.pull()
                msg = json.loads(msg_text, encoding="UTF-8")

                for resolver in self.resolvers:
                    if resolver.resolve_msg(msg) == 0:
                        break

        thread.start_new(loop_msg, ())


class PPServer:
    def __init__(self):
        pass

    @classmethod
    def start_server(cls, context, conf):
        logger = log.get_logger(category="PushPullServer")
        logger.info("[start pushpull server with %s] begin" % (json.dumps(conf, encoding="UTF-8", ensure_ascii=False)))

        conf = context.get("xmq").get(conf.get("xmqServerId"))

        push_addr = conf["pushAddress"]
        pull_addr = conf["pullAddress"]

        context = zmq.Context()
        frontend = context.socket(zmq.PULL)
        frontend.setsockopt(zmq.RCVHWM, 20000)
        frontend.setsockopt(zmq.SNDHWM, 20000)
        frontend.bind(push_addr)

        backend = context.socket(zmq.PUSH)
        backend.setsockopt(zmq.RCVHWM, 20000)
        backend.setsockopt(zmq.SNDHWM, 20000)
        backend.bind(pull_addr)

        zmq.device(zmq.STREAMER, frontend, backend)

        frontend.close()
        backend.close()
        context.term()

