# -*- coding: UTF-8 -*-

import json
import zmq
import time
import thread
import Queue

from utils import log


class Publisher:
    def __init__(self, addr, topic):
        logger = log.get_logger(category="XmqPuber")
        logger.info("[init xmq puber with %s] begin" % (
            json.dumps({"addr": addr, "topic": topic}, encoding="UTF-8", ensure_ascii=False)))

        self.addr = addr
        self.topic = topic

        self.__connect()

    def __connect(self):
        self.context = zmq.Context()
        self.my_xmq = self.context.socket(zmq.PUB)
        self.my_xmq.connect(self.addr)

    def send(self, msg):
        msg_text = self.topic + json.dumps(msg, encoding="UTF-8", ensure_ascii=False)
        self.my_xmq.send_string(msg_text)


class QueuePublisher:
    def __init__(self, addr, topic):
        self.addr = addr
        self.topic = topic
        self.msg_queue = Queue.Queue()
        self.__start_daemon()

    def send(self, msg):
        self.msg_queue.put(msg)

    def __start_daemon(self):
        def loop_msg():
            mqpuber = Publisher(self.addr, self.topic)
            while True:
                msg = self.msg_queue.get()
                mqpuber.send(msg)

        thread.start_new(loop_msg, ())


class Subscriber:
    def __init__(self, addr, topic):
        logger = log.get_logger(category="XmqSuber")
        logger.info("[init xmq suber with %s] begin" % (
            json.dumps({"addr": addr, "topic": topic}, encoding="UTF-8", ensure_ascii=False)))

        self.addr = addr
        self.topic = topic
        self.topic_length = len(self.topic)

        self.__connect()

    def __connect(self):
        self.context = zmq.Context()
        self.my_xmq = self.context.socket(zmq.SUB)
        self.my_xmq.connect(self.addr)
        self.my_xmq.setsockopt_string(zmq.SUBSCRIBE, self.topic)

    def recv(self):
        data = self.my_xmq.recv()
        data = data[self.topic_length:]

        return data


class QueueSubscriber:
    def __init__(self, addr, topic):
        self.addr = addr
        self.topic = topic

        self.msg_queue = Queue.Queue()
        self.__start_daemon()

    def recv(self):
        return self.msg_queue.get()

    def __start_daemon(self):
        def loop_msg():
            mqsuber = Subscriber(self.addr, self.topic)
            while True:
                msg_text = mqsuber.recv()
                msg = json.loads(msg_text, encoding="UTF-8")

                self.msg_queue.put(msg)

        thread.start_new(loop_msg, ())


class SubMsgResolver:
    def __init__(self):
        pass

    def resolve_msg(self, msg):
        return -1


class resolving_suber:
    def __init__(self, addr, topic):
        self.addr = addr
        self.topic = topic
        self.resolvers = []
        self.__start_daemon()

    def add_resolver(self, resolver):
        self.resolvers.append(resolver)

    def __start_daemon(self):
        def loop_msg():
            mqsuber = Subscriber(self.addr, self.topic)
            while True:
                msg_text = mqsuber.recv()
                msg = json.loads(msg_text, encoding="UTF-8")

                for resolver in self.resolvers:
                    if resolver.resolve_msg(msg) == 0:
                        break

        thread.start_new(loop_msg, ())


class PSServer:
    def __init__(self):
        pass

    @classmethod
    def start_server(cls, context, conf):
        logger = log.get_logger(category="PubSubServer")

        logger.info("[start pubsub server with %s] begin" % (json.dumps(conf, encoding="UTF-8", ensure_ascii=False)))

        conf = context.get("xmq").get(conf.get("xmqServerId"))

        pub_addr = conf["pubAddress"]
        sub_addr = conf["subAddress"]

        context = zmq.Context(1)
        frontend = context.socket(zmq.XSUB)
        frontend.setsockopt(zmq.RCVHWM, 20000)
        frontend.setsockopt(zmq.SNDHWM, 20000)
        frontend.bind(pub_addr)

        backend = context.socket(zmq.XPUB)
        backend.setsockopt(zmq.RCVHWM, 20000)
        backend.setsockopt(zmq.SNDHWM, 20000)
        backend.bind(sub_addr)

        zmq.device(zmq.QUEUE, frontend, backend)

        frontend.close()
        backend.close()
        context.term()