import pickle
import random
import sys
import threading
import queue
import multiprocessing.connection
import time
import hashlib
from collections import defaultdict
import signal


PORT_RECV = 5001
PORT_SEND = 5001
HOST_SEND = []
SEND_QUEUE_LOCK = multiprocessing.Lock()
HOLDBACK_LOCK = multiprocessing.Lock()
REPLY_DICT_LOCK = multiprocessing.Lock()
SEQ_LOCK = multiprocessing.Lock()
ACCOUNT_LOCK = multiprocessing.Lock()
BANDWIDTH_LOCK = multiprocessing.Lock()

BANDWIDTH_LIST = []
BANDWIDTH_LIST_TEMP = []

DELAY = []


class Message:
    def __init__(self):
        self.source = None
        self.type = None
        self.amount = None
        self.sender = None
        self.receiver = None
        self.sequence = None
        self.messageId = None

        self.isRecvReply = False
        self.isSendReply = False
        self.isDeliverable = False

    def parseMessage(self, pid, content):
        self.source = pid
        md5 = hashlib.md5()
        md5.update(str.encode(content + str(pid)))
        self.messageId = md5.digest()
        for i, word in enumerate(content.split()):
            if i == 0:
                if word == "DEPOSIT":
                    self.type = "DEPOSIT"
                elif word == "TRANSFER":
                    self.type = "TRANSFER"
            if i == 1:
                if self.type == "DEPOSIT":
                    self.receiver = word
                elif self.type == "TRANSFER":
                    self.sender = word
            if i == 2:
                if self.type == "DEPOSIT":
                    self.amount = float(word)
            if i == 3:
                if self.type == "TRANSFER":
                    self.receiver = word
            if i == 4:
                if self.type == "TRANSFER":
                    self.amount = float(word)

    def assignSeq(self, seq):
        self.sequence = seq

    def assignSource(self, source):
        self.source = source

    def assignMessageId(self, msgId):
        self.messageId = msgId

    def assignReceiver(self, receiver):
        self.receiver = receiver


class Process:
    def __init__(self):
        self.pid = None
        self.totalProcessNum = None
        self.sendSeq = None
        self.agreedSeq = None
        self.accountDict = defaultdict(int)

        self.holdback = []
        self.delivered = []

        self.replyDict = {}

    def assignPid(self, pid):
        self.pid = pid

    def assignTotalProcessNum(self, totalProcessNum):
        self.totalProcessNum = totalProcessNum

    def newMessage(self, content):
        splitMessage = content.split(" ")
        # print("debug messsages: split:" + str(splitMessage))
        if splitMessage[0] == "DEPOSIT":
            msg = Message()
            msg.parseMessage(self.pid, content)
            msg.assignSeq(isisSeq(self))
            self.accountDict[splitMessage[1]] += 0
            self.holdback.append(msg)
            self.replyDict.update({msg.messageId: []})
            return msg
        elif splitMessage[0] == "TRANSFER":
            msg = Message()
            msg.parseMessage(self.pid, content)
            if splitMessage[1] not in self.accountDict:
                return None
            if self.accountDict[splitMessage[1]] - float(splitMessage[4]) < 0:
                return None
            msg.assignSeq(isisSeq(self))
            self.holdback.append(msg)
            self.replyDict.update({msg.messageId: []})
            return msg
        else:
            print("Unknown message")
            return None

    def isNewMsg(self, msg):
        for message in self.holdback:
            if message.messageId == msg.messageId:
                return False
        for message in self.delivered:
            if message.messageId == msg.messageId:
                return False
        self.holdback.append(msg)
        if msg.sender != None and msg.sender not in self.accountDict.keys():
            self.accountDict.update({msg.sender: 0})
        if msg.receiver != None and msg.receiver not in self.accountDict.keys():
            self.accountDict.update({msg.receiver: 0})
        return True

    def recvMessage(self, msg):
        if msg.isRecvReply:
            if msg.messageId in self.replyDict:
                self.replyDict[msg.messageId].append(msg)
            return None
        elif msg.isSendReply:
            for i, message in enumerate(self.holdback):
                self.holdback[i].isDeliverable = True
                self.holdback[i].sequence = msg.sequence
            return None
        else:
            isNewMsg = self.isNewMsg(msg)
            if isNewMsg:
                replyMsg = Message()
                replyMsg.isRecvReply = True
                replyMsg.assignSource(self.pid)
                replyMsg.assignSeq(isisSeq(self))
                replyMsg.assignMessageId(msg.messageId)
                replyMsg.assignReceiver(msg.source)
                return replyMsg

    def newSenderReply(self, msgId):
        receiveReply = Message()
        receiveReply.isSendReply = True
        receiveReply.assignSource(self.pid)
        receiveReply.assignMessageId(msgId)
        receiveReply.assignSeq(isisAgreedSeq(self.replyDict[msgId]))
        for i in range(len(self.holdback)):
            if self.holdback[i].messageId == msgId:
                self.holdback[i].sequence = isisAgreedSeq(self.replyDict[msgId])
                self.holdback[i].isDeliverable = True
                break
        return receiveReply

    def printInfo(self):
        print("BALANCES", end="")
        for k, _ in list(self.accountDict.items()):
            print(" " + str(k) + ":" + str(self.accountDict[k]), end="")
        print()


"""
def acceptFunc(q, i):
    flag = False
    while flag == False:
        with multiprocessing.connection.Listener("", PORT_RECV + i) as s:
            conn = s.accept()
            q.put(conn)
            flag = True


def connectFunc(q, i, addr):
    flag = False
    while flag == False:
        with multiprocessing.connection.Client(HOST_SEND[addr], PORT_SEND + i) as s:
            q.put(s)
            flag = True
"""


def bandwidthFunc():
    while True:
        with BANDWIDTH_LOCK:
            bandwidth = 0
            for i in BANDWIDTH_LIST_TEMP:
                bandwidth += i
            BANDWIDTH_LIST.append(bandwidth)
            BANDWIDTH_LIST_TEMP.clear()
        time.sleep(1)


counter = 0.0


def isisSeq(p):
    global counter
    if p.sendSeq is None or p.agreedSep is None:
        pNum = counter + (float(p.pid) * 0.1)
        p.sendSeq = pNum
        p.agreedSep = pNum
        counter += 1.0
        return pNum
    counter += 1.0
    p.sendSeq += counter
    p.agreedSep += counter
    return max(p.sendSeq, p.agreedSep) + counter


def isisAgreedSeq(list):
    max = list[0].sequence
    for msg in list[1:]:
        if msg.sequence > max:
            max = msg.sequence
    return max + 1.0


def acceptFunc(q, i):
    got = False
    s = None
    while got == False:
        try:
            print(
                "Accept failed ",
            )
            s = multiprocessing.connection.Listener(("", i))
            conn = s.accept()
            q.put(conn)
            got = True
        except Exception as e:
            print("Unable to bind socket", i, flush=True)
            time.sleep(random.random())
        finally:
            s.close()


def connectFunc(q, i, IP):
    got = False
    while got == False:
        try:
            s = multiprocessing.connection.Client((IP, i))
            q.put(s)
            got = True
        except Exception as e:
            print("Failed to connect", i, flush=True)
            time.sleep(random.random())


NODES = None
OFFSET = None
sendQueueList = []


def recvFunc(conn, i):
    data = conn.recv()
    with conn:
        while True:
            data = conn.recv()
            if not data:
                break
            with BANDWIDTH_LOCK:
                BANDWIDTH_LIST_TEMP.append(len(data))
            msg = pickle.loads(data)
            with SEND_QUEUE_LOCK and HOLDBACK_LOCK and REPLY_DICT_LOCK and SEQ_LOCK and ACCOUNT_LOCK:
                reply = p.recvMessage(msg)
                if reply is not None:
                    if reply.isRecvReply:
                        with SEND_QUEUE_LOCK:
                            for j in range(NODES - 1):
                                sendQueueList[j].put(reply)


def sendFunc(s, i):
    # s.send(bytes(str(OFFSET), "utf8"))
    s.send(bytes(str(NODE_NAME), "utf8"))
    while True:
        if len(sendQueueList) > 0 and sendQueueList[i].empty() == False:
            with SEND_QUEUE_LOCK:
                msg = sendQueueList[i].get()
                if msg is not None:
                    s.send(pickle.dumps(msg, protocol=4))
    s.close


def stdIn(p):
    while True:
        for line in sys.stdin:
            if line is not "":
                with SEND_QUEUE_LOCK and HOLDBACK_LOCK and REPLY_DICT_LOCK and SEQ_LOCK and ACCOUNT_LOCK:
                    newMsg = p.newMessage(line)
                    for i in range(len(sendQueueList)):
                        sendQueueList[i].put(newMsg)
                    break


def isisFunc(p):
    while True:
        with SEQ_LOCK and HOLDBACK_LOCK and REPLY_DICT_LOCK:
            delKey = []
            for key, value in list(p.replyDict.items()):
                if len(p.replyDict[key]) >= NODES - 1:
                    sendReply = p.newSenderReply(p.replyDict[key][0].messageId)
                    delKey.append(key)
                    with SEND_QUEUE_LOCK:
                        for i in range(len(sendQueueList)):
                            sendQueueList[i].put(sendReply)
                    # p.printInfo()
            for item in delKey:
                del p.replyDict[item]
        with HOLDBACK_LOCK and SEQ_LOCK:
            holdbackCopy = p.holdback
            for i, item in enumerate(holdbackCopy):
                if item.isDeliverable == True:
                    p.holdback.sort(key=lambda x: x.sequence)
                    break

        holdbackCopy = p.holdback
        toDelMsg = []
        for i in range(len(holdbackCopy)):
            if holdbackCopy[i].isDeliverable == True:
                toDelMsg.append(holdbackCopy[i])
                DELAY.append(time.time() * 1000)
            else:
                break
        p.holdback = list(set(p.holdback) - set(toDelMsg))
        with ACCOUNT_LOCK:
            for m in toDelMsg:
                # print("m.type" + str(m.type))
                if m.type == "DEPOSIT":
                    """
                    print(
                        "Debug message DEPOSIT: p.accountDict[m.receiver]:"
                        + str(p.accountDict[m.receiver])
                        + " m.amount: "
                        + str(m.amount)
                        + "\n"
                    )
                    """
                    p.accountDict[m.receiver] += m.amount
                elif m.type == "TRANSFER":
                    if p.accountDict[m.sender] - m.amount < 0:
                        break
                    p.accountDict[m.sender] -= m.amount
                    p.accountDict[m.receiver] += m.amount
                p.printInfo()


def signal_handler(signal, frame):
    with ACCOUNT_LOCK:
        with open(NODE_NAME + ".txt", "w") as fd:
            fd.write("BALANCES")
            print("BALANCES", end="")
            for k, _ in list(p.accountDict.items()):
                fd.write(" " + str(k) + ":" + str(p.accountDict[k]) + " ")
                print(" " + str(k) + ":" + str(p.accountDict[k]), end="")
            fd.write("\n")
            fd.write("\n")
            fd.write("BANDWIDTH: \n")
            for b in BANDWIDTH_LIST:
                fd.write(str(b))
                fd.write("\n")
            fd.write("\n")
            fd.write("\n")
            fd.write("DELAY: ")
            for i, _ in enumerate(DELAY):
                if i > 0:
                    fd.write(str(DELAY[i] - DELAY[i - 1]))
                fd.write("\n")
            fd.close()
    sys.exit(0)


def get_index(key):
    m = hashlib.md5(key.encode("utf8")).hexdigest()
    map_key = str(m)[-2:]
    index = int(map_key, 16) % 256
    return index


signal.signal(signal.SIGINT, signal_handler)
if len(sys.argv) == 3:
    NODES = int(sys.argv[1])
    OFFSET = int(sys.argv[2])


configList = []
if len(sys.argv) == 4:
    NODE_NAME = str(sys.argv[1])
    PORT = int(sys.argv[2])
    CONFIG_FILE = str(sys.argv[3])

    with open(CONFIG_FILE, "r") as fs:
        for line in fs:
            configList.append(line)
NODES = int(configList[0])
# print("NODES:" + str(NODES))

p = Process()
# p.assignPid(OFFSET)
p.assignPid(get_index(NODE_NAME))
# print("PID: " + str(p.pid))
p.assignTotalProcessNum(NODES)


for i in range(NODES):
    sendQueueList.append(queue.Queue())

receivePortOffsets = []

acceptQueue = queue.Queue()
acceptList = []
connQueue = queue.Queue()
connList = []
for i, line in enumerate(configList[1:]):
    # print("configList: " + line)
    k = get_index(str(line.split()[0]))
    acceptThread = threading.Thread(target=acceptFunc, args=(acceptQueue, PORT + k))
    acceptList.append(acceptThread)
    acceptThread.start()
for i, line in enumerate(configList[1:]):
    # print(get_index(str(line.split()[0])))
    k = get_index(str(line.split()[0]))
    connectThread = threading.Thread(
        target=connectFunc,
        args=(
            connQueue,
            int(line.split()[2]) + int(p.pid),
            str(line.split()[1]),
        ),
    )
    connList.append(connectThread)
    connectThread.start()


for t in acceptList:
    t.join()
for t in connList:
    t.join()
time.sleep(1)

print("Accepting and connecting ... ")

bandwidth_t = threading.Thread(target=bandwidthFunc, args=())
bandwidth_t.start()

connectList = []
sockList = []
for i in range(NODES - 1):
    connectList.append(acceptQueue.get())
    sockList.append(connQueue.get())


connThreadList = []
sendThreadList = []
for i in range(NODES - 1):
    receiveThread = threading.Thread(target=recvFunc, args=(connectList[i], i))
    connThreadList.append(receiveThread)
for i in range(NODES - 1):
    sendThread = threading.Thread(target=sendFunc, args=(sockList[i], i))
    sendThreadList.append(sendThread)
for t in connThreadList:
    t.start()
for t in sendThreadList:
    t.start()

stdInThread = threading.Thread(target=stdIn, args=(p,))
stdInThread.start()
isisThread = threading.Thread(target=isisFunc, args=(p,))
isisThread.start()
