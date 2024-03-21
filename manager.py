import socket 
import sys
import ipaddress
import random

dhtSetup = False
setupInitiated = False

def createTuple(hash):
    string = ""
    i = 0
    for key, peerInfoObject in peerInfoHash.items():
        peerAddr = peerInfoObject.peerAddr
        mPort = peerInfoObject.mPort
        pPort = peerInfoObject.pPort
        state = peerInfoObject.state
        if state == "Leader":
            string += f"{i}: ({key}, {peerAddr}, {pPort})\n"
            i += 1
    for key, peerInfoObject in peerInfoHash.items():
        peerAddr = peerInfoObject.peerAddr
        mPort = peerInfoObject.mPort
        pPort = peerInfoObject.pPort
        state = peerInfoObject.state
        if state == "InDHT":
            string += f"{i}: ({key}, {peerAddr}, {pPort})\n"
            i += 1
    return string 


class peerInfo: 
    def __init__(self, peerAddr, mPort, pPort, state):
        self.peerAddr = peerAddr
        self.mPort = mPort
        self.pPort = pPort
        self.state = state

peerInfoHash = {}
dhtTable = []

def die_with_error(error_message):
    print(error_message, file=sys.stderr)
    sys.exit(1)

if len(sys.argv) > 1: 
    PORT = int(sys.argv[1])
else:
    die_with_error("[ERROR] Arguments Invalid")

IP = socket.gethostbyname(socket.gethostname())

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

sock.bind((IP, PORT)) 

print(f"[LISTENING ON] = {IP}, {PORT}\n")

while True: 
    data, addr = sock.recvfrom(1024) 
    data = data.decode()
    split = data.split()
    if(data == "close"): 
        print(f"Connection Terminated By: {addr}")
        break
    print(f"Recieved command: {data}")
    print(f"Recieved from: {addr}\n")

    if(data == "close"): 
        break

    if setupInitiated == False: 
        if len(split) == 5:
            if(split[0] == "register" and split[1].isalpha() and len(split[1]) <= 15 and ipaddress.IPv4Address(split[2]) and int(split[3]) and int(split[4])): 
                added = True 

                if split[1] in peerInfoHash:
                    added = False 

                if added: 
                    peer = peerInfo(split[2], split[3], split[4], "Free") 
                    peerInfoHash[split[1]] = peer
                    sock.sendto(b"\n[SUCCESS]", addr)
                    print("[SUCCESS]\n")
                else: 
                    sock.sendto(b"\n[FAILURE]", addr)
                    print("[FAILURE]\n")
            else: 
                sock.sendto(b"\n[FAILURE]", addr)
                print("[FAILURE]\n")
        
    if setupInitiated == False: 
        if(len(split) == 4):
            if(split[0] == "setup-dht" and split[1].isalpha() and int(split[3])):
                if split[1] in peerInfoHash and int(split[2]) >= 3 and len(peerInfoHash) >= 3 and dhtSetup == False: 
            
                    peerInfoHash[split[1]].state = "Leader"

                    ranmdomDhtPeers = {k: name for k, name in peerInfoHash.items() if name.state == "Free"}

                    dhtPeers = random.sample(list(ranmdomDhtPeers.keys()), (int(split[2]) - 1))

                    for peer in dhtPeers:
                        peerInfoHash[peer].state = "InDHT"

                    tupleMsg = createTuple(peerInfoHash)
                    

                    for peer, info in peerInfoHash.items(): 
                        if info.state == "Leader": 
                            dhtTable.append(peer)

                    for peer, info in peerInfoHash.items(): 
                        if info.state == "InDHT": 
                            dhtTable.append(peer) 

                    dhtSetup = True
                    setupInitiated = True

                    sock.sendto(f"\n[SUCCESS]\n\n{tupleMsg}".encode(), addr)
                    print(f"[SUCCESS]\n\n{tupleMsg}\n")
                else: 
                    sock.sendto(b"\n[FAILURE]", addr)
                    print("[FAILURE]\n")

    if len(split) == 2 and split[0] == "query-dht":
        if split[1] in peerInfoHash: 
            queriedPeer = peerInfoHash[split[1]]
        if split[1] in peerInfoHash and setupInitiated == False and dhtSetup == True and queriedPeer.state == "Free":
            randomPeerKey = random.choice(dhtTable)

            randomPeerInfo = peerInfoHash[randomPeerKey]
            message = f"\n[SUCCESS]\n\n({randomPeerKey}, {randomPeerInfo.peerAddr}, {randomPeerInfo.pPort})"
            message = message.encode()
            sock.sendto(message, addr)
        else:
            sock.sendto(b"\n[FAILURE]", addr)
            print("[FAILURE]\n")

    if len(split) == 2 and split[0] == "[dht-complete]" and split[1] == dhtTable[0] and dhtSetup == True:
            setupInitiated = False
            sock.sendto(b"\n[SUCCESS]", addr)
            print("[SUCCESS]\n")

sock.close()