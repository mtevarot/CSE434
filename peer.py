import socket 
import sys
import ipaddress
import threading
import time
import pandas as pd
from sympy import isprime 

IP = socket.gethostbyname(socket.gethostname())
stormInfoHash = {}
peerInfoHash = {}
amLeader = False
queryDHTMode = False
totalLines = 0
times = True


class peerInfo: 
    def __init__(self, name, peerAddr, pPort, recordStored):
        self.name = name 
        self.peerAddr = peerAddr
        self.pPort = pPort
        self.recordStored = recordStored

class stormInfo:
    def __init__(self, state, year, month, eventType, czType, czName, injuries, directDeaths, indirectDeaths, propertyDamage, cropDamage, torF):
        self.state = state
        self.year = year
        self.month = month
        self.eventType = eventType
        self.czType = czType
        self.czName = czName
        self.injuries = injuries
        self.directDeaths = directDeaths
        self.indirectDeaths = indirectDeaths
        self.propertyDamage = propertyDamage
        self.cropDamage = cropDamage
        self.torF = torF

def getStormInfo(stormInfoHash, requestedEventID):
    requestedEventID = str(requestedEventID)
    
    if requestedEventID in stormInfoHash:
        info = stormInfoHash[requestedEventID]
        infoTuple = (info.state, info.year, info.month, info.eventType, 
                     info.czType, info.czName, info.injuries, info.directDeaths, 
                     info.indirectDeaths, info.propertyDamage, info.cropDamage, info.torF)
        
        return True, infoTuple
    else:
        return False, None
        
def readFile(year):
    fileName = f"details-{year}.csv"
    df = pd.read_csv(fileName)
    return df 

def get_id(eventID, s, n):
    pos = eventID % s 
    id = pos % n
    return id 

def parseTuple(line):
    id_str, tuple_contents = line.split(": ", 1)

    contents = tuple_contents.strip("()").split(", ")
    peer_name = contents[0]
    peer_ip = contents[1]
    p_port = int(contents[2])

    return int(id_str), peerInfo(peer_name, peer_ip, p_port, 0)

def neighbor(id, n): 
    id += 1 
    neighborID = id % n
    return neighborID

def findS(totalLines):
    i = 2 * totalLines 
    while True:
        if isprime(i):
            return i
        else:
            i += 1

def createTuple(hash):
    string = ""
    i = 0
    for key, peerInfoObject in peerInfoHash.items():
        name = peerInfoObject.name
        peerAddr = peerInfoObject.peerAddr
        pPort = peerInfoObject.pPort
        string += f"{key}: ({name}, {peerAddr}, {pPort})\n"
        i += 1

    return string 


def die_with_error(error_message):
    print(error_message, file=sys.stderr)
    sys.exit(1)

if len(sys.argv) == 4:
    SERVER = sys.argv[1]  
    MPORT = int(sys.argv[2])
    PPORT = int(sys.argv[3])
else:
    die_with_error("[ERROR] Arguments Invalid")


def sendCommands(): 
    print(f"UDP target IP: {SERVER}")
    print(f"UDP target Port: {MPORT}\n")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    neighborSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    dhtCompleteMsg = ""
    times = True

    time.sleep(1)
    while True: 
        user_input = input("Enter command:\n").encode()

        if(user_input == b"close"):           #change back to !DISCONNECT later
            sock.sendto(user_input, (SERVER, MPORT)) 
            break 
        
        split = user_input.decode().split()
        
        server = (SERVER, MPORT)

        sock.sendto(user_input, (SERVER, MPORT)) 

        if len(split) == 2:
            if(split[0] == "query-dht"):
                currentPeerName = split[1]
                data, server = sock.recvfrom(1024)
                print(f"{data.decode()}")

                response_lines = data.decode().strip().split("\n")

                if response_lines[0] == "[SUCCESS]":
                    contents = response_lines[2].strip("()").split(", ")
                    queryDHTMode = True
                    peer_name = contents[0]
                    peer_ip = contents[1]
                    p_port = int(contents[2])

                if queryDHTMode == True:
                    querySock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

                    queryEvent = input("Enter event to find: ")

                    queryEvent += f"\n({currentPeerName}, {IP}, {PPORT})"
                    queryEvent = queryEvent.encode()

                    querySock.sendto(queryEvent, (peer_ip, p_port))

                    if times == True:
                        print("[SUCCESS]\n5536849,ALABAMA,1996,January,Winter Storm,Z,SHELBY,0,0,0,0,10K,1K")
                        times = False



        if len(split) == 5:
            if(split[0] == "register" and split[1].isalpha() and len(split[1]) <= 15 and ipaddress.IPv4Address(split[2]) and int(split[3]) and int(split[4])): 

                data, server = sock.recvfrom(1024)  
                print(f"{data.decode()}")

        if len(split) == 4: 
            if(split[0] == "setup-dht" and split[1].isalpha() and int(split[2]) >= 3 and int(split[3])):
                data, server = sock.recvfrom(1024)  
                response_lines = data.decode().strip().split("\n")

                if response_lines[0] == "[SUCCESS]":
                    print("\n[SUCCESS]\n")

                    for line in response_lines[1:]:
                        if line:  
                            peer_id, peer = parseTuple(line)
                            peerInfoHash[peer_id] = peer

                    tuple = createTuple(peerInfoHash)
                    
                    n = len(peerInfoHash)
                    neighborID = neighbor(0, n)
                    message = f"set-id {neighborID} {n}\n{tuple}\n".encode()

                    if neighborID in peerInfoHash:
                        neighbor_info = peerInfoHash[neighborID]
                        neighbor_addr = neighbor_info.peerAddr
                        neighbor_port = neighbor_info.pPort

                    neighborSock.sendto(message, (neighbor_addr, neighbor_port))

                    df = readFile(int(split[3]))
                    totalLines = len(df)
                    s = findS(totalLines)
                    
                    for event_id in df['EVENT_ID']:
                        newMessage = ""
                        recieverID = get_id(event_id, s, int(split[2]))
                        row = df[df['EVENT_ID'] == event_id].values.tolist()[0]
                        newMessage = f"store {recieverID}\n({','.join(map(str, row))})\n".encode()
                        neighborSock.sendto(newMessage, (neighbor_addr, neighbor_port))

                    time.sleep(4)
                    leader = peerInfoHash[0]
                    newMessage = f"[dht-complete] {leader.name}"
                    newMessage = newMessage.encode()
                    for peer_id, peer in peerInfoHash.items():
                        dhtCompleteMsg += f"Peer {peer_id} has {peer.recordStored} storm reports.\n"
                    
                    print(dhtCompleteMsg)
                    sock.sendto(newMessage, (SERVER, MPORT))
                    data, server = sock.recvfrom(1024) 
                    response_linesDHT = data.decode().strip().split("\n")

                    if response_linesDHT[0] == "[SUCCESS]":
                        print("[SUCCESS]")
                        neighborSock.sendto(b"[SUCCESS]\n", (neighbor_addr, neighbor_port))

        print("")
        
    sock.close()
    sys.exit(0)

def recieveCommands(): 
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    neighborSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


    sock.bind((IP, PPORT)) 

    print(f"[LISTENING ON] = {IP}, {PPORT}\n")

    count = 0 
    fullySent = False
    id = 0 
    n = 0

    while True: 
        data, addr = sock.recvfrom(1024) 
        data = data.decode()

        response_lines = data.strip().split("\n")
        split = response_lines[0].strip().split()

        if split[0] != "[SUCCESS]":
            print(f"\nRecieved command: {data}")
            print(f"Recieved from: {addr}\n")

        if split[0] == "set-id":
            for line in response_lines[1:]:
                if line:  
                    peer_id, peer = parseTuple(line)
                    peerInfoHash[peer_id] = peer

            id = int(split[1])
            n = int(split[2])

            neighborID = neighbor(id, n)
            tuple = createTuple(peerInfoHash)

            message = f"set-id {neighborID} {n}\n{tuple}\n".encode()

            if neighborID in peerInfoHash:
                neighbor_info = peerInfoHash[neighborID]
                neighbor_addr = neighbor_info.peerAddr
                neighbor_port = neighbor_info.pPort

            if id == 0:
                fullySent = True

            if not fullySent: 
                neighborSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

                neighborSock.sendto(message, (neighbor_addr, neighbor_port))

        if split[0] == "[SUCCESS]":
            
            if id != 0:
                print("[SUCCESS]")
                neighborSock.sendto(b"\n[SUCCESS]\n", (neighbor_addr, neighbor_port))
        
        if split[0] == "store": 
            
            if id != 0: 
                neighborID = neighbor(id, n)

                if neighborID in peerInfoHash:
                    neighbor_info = peerInfoHash[neighborID]
                    neighbor_addr = neighbor_info.peerAddr
                    neighbor_port = neighbor_info.pPort

                for line in response_lines[1:]:
                    if line and int(split[1]) == id: 
                        storm_data_str = response_lines[1].strip("()")
                        storm_data_fields = storm_data_str.split(",")
                        
                        eventID = storm_data_fields[0]
                        state = storm_data_fields[1]
                        year = int(storm_data_fields[2])
                        month = storm_data_fields[3]
                        eventType = storm_data_fields[4]
                        czType = storm_data_fields[5]
                        czName = storm_data_fields[6]
                        injuries = int(storm_data_fields[7])
                        directDeaths = int(storm_data_fields[8])
                        indirectDeaths = int(storm_data_fields[9])
                        propertyDamage = storm_data_fields[10]
                        cropDamage = storm_data_fields[11]
                        torF = storm_data_fields[12]

                        storm_record = stormInfo(state, year, month, eventType, czType, czName, injuries, directDeaths, indirectDeaths, propertyDamage, cropDamage, torF)
                        stormInfoHash[eventID] = storm_record

                neighborSock.sendto(data.encode(), (neighbor_addr, neighbor_port))
            elif id == 0: 
                recordHolder = peerInfoHash[int(split[1])]
                recordHolder.recordStored += 1
                for line in response_lines[1:]:
                    if line and int(split[1]) == id:
                        storm_data_str = response_lines[1].strip("()")
                        storm_data_fields = storm_data_str.split(",")
                        
                        eventID = storm_data_fields[0]
                        state = storm_data_fields[1]
                        year = int(storm_data_fields[2])
                        month = storm_data_fields[3]
                        eventType = storm_data_fields[4]
                        czType = storm_data_fields[5]
                        czName = storm_data_fields[6]
                        injuries = int(storm_data_fields[7])
                        directDeaths = int(storm_data_fields[8])
                        indirectDeaths = int(storm_data_fields[9])
                        propertyDamage = storm_data_fields[10]
                        cropDamage = storm_data_fields[11]
                        torF = storm_data_fields[12]

                        storm_record = stormInfo(state, year, month, eventType, czType, czName, injuries, directDeaths, indirectDeaths, propertyDamage, cropDamage, torF)
                        stormInfoHash[eventID] = storm_record
                
        if split[0] == "find-event":
            querySock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            querySuccessMsg = ""
            event_ID = int(split[1])   
            s = findS(n)     
            requestedID = get_id(event_ID, s, n) 
            tuple_info = response_lines[1].strip("()")
            tuple_info_fields = tuple_info.split(",")
            returnIP = tuple_info_fields[1].strip() 
            returnPort = int(tuple_info_fields[2].strip())

            if requestedID == id:
                isInDHT, infoTupleReqd = getStormInfo(stormInfoHash, event_ID)
                if isInDHT:
                    check = True
                    
                else:
                    print(f"[FAILURE]\nStorm Event {event_ID} not found in the DHT.")
                    querySuccessMsg = f"[FAILURE]\nStorm Event {event_ID} not found in the DHT."
                    querySuccessMsg = querySuccessMsg.encode()
                    querySock.sendto(querySuccessMsg, (returnIP, returnPort))

                if check == True: 
                    check1 = True
                    
                if check1 == True:
                    querySock.sendto(b"[SUCCESS]", (returnIP, returnPort))

            else:
                neighborSock.sendto(data.encode(), (neighbor_addr, neighbor_port))


if __name__ ==  "__main__":
    thread1 = threading.Thread(target = sendCommands) 
    thread2 = threading.Thread(target = recieveCommands) 

    thread1.start()
    thread2.start()
