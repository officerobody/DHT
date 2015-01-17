# February 21, 2013
# Alexander Zaranek (with Alex Halluwushska and Karl Good)

import threading, re, Queue, time

# Number of peers = 2^N
N=10

class Peers(threading.Thread):

    def __init__(self, delete_queue):
        super(Peers, self).__init__()
        self.delete_queue = delete_queue
        self.stoprequest = threading.Event()
        self.connections = []       # List of all the connections the peer has to other peers
        self.successor = None       # The peers succuessor initially set to nothing
        self.predecessor = None     # The peers predecessor initially set to nothing 
        self.dict = {}              # Dictionary to store the (key,value) pairs

    def run(self):
        MyName = self.getName()
        stop = 0
        while (stop == 0):
            if not self.delete_queue.empty():
                DeletePeer = self.delete_queue.get()
                if (DeletePeer != MyName):
                    self.delete_queue.put(DeletePeer)
                else:
                    stop = 1
                # here peer works on other stuff...
         
    # Function that is ran when the peer joins the network. It will first locate the appropriate successor of
    # the peer and then rearrange the connections of surrounding peers in order to place its self at that location.
    # The peer will then determine the rest of its connections using the function c + 2^i (i = 0...N).
    def joinNetwork(self,i0): 
        tempConnections = [] # Temporary list of connections

        for i in i0.getConnections():
            tempConnections.append(i) # Set to the tempory list to the connections of i0
            
        for i in i0.getConnections():
            if (int(self.getName()) < int(i.getName())):
                tempConnections.remove(i)   # If one of the peers i0 connects to has a greater
                                            # id than the joining peer, remove it from the peer list

        farPeer = int(i0.getName())     # The farthest peer from the current peer (defaults to itself)
        nextPeer = i0                   # The successor of the new peer (defaults to the current peer)
        for i in tempConnections:       # Goes through all possible successors
            if (int(i.getName()) > farPeer):
                farPeer = int(i.getName())   # Sets the new farthest peer 
                nextPeer = i                 # Farthest peer set as current successor

        if (farPeer != int(i0.getName())):  # if farPeer was updated, recursively call joinNetwork
            self.joinNetwork(nextPeer)

        else: # If farPeer was not updated, the appropriate successor of the joining peer has been found

            # Rearranges the connections of the predeccesor and successor of the joining peer
            # so that they correctly reflect the newly added peer
            nextPeer.removeConnection(nextPeer.getSuccessor())
            nextPeer.getSuccessor().removeConnection(nextPeer)
            self.createConnection(nextPeer) 
            nextPeer.createConnection(self) 
            self.createConnection(nextPeer.getSuccessor())
            nextPeer.getSuccessor().createConnection(self)
            self.setSuccessor(nextPeer.getSuccessor())
            nextPeer.getSuccessor().setPredecessor(self)
           
            nextPeer.setSuccessor(self)
            self.setPredecessor(nextPeer)

            # Checks the (key,value) pairs of the successor to see if any of the pairs
            # need to be moved down to the newly added node
            keysMoved = []
            for i, j in self.getSuccessor().getPairs().iteritems():
                if (i >= int(self.getName())):
                    self.addPair(i,j)
                    keysMoved.append(i)
            for i in keysMoved:
                self.getSuccessor().removePair(i)

            # New peer checks if peers with 2^N distance away exist and then forms connections
            for i in range(1,N):    # Loops through log N amount
                peerDistance = ((int(self.getName()) + 2**i) % 2**N) # Determine distance of the peer it needs to connect to
                truth,peerConnect = peerExists(peerDistance,i0)      # If that peer exists set truth to true
                if (truth):
                    self.createConnection(peerConnect) # Creates a connection with that peer 

    def getSuccessor(self):     # Return the successor of the peer
        return self.successor
    
    def setSuccessor(self,p):   # Sets the successor of the peer
        self.successor = p

    def getPredecessor(self):   # Returns the predecsessor of the peer
        return self.predecessor
    
    def setPredecessor(self,p): # Sets the predecessor of the peer
        self.predecessor = p
        
    def getConnections(self):   # Returns the connections of the peer
        return self.connections
    
    def createConnection(self,p):   # Creates a connection from the current node to p
        self.connections.append(p)
              
    def removeConnection(self,p):   # Removes the connection form self to p
        self.connections.remove(p)

    def deletePeer(self):   # Deletes the peer
        self.getPredecessor().createConnection(self.getSuccessor())
        self.getPredecessor().setSuccessor(self.getSuccessor())
        self.getSuccessor().setPredecessor(self.getPredecessor())
        peerList = self.getConnections()
        for i in self.getConnections(): # Removes its connections
            self.removeConnection(i)
          
    def addPair(self,key,value): # Adds a (key, value) pair to the peer
        self.dict[key]=value

    def removePair(self,key): # Removes a (key, value) pair from a peer
        del self.dict[key]
      
    def getPairs(self): # Returns the (key, value) pairs of the peer
        return self.dict

# Function to determine if a node at the given distance value dist from node i0 exists.
# Will return a boolean value of true if it exists and the id of the node at the given distance.
# If no such node exists, it will return false with no node given.
def peerExists(dist,i0):
    minDist = abs(dist - int(i0.getName())) # Finds the minimum distance from peer i0
    nextPeer = i0
    
    for i in i0.getConnections():       # Cycles through current nodes peers
        if (int(i.getName()) == dist):  # If the required peer is found in list of connections, return
            return True,i

        else:
            tempDist = abs(dist - int(i.getName())) # 
            if (tempDist < minDist):
                minDist = tempDist
                nextPeer = i

    if (nextPeer != i0):
        return peerExists(dist,nextPeer)    # Recursively calls the next peer to check its peers for
                                            # the node being searched for.
    else:
        return False,None

# Function that finds a given (key, value) pair starting at the node i0. Will recursively search nodes until it
# it reaches the peer with the same id as the key. If there is no such peer, it will find the peer
# with the closest id that is greater then the value of the key and attempt to find the (key, value) there.
# Once found, it will print its value and the node it was found.
def find(i0,key):
    tempList = []    # Will contain list of peers that will potentialy be searched

    # If current node has same id as the key, it will search its pairs for the key
    if (int(key) == int(i0.getName())):
        for i, j in i0.getPairs().iteritems():
            if (i == int(key)):
                print "Key found at peer", nextPeer.getName(), "with the value: ", j
    else:
        for i in i0.getConnections():   # Fills search list with peers of current node
            tempList.append(i)
        for i in i0.getConnections():   # If any peer id is less then the key, it is removed from the list
            if (int(key) > int(i.getName())):
                tempList.remove(i)
        
        if (len(tempList) == 0):        # If all peer id's are less then the key, it will search the current peers
            farPeer = int(i0.getName())     # Holds the farthest peer
            nextPeer = i0                   # Farthest peer will be the peer to be searched next
            for i in i0.getConnections():
                if (int(i.getName()) > farPeer):
                    farPeer = int(i.getName())
                    nextPeer = i
            if (farPeer != int(i0.getName())):  # If the farthest peer wasn't updated, it will be searched
                find(nextPeer,key)

            else:
                if (int(key) > int(nextPeer.getName())):    # Ensures the key is less than the node's id
                    nextPeer = nextPeer.getSuccessor()
                for i, j in nextPeer.getPairs().iteritems():    # Searches the peer for the key
                    if (i == int(key)):
                        print "Key found at peer", nextPeer.getName(), "with the value: ", j

        else:                           
            minDist= abs(int(key) - int(i0.getName()))  # Holds the closest peer
            nextPeer = i0                               # Closest peer will be the peer to be searched next
            for i in tempList:
                tempDist = abs(int(key) - int(i.getName()))     # Finds the closest peer
                if (tempDist < minDist):
                    minDist = tempDist
                    nextPeer = i
            if (minDist != abs(int(key) - int(i0.getName()))):  # If the closest peer wasn't updated, it will be searched
                find(nextPeer,key)

            else:
                if (int(key) > int(nextPeer.getName())):    # Ensures the key is less than the node's id
                    nextPeer = nextPeer.getSuccessor()
                for i, j in nextPeer.getPairs().iteritems():    # Searches the peer for the key
                    if (i == int(key)):
                        print "Key found at peer", nextPeer.getName(), "with the value: ", j

# Function that inserts a given (key, value) pair starting at the node i0. Will recursively search nodes until it
# it reaches the peer with the same id as the key. If there is no such peer, it will find the peer
# with the closest id that is greater then the value of the key and will insert the (key, value) there.
def insertPair(i0,key,value):
    tempList = []   # Will contain list of peers that will potentialy be searched

    # If current node has same id as the key, it will insert the pair at that node
    if (int(key) == int(i0.getName())):
        i0.addPair(int(key),value)
    else:
        for i in i0.getConnections():   # Fills search list with peers of current node
            tempList.append(i)
        for i in i0.getConnections():   # If any peer id is less then the key, it is removed from the list
            if (int(key) > int(i.getName())):
                tempList.remove(i)
        
        if (len(tempList) == 0):            # If all peer id's are less then the key, it will search the current peers
            farPeer = int(i0.getName())     # Holds the farthest peer
            nextPeer = i0                   # Farthest peer will be the peer to be contacted next
            for i in i0.getConnections():
                if (int(i.getName()) > farPeer):
                    farPeer = int(i.getName())
                    nextPeer = i
            if (farPeer != int(i0.getName())):    # If the farthest peer wasn't updated, it will be contacted
                insertPair(nextPeer,key,value)

            else:
                if (int(key) > int(nextPeer.getName())):    # Ensures the key is less than the node's id
                    nextPeer = nextPeer.getSuccessor()
                nextPeer.addPair(int(key),value)

        else:           
            minDist= abs(int(key) - int(i0.getName()))      # Holds the closest peer
            nextPeer = i0                                   # Closest peer will be the peer to be contacted next
            for i in tempList:
                tempDist = abs(int(key) - int(i.getName()))      # Finds the closest peer
                if (tempDist < minDist):
                    minDist = tempDist
                    nextPeer = i
            if (minDist != abs(int(key) - int(i0.getName()))):
                insertPair(nextPeer,key,value)

            else:
                if (int(key) > int(nextPeer.getName())):     # Ensures the key is less than the node's id
                    nextPeer = nextPeer.getSuccessor()
                nextPeer.addPair(int(key),value)

# Print currently active peers (i.e., threads)
# 'MainThread' always included, but we do no show/count it
def PrintInfoCurrentPeers():
    peer_count = threading.active_count()-1
    print "Currently %d peers: " % peer_count
    for p in threading.enumerate():
        name = p.getName()
        if (name != 'MainThread'):
            print 'Peer:', name

# data structure containing peers to delete
delete_queue = Queue.Queue()

# Initializes several peers (with ids 1, 4, and 6) for testing as well as the node i0,
# which is required for connecting a new peer from the outside.
i0 = Peers(delete_queue)
i0.setName("0")
i0.start()

i1 = Peers(delete_queue)
i1.setName("1")
i0.createConnection(i1)
i1.start()


i4 = Peers(delete_queue)
i4.setName("4")
i4.createConnection(i1)
i1.createConnection(i4)
i1.setSuccessor(i4)
i4.setPredecessor(i1)
i4.start()

i6 = Peers(delete_queue)
i6.setName("6")
i6.createConnection(i4)
i4.createConnection(i6)
i6.createConnection(i1)
i1.createConnection(i6)
i4.setSuccessor(i6)
i6.setPredecessor(i4)
i6.setSuccessor(i1)
i1.setPredecessor(i6)
i6.start()

# Main command loop
while 1:
    execute_command = 1
    PrintInfoCurrentPeers()
    command = raw_input("Enter command: ")
    words = command.split()

    if (len(words) == 0):
        execute_command = 0

    if (len(words)==0):
        execute_command=0
    elif(execute_command==1 and words[0] == "addp"): # Adds a peer with given id value
        execute_command=0
        if (int(words[1])<=2**N): # Determines if the peer id isnt greater than the max number of peers
            if (peerExists(int(words[1]),i0)): # Determines if the peer already exists
                p=Peers(delete_queue)
                p.setName(words[1]) 
                p.joinNetwork(i0)
                p.start()
            else:
                print "A peer with the same id already exists"
        else:
            print "Add a peer in the range from 1 to ", (2**N)

    elif (execute_command == 1 and words[0] == "delp"): # Deletes a given peer
        execute_command = 0
        delete_queue.put(words[1])

    elif (execute_command == 1 and words[0] == "delallp"): # Deletes all peers
        execute_command = 0
        for p in threading.enumerate():
            delete_queue.put(p.getName())

    elif(execute_command==1 and words[0] == "insert"): # Inserts a (key, value) pair into the network
        execute_command = 0
        truth,peer = peerExists(int(words[1]),i0)
        if (truth):
            insertPair(peer,words[2],words[3])
        else:
            print "Peer does not exist"

    elif(execute_command==1 and words[0] == "find"): # Finds a (key, value) pair in the the network
        execute_command = 0
        truth,peer = peerExists(int(words[1]),i0)
        if (truth):
            find(peer,words[2])
        else:
            print "Peer does not exist"

    elif(execute_command==1 and words[0] == "pairs"): # Displays all (key, value) pairs of a given peer
        truth,peer = peerExists(int(words[1]),i0)
        if (truth):
            print peer.getPairs()
        else:
            print "Peer does not exist"
        
    elif(execute_command==1 and words[0] == "peers"): # Displays all connections of a given peer
        truth,peer = peerExists(int(words[1]),i0)
        if (truth):
            print peer.getConnections()
        else:
            print "Peer does not exist"

    elif (execute_command == 1):
        print "unknown command"
