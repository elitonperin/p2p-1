import socket
import struct
import threading
import time
import traceback


def btdebug(msg):
    """ Prints a messsage to the screen with the name of the current thread """
    print("[%s] %s" % (str(threading.currentThread().getName()), msg))


class Peer:
    """ Implements the core functionality that might be used by a peer in a
    P2P network.

    """

    def __init__(self, maxpeers, serverport, myid=None, serverhost=None):
        """ Initializes a peer servent (sic.) with the ability to catalog
        information for up to maxpeers number of peers (maxpeers may
        be set to 0 to allow unlimited number of peers), listening on
        a given server port , with a given canonical peer name (id)
        and host address. If not supplied, the host address
        (serverhost) will be determined by attempting to connect to an
        Internet host like Google.

        """
        self.debug = False

        self.maxpeers = int(maxpeers)
        self.serverport = int(serverport)
        if serverhost:
            self.serverhost = serverhost
        else:
            self.__initserverhost()

        if myid:
            self.myid = myid
        else:
            self.myid = '%s:%d' % (self.serverhost, self.serverport)

        self.peerlock = threading.Lock()  # ensure proper access to
        # peers list (maybe better to use
        # threading.RLock (reentrant))
        self.peers = {}  # peerid ==> (host, port) mapping
        self.shutdown = False  # used to stop the main loop

        self.handlers = {}
        self.router = None

    def __initserverhost(self):
        """ Attempt to connect to an Internet host in order to determine the
        local machine's IP address.

        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("www.google.com", 80))
        self.serverhost = s.getsockname()[0]
        s.close()

    def log(self, msg):
        if self.debug:
            btdebug(msg)

    def __handlepeer(self, clientsock):
        """
        handlepeer( new socket connection ) -> ()

        Dispatches messages from the socket connection
        """

        self.log('New child ' + str(threading.currentThread().getName()))
        self.log('Connected ' + str(clientsock.getpeername()))

        host, port = clientsock.getpeername()
        peerconn = PeerConnection(None, host, port, clientsock, self.debug)

        try:
            msgtype, msgdata = peerconn.recvdata()
            if msgtype:
                msgtype = msgtype.upper()
            if msgtype not in self.handlers:
                self.log('Not handled: %s: %s' % (msgtype, msgdata))
            else:
                self.log('Handling peer msg: %s: %s' % (msgtype, msgdata))
                self.handlers[msgtype](peerconn, msgdata)
        except KeyboardInterrupt:
            raise
        except:
            if self.debug:
                traceback.print_exc()

        self.log('Disconnecting ' + str(clientsock.getpeername()))
        peerconn.close()

    def __runstabilizer(self, stabilizer, delay):
        while not self.shutdown:
            stabilizer()
            time.sleep(delay)

    def setmyid(self, myid):
        self.myid = myid

    def startstabilizer(self, stabilizer, delay):
        """ Registers and starts a stabilizer function with this peer.
        The function will be activated every <delay> seconds.

        """
        t = threading.Thread(
            target=self.__runstabilizer, args=[stabilizer, delay])
        t.start()

    def addhandler(self, msgtype, handler):
        """ Registers the handler for the given message type with this peer """
        assert len(msgtype) == 4
        self.handlers[msgtype] = handler

    def addrouter(self, router):
        """ Registers a routing function with this peer. The setup of routing
        is as follows: This peer maintains a list of other known peers
        (in self.peers). The routing function should take the name of
        a peer (which may not necessarily be present in self.peers)
        and decide which of the known peers a message should be routed
        to next in order to (hopefully) reach the desired peer. The router
        function should return a tuple of three values: (next-peer-id, host,
        port). If the message cannot be routed, the next-peer-id should be
        None.

        """
        self.router = router

    def addpeer(self, peerid, host, port):
        """ Adds a peer name and host:port mapping to the known list of peers.

        """
        if peerid not in self.peers and (self.maxpeers == 0
                                         or len(self.peers) < self.maxpeers):
            self.peers[peerid] = (host, int(port))
            return True
        else:
            return False

    def getpeer(self, peerid):
        """ Returns the (host, port) tuple for the given peer name """
        assert peerid in self.peers  # maybe make this just a return NULL?
        return self.peers[peerid]

    def removepeer(self, peerid):
        """ Removes peer information from the known list of peers. """
        if peerid in self.peers:
            del self.peers[peerid]

    def getpeerids(self):
        """ Return a list of all known peer id's. """
        return self.peers.keys()

    def numberofpeers(self):
        """ Return the number of known peer's. """
        return len(self.peers)

    def maxpeersreached(self):
        """ Returns whether the maximum limit of names has been added to the
        list of known peers. Always returns True if maxpeers is set to
        0.

        """
        assert self.maxpeers == 0 or len(self.peers) <= self.maxpeers
        return self.maxpeers > 0 and len(self.peers) == self.maxpeers

    def makeserversocket(self, port, backlog=5):
        """ Constructs and prepares a server socket listening on the given
        port.

        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', port))
        s.listen(backlog)
        return s

    def sendtopeer(self, peerid, msgtype, msgdata, waitreply=True):
        """
        sendtopeer( peer id, message type, message data, wait for a reply )
         -> [ ( reply type, reply data ), ... ]

        Send a message to the identified peer. In order to decide how to
        send the message, the router handler for this peer will be called.
        If no router function has been registered, it will not work. The
        router function should provide the next immediate peer to whom the
        message should be forwarded. The peer's reply, if it is expected,
        will be returned.

        Returns None if the message could not be routed.
        """

        if self.router:
            nextpid, host, port = self.router(peerid)
        if not self.router or not nextpid:
            self.log('Unable to route %s to %s' % (msgtype, peerid))
            return None
        # host,port = self.peers[nextpid]
        return self.connectandsend(
            host, port, msgtype, msgdata, pid=nextpid, waitreply=waitreply)

    def connectandsend(self,
                       host,
                       port,
                       msgtype,
                       msgdata,
                       pid=None,
                       waitreply=True):
        """
        connectandsend( host, port, message type, message data, peer id,
        wait for a reply ) -> [ ( reply type, reply data ), ... ]

        Connects and sends a message to the specified host:port. The host's
        reply, if expected, will be returned as a list of tuples.

        """
        msgreply = []
        try:
            peerconn = PeerConnection(pid, host, port, debug=self.debug)
            peerconn.senddata(msgtype, msgdata)
            self.log('Sent %s: %s' % (pid, msgtype))

            if waitreply:
                onereply = peerconn.recvdata()
                while onereply != (None, None):
                    msgreply.append(onereply)
                    self.log('Got reply %s: %s' % (pid, str(msgreply)))
                    onereply = peerconn.recvdata()
            peerconn.close()
        except KeyboardInterrupt:
            raise
        except:
            if self.debug:
                traceback.print_exc()

        return msgreply

    def checklivepeers(self):
        """ Attempts to ping all currently known peers in order to ensure that
        they are still active. Removes any from the peer list that do
        not reply. This function can be used as a simple stabilizer.

        """
        todelete = []
        for pid in self.peers:
            isconnected = False
            try:
                self.log('Check live %s' % pid)
                host, port = self.peers[pid]
                peerconn = PeerConnection(pid, host, port, debug=self.debug)
                peerconn.senddata('PING', '')
                isconnected = True
            except:
                todelete.append(pid)
            if isconnected:
                peerconn.close()

        self.peerlock.acquire()
        try:
            for pid in todelete:
                if pid in self.peers:
                    del self.peers[pid]
        finally:
            self.peerlock.release()

    def mainloop(self):
        s = self.makeserversocket(self.serverport)
        s.settimeout(2)
        self.log('Server started: %s (%s:%d)' % (self.myid, self.serverhost,
                                                 self.serverport))

        while not self.shutdown:
            try:
                # FIXME: getting sick of this being printed with every loop
                # self.log('Listening for connections...')
                clientsock, clientaddr = s.accept()
                clientsock.settimeout(None)

                t = threading.Thread(
                    target=self.__handlepeer, args=[clientsock])
                t.start()
            except KeyboardInterrupt:
                print('KeyboardInterrupt: stopping mainloop')
                self.shutdown = True
                continue
            except:
                if self.debug:
                    # FIXME: getting sick of this being printed with every loop
                    # traceback.print_exc()
                    continue

        self.log('Main loop exiting')

        s.close()


class PeerConnection:
    def __init__(self, peerid, host, port, sock=None, debug=False):
        # any exceptions thrown upwards

        self.id = peerid
        self.debug = debug

        if not sock:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.s.connect((host, int(port)))
        else:
            self.s = sock

        self.sd = self.s.makefile('rwb', 0)

    def __makemsg(self, msgtype, msgdata):
        msglen = len(msgdata)

        msg = struct.pack("!4sL%ds" % msglen, msgtype, msglen, msgdata)
        return msg

    def log(self, msg):
        if self.debug:
            btdebug(msg)

    def senddata(self, msgtype, msgdata):
        """
        senddata( message type, message data ) -> boolean status

        Send a message through a peer connection. Returns True on success
        or False if there was an error.
        """
        # Make sure this is a bytestring
        if type(msgdata) == str:
            msgdata = msgdata.encode()
        try:
            msg = self.__makemsg(msgtype, msgdata)
            self.sd.write(msg)
            self.sd.flush()
        except KeyboardInterrupt:
            raise
        except:
            if self.debug:
                traceback.print_exc()
            return False
        return True

    def recvdata(self):
        """
        recvdata() -> (msgtype, msgdata)

        Receive a message from a peer connection. Returns (None, None)
        if there was any error.
        """

        try:
            msgtype = self.sd.read(4)
            if not msgtype:
                return None, None

            lenstr = self.sd.read(4)
            msglen = int(struct.unpack("!L", lenstr)[0])
            msg = b""

            while len(msg) != msglen:
                data = self.sd.read(min(2048, msglen - len(msg)))
                if not len(data):
                    break
                msg += data

            if len(msg) != msglen:
                return None, None

        except KeyboardInterrupt:
            raise
        except:
            if self.debug:
                traceback.print_exc()
            return None, None

        return (msgtype, msg)

    def close(self):
        """
        close()

        Close the peer connection. The send and recv methods will not work
        after this call.
        """

        self.s.close()
        self.s = None
        self.sd = None

    def __str__(self):
        return "|%s|" % self.id


PEER_NAME = b"NAME"  # request a peer's canonical id
LIST_PEERS = b"LIST"
INSERT_PEER = b"JOIN"
QUERY = b"QUER"
QRESPONSE = b"RESP"
FILE_GET = b"FGET"
PEER_QUIT = b"QUIT"

REPLY = b"REPL"
ERROR = b"ERRO"


class FileSharingPeer(Peer):
    def __init__(self, maxpeers, serverport, myid=None, serverhost=None):
        super().__init__(maxpeers, serverport, myid, serverhost)
        self.handlers = {
            INSERT_PEER: self.handle_insert_peer,
            LIST_PEERS: self.handle_list_peers,
            PEER_NAME: self.handle_peer_name,
        }

    def handle_peer_name(self, peerconn, data):
        """ Handles the NAME message type. Message data is not used. """
        peerconn.senddata(REPLY, self.myid)

    def handle_insert_peer(self, peerconn, data):
        """Handles the INSERT_PEER (join) message type. The message data
        should be a string of the form, "peerid  host  port", where peer-id
        is the canonical name of the peer that desires to be added to this
        peer's list of peers, host and port are the necessary data to connect
        to the peer.

        """
        self.peerlock.acquire()
        try:
            try:
                # FIXME:dt
                peerid, host, port = [x.decode() for x in data.split()]
                if self.maxpeersreached():
                    self.log('maxpeers %d reached: connection terminating' %
                             self.maxpeers)
                    peerconn.senddata(ERROR, 'Join: too many peers')
                    return
                # peerid = '%s:%s' % (host,port)
                if peerid not in self.getpeerids() and peerid != self.myid:
                    self.addpeer(peerid, host, port)
                    self.log('added peer: %s' % peerid)
                    peerconn.senddata(REPLY, 'Join: peer added: %s' % peerid)
                else:
                    peerconn.senddata(
                        ERROR, 'Join: peer already inserted %s' % peerid)
            except:
                self.log('invalid insert %s: %s' % (str(peerconn), data))
                peerconn.senddata(ERROR, 'Join: incorrect arguments')
        finally:
            self.peerlock.release()

    def handle_list_peers(self, peerconn, data):
        """ Handles the LIST_PEERS message type. Message data is not used. """
        self.peerlock.acquire()
        try:
            self.log('Listing peers %d' % self.numberofpeers())
            peerconn.senddata(REPLY, '%d' % self.numberofpeers())
            for pid in self.getpeerids():
                host, port = self.getpeer(pid)
                peerconn.senddata(REPLY, '%s %s %d' % (pid, host, port))
        finally:
            self.peerlock.release()

    def build_peers(self, host, port, hops=1):
        """
        Attempt to build the local peer list up to the limit stored by
        self.maxpeers, using a simple depth-first search given an
        initial host and port as starting point. The depth of the
        search is limited by the hops parameter.
        """
        # precondition: may be a good idea to hold the lock before going
        #               into this function
        if self.maxpeersreached() or not hops:
            return

        peerid = None

        self.log("Building peers from (%s,%s)" % (host, port))

        try:
            _, peerid = self.connectandsend(host, port, PEER_NAME, b'')[0]

            peerid = peerid.decode()

            self.log("contacted " + peerid)

            resp = self.connectandsend(
                host, port, INSERT_PEER,
                '%s %s %d' % (self.myid, self.serverhost, self.serverport))[0]
            self.log(str(resp))
            if (resp[0] != REPLY) or (peerid in self.getpeerids()):
                return

            self.addpeer(peerid, host, port)

            # do recursive depth first search to add more peers
            resp = self.connectandsend(host, port, LIST_PEERS, b'', pid=peerid)
            if len(resp) > 1:
                resp.reverse()
            resp.pop()  # get rid of header count reply
            while len(resp):
                nextpid, host, port = [
                    x.decode() for x in resp.pop()[1].split()
                ]
                if nextpid != self.myid:
                    self.build_peers(host, port, hops - 1)
        except:
            if self.debug:
                traceback.print_exc()
            self.removepeer(peerid)
