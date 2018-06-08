import socket
import struct
import threading
import time
import traceback


class RemotePeer:
    def __init__(self, host: str, port: int):
        port = int(port)
        assert type(host) == str
        assert type(port) == int
        self.host = host
        self.port = port

    @property
    def id(self):
        return f'{self.host}:{self.port}'

    @property
    def tuple(self):
        return (self.host, self.port)

    @classmethod
    def from_tuple(cls, tup):
        return cls(tup[0], tup[1])

    def __eq__(self, other):
        return self.host == other.host and self.port == other.port


class Peer:
    """ Implements the core functionality that might be used by a peer in a
    P2P network.

    """

    def __init__(self, maxpeers, port, host=None):
        """ Initializes a peer servent (sic.) with the ability to catalog
        information for up to maxpeers number of peers (maxpeers may
        be set to 0 to allow unlimited number of peers), listening on
        a given server port , with a given canonical peer name (id)
        and host address. If not supplied, the host address
        (host) will be determined by attempting to connect to an
        Internet host like Google.

        """
        self.debug = False

        self.maxpeers = int(maxpeers)
        self.port = int(port)
        if host:
            self.host = host
        else:
            self.__inithost()

        self.peerlock = threading.Lock()  # ensure proper access to
        # peers list (maybe better to use threading.RLock (reentrant))
        self.remote_peers = []
        self.shutdown = False  # used to stop the main loop

        self.handlers = {}
        self.router = None

    @property
    def id(self):
        return f'{self.host}:{self.port}'

    def __inithost(self):
        """ Attempt to connect to an Internet host in order to determine the
        local machine's IP address.

        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("www.google.com", 80))
        self.host = s.getsockname()[0]
        s.close()

    def log(self, msg):
        """ Prints a messsage to the screen with the name of the current
        thread """
        if self.debug:
            print("[%s] %s" % (str(threading.currentThread().getName()), msg))

    def __handlepeer(self, clientsock):
        """
        handlepeer( new socket connection ) -> ()

        Dispatches messages from the socket connection
        """

        self.log('New child ' + str(threading.currentThread().getName()))
        self.log('Connected ' + str(clientsock.getpeername()))

        host, port = clientsock.getpeername()
        remote_peer = RemotePeer(host, port)
        peerconn = PeerConnection(remote_peer, clientsock, self.debug)

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

    def startstabilizer(self, stabilizer, delay):
        """ Registers and starts a stabilizer function with this peer.
        The function will be activated every <delay> seconds """
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

    def peer_is_self(self, rp):
        return rp.host == self.host and rp.port == self.port

    def peer_is_connected(self, rp):
        return rp in self.remote_peers

    def can_add_peer(self, host, port):
        remote_peer = RemotePeer(host, port)
        return not self.peer_is_self(
            remote_peer) and not self.peer_is_connected(
                remote_peer) and not self.maxpeersreached()

    def addpeer(self, host, port):
        """ Adds a peer name and host:port mapping to the known list of peers.

        """
        if self.can_add_peer(host, port):
            self.remote_peers.append(RemotePeer(host, int(port)))
            return True
        else:
            return False

    def removepeer(self, remote_peer):
        """ Removes peer information from the known list of peers. """
        if self.peer_is_connected(remote_peer):
            self.remote_peers.remove(remote_peer)

    def numberofpeers(self):
        """ Return the number of known peer's. """
        return len(self.remote_peers)

    def maxpeersreached(self):
        """ Returns whether the maximum limit of names has been added to the
        list of known peers. Always returns True if maxpeers is set to
        0.

        """
        assert self.maxpeers == 0 or len(self.remote_peers) <= self.maxpeers
        return self.maxpeers > 0 and len(self.remote_peers) == self.maxpeers

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
            remote_peer = RemotePeer(host, port)
            peerconn = PeerConnection(remote_peer, debug=self.debug)
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
        for remote_peer in self.remote_peers:
            isconnected = False
            try:
                self.log('Check live %s' % remote_peer.id)
                peerconn = PeerConnection(remote_peer, debug=self.debug)
                peerconn.senddata('PING', '')
                isconnected = True
            except:
                todelete.append()  # FIXME: wtf. this will blow up.
            if isconnected:
                peerconn.close()

        self.peerlock.acquire()
        try:
            for peer in todelete:
                self.removepeer(peer)
        finally:
            self.peerlock.release()

    def mainloop(self):
        s = self.makeserversocket(self.port)
        s.settimeout(2)
        self.log(
            'Server started: %s (%s:%d)' % (self.id, self.host, self.port))

        while not self.shutdown:
            try:
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
                    # traceback.print_exc()
                    continue

        self.log('Main loop exiting')

        s.close()


class PeerConnection:
    def __init__(self, remote_peer, sock=None, debug=False):
        self.debug = debug
        self.remote_peer = remote_peer

        if not sock:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.s.connect(self.remote_peer.tuple)
        else:
            self.s = sock

        self.sd = self.s.makefile('rwb', 0)

    @property
    def id(self):
        return f'{self.remote_peer.host}:{self.remote_peer.port}'

    def __makemsg(self, msgtype, msgdata):
        # application formatting uses strings, networking uses bytestrings
        if msgdata.encode:
            msgdata = msgdata.encode()
        if msgtype.encode:
            msgtype = msgtype.encode()

        msglen = len(msgdata)
        msg = struct.pack("!4sL%ds" % msglen, msgtype, msglen, msgdata)
        return msg

    def senddata(self, msgtype, msgdata):
        """
        senddata( message type, message data ) -> boolean status

        Send a message through a peer connection. Returns True on success
        or False if there was an error.
        """
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

        # application logic uses strings
        return (msgtype.decode(), msg.decode())

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


PEER_NAME = "NAME"  # request a peer's canonical id
LIST_PEERS = "LIST"
INSERT_PEER = "JOIN"
QUERY = "QUER"
QRESPONSE = "RESP"
FILE_GET = "FGET"
PEER_QUIT = "QUIT"

REPLY = "REPL"
ERROR = "ERRO"


class FileSharingPeer(Peer):
    def __init__(self, maxpeers, port, host=None):
        super().__init__(maxpeers, port, host)
        self.files = {}
        self.handlers = {
            INSERT_PEER: self.handle_insert_peer,
            LIST_PEERS: self.handle_list_peers,
            PEER_NAME: self.handle_peer_name,
            FILE_GET: self.handle_file_get,
        }

    def handle_peer_name(self, peerconn, data):
        """ Handles the NAME message type. Message data is not used. """
        peerconn.senddata(REPLY, self.id)

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
                peerid, host, port = data.split()
                if self.maxpeersreached():
                    self.log('maxpeers %d reached: connection terminating' %
                             self.maxpeers)
                    peerconn.senddata(ERROR, 'Join: too many peers')
                    return
                if self.can_add_peer(host, port):
                    self.addpeer(host, port)
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
            for rp in self.remote_peers:
                peerconn.senddata(REPLY,
                                  '%s %s %d' % (rp.id, rp.host, rp.port))
        finally:
            self.peerlock.release()

    # QUERY arguments: "return-peerid key ttl"
    def handle_query(self, peerconn, data):
        """ Handles the QUERY message type. The message data should be in the
        format of a string, "return-peer-id  key  ttl", where return-peer-id
        is the name of the peer that initiated the query, key is the (portion
        of the) file name being searched for, and ttl is how many further
        levels of peers this query should be propagated on.

        """
        # self.peerlock.acquire()
        try:
            peerid, key, ttl = data.split()
            peerconn.senddata(REPLY, 'Query ACK: %s' % key)
        except:
            self.log('invalid query %s: %s' % (str(peerconn), data))
            peerconn.senddata(ERROR, 'Query: incorrect arguments')
        # self.peerlock.release()

        t = threading.Thread(
            target=self.process_query, args=[peerid, key,
                                             int(ttl)])
        t.start()

    def process_query(self, peerid, key, ttl):
        """ Handles the processing of a query message after it has been
        received and acknowledged, by either replying with a QRESPONSE message
        if the file is found in the local list of files, or propagating the
        message onto all immediate neighbors.

        """
        for fname in self.files.keys():
            if key in fname:
                fpeerid = self.files[fname]
                if not fpeerid:  # local files mapped to None
                    fpeerid = self.myid
                host, port = peerid.split(':')
                # can't use sendtopeer here because peerid is not necessarily
                # an immediate neighbor
                self.connectandsend(
                    host,
                    int(port),
                    QRESPONSE,
                    '%s %s' % (fname, fpeerid),
                    pid=peerid)
                return
        # will only reach here if key not found... in which case
        # propagate query to neighbors
        if ttl > 0:
            msgdata = '%s %s %d' % (peerid, key, ttl - 1)
            for nextpid in self.getpeerids():
                self.sendtopeer(nextpid, QUERY, msgdata)

    def handle_qresponse(self, peerconn, data):
        """ Handles the QRESPONSE message type. The message data should be
        in the format of a string, "file-name  peer-id", where file-name is
        the file that was queried about and peer-id is the name of the peer
        that has a copy of the file.

        """
        try:
            fname, fpeerid = data.split()
            if fname in self.files:
                self.log('Can\'t add duplicate file %s %s' % (fname, fpeerid))
            else:
                self.files[fname] = fpeerid
        except:
            if self.debug:
                traceback.print_exc()

    def handle_file_get(self, peerconn, data):
        """ Handles the FILEGET message type. The message data should be in
        the format of a string, "file-name", where file-name is the name
        of the file to be fetched.

        """
        fname = data
        if fname not in self.files:
            self.log('File not found %s' % fname)
            peerconn.senddata(ERROR, 'File not found')
            return
        try:
            fd = open(fname, 'r')
            filedata = ''
            while True:
                data = fd.read(2048)
                if not len(data):
                    break
                filedata += data
            fd.close()
        except:
            self.log('Error reading file %s' % fname)
            peerconn.senddata(ERROR, 'Error reading file')
            return

        peerconn.senddata(REPLY, filedata)

    def handle_quit(self, peerconn, data):
        """ Handles the QUIT message type. The message data should be in the
        format of a string, "peer-id", where peer-id is the canonical
        name of the peer that wishes to be unregistered from this
        peer's directory.

        """
        self.peerlock.acquire()
        try:
            peerid = data.lstrip().rstrip()
            if peerid in self.getpeerids():
                msg = 'Quit: peer removed: %s' % peerid
                self.log(msg)
                peerconn.senddata(REPLY, msg)
                self.removepeer(peerid)
            else:
                msg = 'Quit: peer not found: %s' % peerid
                self.log(msg)
                peerconn.senddata(ERROR, msg)
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
        rp = RemotePeer(host, port)
        if self.maxpeersreached() or not hops:
            return

        peerid = None

        self.log("Building peers from (%s:%s)" % (host, port))

        try:
            _, peerid = self.connectandsend(host, port, PEER_NAME, '')[0]

            self.log("contacted " + peerid)

            resp = self.connectandsend(
                host, port, INSERT_PEER,
                '%s %s %d' % (self.id, self.host, self.port))[0]
            self.log(str(resp))
            if (resp[0] != REPLY) or self.peer_is_connected(rp):
                return

            self.addpeer(host, port)

            # do recursive depth first search to add more peers
            resp = self.connectandsend(host, port, LIST_PEERS, '')
            if len(resp) > 1:
                resp.reverse()
            resp.pop()  # get rid of header count reply
            while len(resp):
                nextpid, host, port = resp.pop()[1].split()
                if nextpid != self.id:
                    self.build_peers(host, port, hops - 1)
        except:
            if self.debug:
                traceback.print_exc()
            self.removepeer(rp)

    def add_local_file(self, filename):
        """ Registers a locally-stored file with the peer. """
        self.files[filename] = None
        self.log("Added local file %s" % filename)
