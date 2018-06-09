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

    @classmethod
    def from_id(cls, id_):
        host, port = id_.split(':')
        return cls(host, int(port))

    def __repr__(self):
        return f'{self.host}:{self.port}'

    def __eq__(self, other):
        return self.host == other.host and self.port == other.port


class Peer:
    """ Implements the core functionality that might be used by a peer in a
    P2P network.

    """

    def __init__(self, maxpeers, port, host=None, debug=False):
        """ Initializes a peer servent (sic.) with the ability to catalog
        information for up to maxpeers number of peers (maxpeers may
        be set to 0 to allow unlimited number of peers), listening on
        a given server port , with a given canonical peer name (id)
        and host address. If not supplied, the host address
        (host) will be determined by attempting to connect to an
        Internet host like Google.

        """
        self.debug = debug

        self.maxpeers = int(maxpeers)
        self.port = int(port)
        if host:
            self.host = host
        else:
            self.init_host()

        self.peerlock = threading.Lock()  # ensure proper access to
        # peers list (maybe better to use threading.RLock (reentrant))
        self.remote_peers = []
        self.shutdown = False  # used to stop the main loop

        self.handlers = {}
        self.router = None

    @property
    def id(self):
        return f'{self.host}:{self.port}'

    def init_host(self):
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

    def handle_peer(self, clientsock):
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
            msgtype, msgdata = peerconn.receive_data()
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

    def run_stabilizer(self, stabilizer, delay):
        while not self.shutdown:
            stabilizer()
            time.sleep(delay)

    def start_stabilizer(self, stabilizer, delay):
        """ Registers and starts a stabilizer function with this peer.
        The function will be activated every <delay> seconds """
        t = threading.Thread(
            target=self.run_stabilizer, args=[stabilizer, delay])
        t.start()

    def add_handler(self, msgtype, handler):
        """ Registers the handler for the given message type with this peer """
        assert len(msgtype) == 4
        self.handlers[msgtype] = handler

    def add_router(self, router):
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
                remote_peer) and not self.max_peers_reached()

    def add_peer(self, host, port):
        """ Adds a peer name and host:port mapping to the known list of peers.

        """
        if self.can_add_peer(host, port):
            self.remote_peers.append(RemotePeer(host, int(port)))
            return True
        else:
            return False

    def remove_peer(self, remote_peer):
        """ Removes peer information from the known list of peers. """
        if self.peer_is_connected(remote_peer):
            self.remote_peers.remove(remote_peer)

    def num_peers(self):
        """ Return the number of known peer's. """
        return len(self.remote_peers)

    def max_peers_reached(self):
        """ Returns whether the maximum limit of names has been added to the
        list of known peers. Always returns True if maxpeers is set to
        0.

        """
        assert self.maxpeers == 0 or len(self.remote_peers) <= self.maxpeers
        return self.maxpeers > 0 and len(self.remote_peers) == self.maxpeers

    def make_server_socket(self, port, backlog=5, timeout=2):
        """ Constructs and prepares a server socket listening on the given
        port.

        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', port))
        s.listen(backlog)
        return s

    def send_to_peer(self, peerid, msgtype, msgdata, waitreply=True):
        """
        send_to_peer( peer id, message type, message data, wait for a reply )
         -> [ ( reply type, reply data ), ... ]

        Send a message to the identified peer. In order to decide how to
        send the message, the router handler for this peer will be called.
        If no router function has been registered, it will not work. The
        router function should provide the next immediate peer to whom the
        message should be forwarded. The peer's reply, if it is expected,
        will be returned.

        Returns None if the message could not be routed.
        """

        # FIXME: this method isn't used and doesn't seem to have any advantages
        # over connect_and_send ...
        # Perhaps replies are to be sent to someone else???

        if self.router:
            nextpid, host, port = self.router(peerid)
        if not self.router or not nextpid:
            self.log('Unable to route %s to %s' % (msgtype, peerid))
            return None
        return self.connect_and_send(
            host, port, msgtype, msgdata, pid=nextpid, waitreply=waitreply)

    def connect_and_send(self,
                         host,
                         port,
                         msgtype,
                         msgdata,
                         pid=None,
                         waitreply=True):
        """
        connect_and_send( host, port, message type, message data, peer id,
        wait for a reply ) -> [ ( reply type, reply data ), ... ]

        Connects and sends a message to the specified host:port. The host's
        reply, if expected, will be returned as a list of tuples.

        """
        # FIXME remove pid
        msgreply = []
        try:
            remote_peer = RemotePeer(host, port)
            peerconn = PeerConnection(remote_peer, debug=self.debug)
            peerconn.send_data(msgtype, msgdata)
            self.log('Sent %s: %s' % (pid, msgtype))

            if waitreply:
                onereply = peerconn.receive_data()
                while onereply != (None, None):
                    msgreply.append(onereply)
                    self.log('Got reply %s: %s' % (pid, str(msgreply)))
                    onereply = peerconn.receive_data()
            peerconn.close()
        except KeyboardInterrupt:
            raise
        except:
            if self.debug:
                traceback.print_exc()

        return msgreply

    def prune_peers(self):
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
                peerconn.send_data('PING', '')
                isconnected = True
            except:
                todelete.append(remote_peer)
            if isconnected:
                peerconn.close()

        self.peerlock.acquire()
        try:
            for peer in todelete:
                self.remove_peer(peer)
        finally:
            self.peerlock.release()

    def main_loop(self):
        s = self.make_server_socket(self.port, timeout=2)
        self.log(
            'Server started: %s (%s:%d)' % (self.id, self.host, self.port))

        while not self.shutdown:
            try:
                clientsock, clientaddr = s.accept()
                clientsock.settimeout(None)

                t = threading.Thread(
                    target=self.handle_peer, args=[clientsock])
                t.start()
            except KeyboardInterrupt:
                print('KeyboardInterrupt: stopping main_loop')
                self.exit()
                continue
            except:
                if self.debug:
                    # traceback.print_exc()
                    continue

        self.log('Main loop exiting')

        s.close()

    def exit(self):
        self.shutdown = True
        # FIXME perhaps this cleanup should be handled by the mainloop?
        for rp in self.remote_peers:
            self.send_quit_message(rp.host, rp.port)


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

    def make_msg(self, msgtype, msgdata):
        # application formatting uses strings, networking uses bytestrings
        if msgdata.encode:
            msgdata = msgdata.encode()
        if msgtype.encode:
            msgtype = msgtype.encode()

        msglen = len(msgdata)
        msg = struct.pack("!4sL%ds" % msglen, msgtype, msglen, msgdata)
        return msg

    def send_data(self, msgtype, msgdata):
        """
        send_data( message type, message data ) -> boolean status

        Send a message through a peer connection. Returns True on success
        or False if there was an error.
        """
        try:
            msg = self.make_msg(msgtype, msgdata)
            self.sd.write(msg)
            self.sd.flush()
        except KeyboardInterrupt:
            raise
        except:
            if self.debug:
                traceback.print_exc()
            return False
        return True

    def receive_data(self):
        """
        receive_data() -> (msgtype, msgdata)

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
    def __init__(self, maxpeers, port, host=None, debug=False):
        super().__init__(maxpeers, port, host, debug)
        self.files = {}
        self.handlers = {
            INSERT_PEER: self.handle_insert_peer,
            LIST_PEERS: self.handle_list_peers,
            PEER_NAME: self.handle_peer_name,
            FILE_GET: self.handle_file_get,
            QUERY: self.handle_query,
            QRESPONSE: self.handle_qresponse,
            PEER_QUIT: self.handle_quit,
        }
        self.add_router(self.router_func)

    def router_func(self, peer_id):
        # FIXME this doesn't seem to work ...
        host, port = peer_id.split()
        if (host, int(port)) not in self.remote_peers:
            return (None, None)
        else:
            return [peer_id, host, port]

    def handle_peer_name(self, peerconn, data):
        """ Handles the NAME message type. Message data is not used. """
        peerconn.send_data(REPLY, self.id)

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
                if self.max_peers_reached():
                    self.log('maxpeers %d reached: connection terminating' %
                             self.maxpeers)
                    peerconn.send_data(ERROR, 'Join: too many peers')
                    return
                if self.can_add_peer(host, port):
                    self.add_peer(host, port)
                    self.log('added peer: %s' % peerid)
                    peerconn.send_data(REPLY, 'Join: peer added: %s' % peerid)
                else:
                    peerconn.send_data(
                        ERROR, 'Join: peer already inserted %s' % peerid)
            except:
                self.log('invalid insert %s: %s' % (str(peerconn), data))
                peerconn.send_data(ERROR, 'Join: incorrect arguments')
        finally:
            self.peerlock.release()

    def handle_list_peers(self, peerconn, data):
        """ Handles the LIST_PEERS message type. Message data is not used. """
        self.peerlock.acquire()
        try:
            self.log('Listing peers %d' % self.num_peers())
            peerconn.send_data(REPLY, '%d' % self.num_peers())
            for rp in self.remote_peers:
                peerconn.send_data(REPLY,
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
            peerconn.send_data(REPLY, 'Query ACK: %s' % key)
        except:
            self.log('invalid query %s: %s' % (str(peerconn), data))
            peerconn.send_data(ERROR, 'Query: incorrect arguments')
            # FIXME returning b/c can't open thread without args defined ...
            return
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
        host, port = peerid.split(':')
        for file_name in self.files.keys():
            if key in file_name:
                file_peer_id = self.files[file_name]
                if not file_peer_id:  # local files mapped to None
                    file_peer_id = self.id
                # can't use send_to_peer here because peerid is not necessarily
                # an immediate neighbor
                self.connect_and_send(
                    host,
                    int(port),
                    QRESPONSE,
                    '%s %s' % (file_name, file_peer_id),
                    pid=peerid)
                return
        # will only reach here if key not found... in which case
        # propagate query to neighbors
        if ttl > 0:
            msgdata = '%s %s %d' % (peerid, key, ttl - 1)
            for nextpid in self.remote_peers:
                # FIXME this was send_to_peer which doesn't work and doesn't
                # seem to do anything useful

                self.connect_and_send(host, port, QUERY, msgdata)

    def handle_qresponse(self, peerconn, data):
        """ Handles the QRESPONSE message type. The message data should be
        in the format of a string, "file-name  peer-id", where file-name is
        the file that was queried about and peer-id is the name of the peer
        that has a copy of the file. """
        try:
            file_name, file_peer_id = data.split()
            if file_name in self.files:
                self.log('Can\'t add duplicate file %s %s' % (file_name,
                                                              file_peer_id))
            else:
                self.files[file_name] = file_peer_id
        except:
            if self.debug:
                traceback.print_exc()

    def handle_file_get(self, peerconn, data):
        """ Handles the FILEGET message type. The message data should be in
        the format of a string, "file-name", where file-name is the name
        of the file to be fetched. """
        file_name = data
        if file_name not in self.files:
            self.log('File not found %s' % file_name)
            peerconn.send_data(ERROR, 'File not found')
            return
        try:
            fd = open(file_name, 'r')
            filedata = ''
            while True:
                data = fd.read(2048)
                if not len(data):
                    break
                filedata += data
            fd.close()
        except:
            self.log('Error reading file %s' % file_name)
            peerconn.send_data(ERROR, 'Error reading file')
            return

        peerconn.send_data(REPLY, filedata)

    def handle_quit(self, peerconn, data):
        """ Handles the QUIT message type. The message data should be in the
        format of a string, "peer-id", where peer-id is the canonical
        name of the peer that wishes to be unregistered from this
        peer's directory.

        """
        self.peerlock.acquire()
        try:
            peerid = data.lstrip().rstrip()
            rp = RemotePeer.from_id(peerid)
            if rp in self.remote_peers:
                msg = 'Quit: peer removed: %s' % peerid
                self.log(msg)
                peerconn.send_data(REPLY, msg)
                self.remove_peer(rp)
            else:
                msg = 'Quit: peer not found: %s' % peerid
                self.log(msg)
                peerconn.send_data(ERROR, msg)
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
        if self.max_peers_reached() or not hops:
            return

        self.log("Building peers from (%s:%s)" % (host, port))

        try:
            self.send_join_message(host, port)

            # do recursive depth first search to add more peers
            resp = self.connect_and_send(host, port, LIST_PEERS, '')
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
            self.remove_peer(rp)

    def add_local_file(self, filename):
        """ Registers a locally-stored file with the peer. """
        self.files[filename] = None
        self.log("Added local file %s" % filename)

    def send_join_message(self, host, port):
        resp = self.connect_and_send(
            host, port, INSERT_PEER,
            '%s %s %d' % (self.id, self.host, self.port))[0]
        self.log(str(resp))

        # FIXME: rp is a little awkward here ...
        rp = RemotePeer(host, port)

        if (resp[0] != REPLY) or self.peer_is_connected(rp):
            # FIXME this "error state" isn't really an "error state"
            if 'already inserted' not in resp[1]:
                # FIXME this return statements was used to prematurely stop
                # execution of build_peers, but it no longer does this. Raising
                # an exception would be more appropriate
                return

        self.add_peer(host, port)  # FIXME leaving here for now ...

    def send_quit_message(self, host, port):
        resp = self.connect_and_send(host, int(port), PEER_QUIT, self.id)

        # FIXME: why is this double nested??
        if resp[0][0] == REPLY:
            self.remove_peer(RemotePeer(host, port))
