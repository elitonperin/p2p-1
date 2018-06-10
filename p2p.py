import socket
import struct
import threading
import contextlib


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
        return f'<Remote Peer {self.host}:{self.port}>'

    def __eq__(self, other):
        return self.host == other.host and self.port == other.port


class Peer:
    def __init__(self, maxpeers, port, host=None, debug=False):
        self.debug = debug

        self.maxpeers = int(maxpeers)
        self.port = int(port)
        self.host = host

        # Only one thread can alter `remove_peers` at a time
        self.peerlock = threading.Lock()
        self.remote_peers = []

        self.handlers = {}

        self.shutdown = False  # used to stop the main loop

    @property
    def id(self):
        return f'{self.host}:{self.port}'

    def log(self, msg):
        if self.debug:
            thread_name = threading.currentThread().getName()
            print(f"[{thread_name}] {msg}")

    def handle_peer(self, clientsock):
        self.log('New child ' + str(threading.currentThread().getName()))
        self.log('Connected ' + str(clientsock.getpeername()))

        host, port = clientsock.getpeername()
        remote_peer = RemotePeer(host, port)
        peerconn = PeerConnection(remote_peer, clientsock, self.debug)

        msgtype, msgdata = peerconn.receive_data()
        if msgtype:
            msgtype = msgtype.upper()
        if msgtype in self.handlers:
            self.log(f'Handling peer msg: {msgtype}: {msgdata}')
            self.handlers[msgtype](peerconn, msgdata)
        else:
            self.log(f'Not handled: {msgtype}: {msgdata}')

        self.log('Disconnecting ' + str(clientsock.getpeername()))
        peerconn.close()

    def peer_is_self(self, rp):
        return rp.host == self.host and rp.port == self.port

    def peer_is_connected(self, rp):
        return rp in self.remote_peers

    def can_add_peer(self, rp):
        return not self.peer_is_self(rp) and not self.peer_is_connected(
            rp) and not self.max_peers_reached()

    def add_peer(self, rp):
        # FIXME returning true/false is odd
        # FIXME Intuitively, method named `add_x` should take input of type `x`
        # not (host, port) ...
        if self.can_add_peer(rp):
            self.remote_peers.append(rp)
            return True
        else:
            return False

    def remove_peer(self, remote_peer):
        if self.peer_is_connected(remote_peer):
            self.remote_peers.remove(remote_peer)

    @property
    def num_peers(self):
        return len(self.remote_peers)

    def max_peers_reached(self):
        assert self.maxpeers == 0 or len(self.remote_peers) <= self.maxpeers
        # returns False if maxpeers is set to 0 (uncapped)
        return self.maxpeers > 0 and len(self.remote_peers) == self.maxpeers

    def make_server_socket(self, port, backlog=5, timeout=2):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', port))  # FIXME: why don't we pass a host in???
        s.listen(backlog)
        return s

    def connect_and_send(self,
                         host,
                         port,
                         msgtype,
                         msgdata,
                         pid=None,
                         waitreply=True):
        # FIXME remove pid
        # Or do wee need it for messages that need to be returned so someone
        # other than sender?
        msgreply = []

        remote_peer = RemotePeer(host, port)
        peerconn = PeerConnection(remote_peer, debug=self.debug)
        peerconn.send_data(msgtype, msgdata)
        self.log(f'Sent {pid}: {msgtype}')

        if waitreply:
            onereply = peerconn.receive_data()
            while onereply != (None, None):
                msgreply.append(onereply)
                self.log(f'Got reply {pid}: {str(msgreply)}')
                onereply = peerconn.receive_data()
        peerconn.close()

        return msgreply

    def prune_peers(self):
        dead_peers = []
        for remote_peer in self.remote_peers:
            isconnected = False
            try:
                self.log(f'Check live {remote_peer.id}')
                peerconn = PeerConnection(remote_peer, debug=self.debug)
                peerconn.send_data('PING', '')
                isconnected = True
            except:
                dead_peers.append(remote_peer)
            if isconnected:
                peerconn.close()

        self.peerlock.acquire()
        try:
            for peer in dead_peers:
                self.remove_peer(peer)
        finally:
            self.peerlock.release()

    def main_loop(self):
        s = self.make_server_socket(self.port, timeout=2)
        self.log(f'Server started: {self.id}')

        while not self.shutdown:
            with contextlib.suppress(socket.timeout):
                clientsock, clientaddr = s.accept()
                clientsock.settimeout(None)

                t = threading.Thread(
                    target=self.handle_peer, args=[clientsock])
                t.start()

        s.close()

    def exit(self):
        self.shutdown = True
        # FIXME perhaps this cleanup should be handled by the mainloop?
        # Or rename to something like "teardown"
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
        # FIXME how to do this with an f-string
        msg = struct.pack("!4sL%ds" % msglen, msgtype, msglen, msgdata)
        return msg

    def send_data(self, msgtype, msgdata):
        msg = self.make_msg(msgtype, msgdata)
        self.sd.write(msg)
        self.sd.flush()

    def receive_data(self):
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

        # application logic uses strings
        return (msgtype.decode(), msg.decode())

    def close(self):
        self.s.close()
        self.s = None
        self.sd = None

    def __str__(self):
        return f"<PeerConnection {self.id}>"


PEER_NAME = "NAME"
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

    def handle_peer_name(self, peerconn, data):
        peerconn.send_data(REPLY, self.id)

    def handle_insert_peer(self, peerconn, data):
        self.peerlock.acquire()
        try:
            try:
                rp = RemotePeer.from_id(data)
                if self.max_peers_reached():
                    self.log(
                        f'maxpeers {self.maxpeers} reached: connection terminating'
                    )
                    peerconn.send_data(ERROR, 'Join: too many peers')
                    return
                if self.add_peer(rp):
                    self.log(f'added peer: {rp.id}')
                    peerconn.send_data(REPLY, f'Join: peer added: {rp.id}')
                else:
                    peerconn.send_data(ERROR,
                                       f'Join: peer already inserted {rp.id}')
            except:
                self.log(f'invalid insert {str(peerconn)}: {data}')
                peerconn.send_data(ERROR, 'Join: incorrect arguments')
        finally:
            self.peerlock.release()

    def handle_list_peers(self, peerconn, data):
        self.peerlock.acquire()
        try:
            self.log(f'Listing peers {self.num_peers}')
            peerconn.send_data(REPLY, str(self.num_peers))  # FIXME str
            for rp in self.remote_peers:
                peerconn.send_data(REPLY, self.id)
        finally:
            self.peerlock.release()

    def handle_query(self, peerconn, data):
        # self.peerlock.acquire()
        try:
            peerid, key, ttl = data.split()
            peerconn.send_data(REPLY, f'Query ACK: {key}')
        except:
            self.log(f'invalid query {str(peerconn)}: {data}')
            peerconn.send_data(ERROR, 'Query: incorrect arguments')
            # FIXME returning b/c can't open thread without args defined ...
            return
        # self.peerlock.release()

        t = threading.Thread(
            target=self.process_query, args=[peerid, key,
                                             int(ttl)])
        t.start()

    def process_query(self, peerid, key, ttl):
        host, port = peerid.split(':')
        for file_name in self.files.keys():
            if key in file_name:
                file_peer_id = self.files[file_name]
                if not file_peer_id:  # local files mapped to None
                    file_peer_id = self.id
                self.connect_and_send(
                    host,
                    int(port),
                    QRESPONSE,
                    f'{file_name} {file_peer_id}',
                    pid=peerid)
                return
        # if no match and ttl > 0, propagate query to neighbors
        if ttl > 0:
            msgdata = f'{peerid} {key} {ttl - 1}'
            for nextpid in self.remote_peers:
                self.connect_and_send(host, port, QUERY, msgdata)

    def handle_qresponse(self, peerconn, data):
        # peerlock? we alter Peer.files here ...
        file_name, file_peer_id = data.split()
        if file_name in self.files:
            self.log(f'Can\'t add duplicate file {file_name} {file_peer_id}')
        else:
            self.files[file_name] = file_peer_id

    def handle_file_get(self, peerconn, data):
        file_name = data
        if file_name not in self.files:
            self.log(f'File not found {file_name}')
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
            self.log(f'Error reading file {file_name}')
            peerconn.send_data(ERROR, 'Error reading file')
            return

        peerconn.send_data(REPLY, filedata)

    def handle_quit(self, peerconn, data):
        self.peerlock.acquire()
        try:
            peerid = data.lstrip().rstrip()
            rp = RemotePeer.from_id(peerid)
            if rp in self.remote_peers:
                msg = f'Quit: peer removed: {peerid}'
                self.log(msg)
                peerconn.send_data(REPLY, msg)
                self.remove_peer(rp)
            else:
                msg = f'Quit: peer not found: {peerid}'
                self.log(msg)
                peerconn.send_data(ERROR, msg)
        finally:
            self.peerlock.release()

    def build_peers(self, host, port, hops=1):
        # precondition: may be a good idea to hold the lock before going
        #               into this function
        rp = RemotePeer(host, port)
        if self.max_peers_reached() or not hops:
            return

        self.log(f"Building peers from {rp.id}")

        try:
            self.send_join_message(host, port)

            # do recursive depth first search to add more peers
            resp = self.connect_and_send(host, port, LIST_PEERS, '')
            if len(resp) > 1:
                resp.reverse()
            resp.pop()  # get rid of header count reply
            while len(resp):
                nextpid = resp.pop()[1]
                if nextpid != self.id:
                    host, port = nextpid.split(
                        ':')  # FIXME helper function ...
                    port = int(port)
                    self.build_peers(host, port, hops - 1)
        except:
            self.remove_peer(rp)
            raise

    def add_local_file(self, filename):
        self.files[filename] = None
        self.log(f"Added local file {filename}")

    def remove_local_file(self, filename):
        self.files.pop(filename)
        self.log(f"Removed local file {filename}")

    def send_join_message(self, host, port):
        resp = self.connect_and_send(host, port, INSERT_PEER, self.id)[0]
        self.log(str(resp))  # FIXME

        # FIXME: rp is a little awkward here ...
        rp = RemotePeer(host, port)

        if (resp[0] != REPLY) or self.peer_is_connected(rp):
            # FIXME this "error state" isn't really an "error state"
            if 'already inserted' not in resp[1]:
                # FIXME this return statements was used to prematurely stop
                # execution of build_peers, but it no longer does this. Raising
                # an exception would be more appropriate
                return

        self.add_peer(rp)  # FIXME leaving here for now ...

    def send_quit_message(self, host, port):
        resp = self.connect_and_send(host, int(port), PEER_QUIT, self.id)

        # FIXME: why is this double nested??
        if resp[0][0] == REPLY:
            self.remove_peer(RemotePeer(host, port))

    def query_peers(self, key, ttl='10'):
        # FIXME: a string ttl is weird
        for peer in self.remote_peers:
            data = ' '.join([self.id, key, ttl])
            self.connect_and_send(peer.host, peer.port, QUERY, data)
