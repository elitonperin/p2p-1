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
        return f"{self.host}:{self.port}"

    @property
    def tuple(self):
        return (self.host, self.port)

    @classmethod
    def from_tuple(cls, tup):
        return cls(tup[0], tup[1])

    @classmethod
    def from_id(cls, id_):
        host, port = id_.split(":")
        return cls(host, int(port))

    def __repr__(self):
        return f"<Remote Peer {self.host}:{self.port}>"

    def __eq__(self, other):
        return self.host == other.host and self.port == other.port


class Peer:
    def __init__(self, maxpeers, port, host=None, debug=False):
        self.debug = debug

        self.maxpeers = int(maxpeers)
        self.port = int(port)
        self.host = host

        # Only one thread can alter `remove_peers` at a time
        self.peer_lock = threading.Lock()
        self.remote_peers = []

        self.handlers = {}

        self.shutdown = False  # used to stop the main loop

    @property
    def id(self):
        return f"{self.host}:{self.port}"

    def log(self, msg):
        if self.debug:
            thread_name = threading.current_thread().getName()
            print(f"[{thread_name}] {msg}")

    def handle_peer(self, client_socket):
        self.log("New child " + str(threading.current_thread().getName()))
        self.log("Connected " + str(client_socket.getpeername()))

        host, port = client_socket.getpeername()
        remote_peer = RemotePeer(host, port)
        peer_connection = PeerConnection(remote_peer, client_socket, self.debug)

        msg_type, msg_data = peer_connection.receive_data()
        if msg_type:
            msg_type = msg_type.upper()
        if msg_type in self.handlers:
            self.log(f"Handling peer msg: {msg_type}: {msg_data}")
            self.handlers[msg_type](peer_connection, msg_data)
        else:
            self.log(f"Not handled: {msg_type}: {msg_data}")

        self.log("Disconnecting " + str(client_socket.getpeername()))
        peer_connection.close()

    def peer_is_self(self, remote_peer):
        return remote_peer.host == self.host and remote_peer.port == self.port

    def peer_is_connected(self, remote_peer):
        return remote_peer in self.remote_peers

    def can_add_peer(self, remote_peer):
        return (
            not self.peer_is_self(remote_peer)
            and not self.peer_is_connected(remote_peer)
            and not self.max_peers_reached()
        )

    def add_peer(self, remote_peer):
        # FIXME returning true/false is odd
        # FIXME Intuitively, method named `add_x` should take input of type `x`
        # not (host, port) ...
        if self.can_add_peer(remote_peer):
            self.remote_peers.append(remote_peer)
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
        s.bind(("", port))  # FIXME: why don't we pass a host in???
        s.listen(backlog)
        return s

    def connect_and_send(self, remote_peer, msg_type, msg_data, wait_for_reply=True):
        # FIXME remove pid
        # Or do wee need it for messages that need to be returned so someone
        # other than sender?
        msg_reply = []

        peer_connection = PeerConnection(remote_peer, debug=self.debug)
        peer_connection.send_message(msg_type, msg_data)
        self.log(f"Sent {remote_peer.id}: {msg_type}")

        if wait_for_reply:
            one_reply = peer_connection.receive_data()
            while one_reply != (None, None):
                msg_reply.append(one_reply)
                self.log(f"Got reply {remote_peer.id}: {str(msg_reply)}")
                one_reply = peer_connection.receive_data()
        peer_connection.close()

        return msg_reply

    def prune_peers(self):
        dead_remote_peers = []
        for remote_peer in self.remote_peers:
            isconnected = False
            try:
                self.log(f"Check live {remote_peer.id}")
                peer_connection = PeerConnection(remote_peer, debug=self.debug)
                peer_connection.send_message("PING", "")
                isconnected = True
            except:
                dead_remote_peers.append(remote_peer)
            if isconnected:
                peer_connection.close()

        with self.peer_lock:
            for remote_peer in dead_remote_peers:
                self.remove_peer(remote_peer)

    def main_loop(self):
        s = self.make_server_socket(self.port, timeout=2)
        self.log(f"Server started: {self.id}")

        while not self.shutdown:
            with contextlib.suppress(socket.timeout):
                client_socket, client_address = s.accept()
                client_socket.settimeout(None)

                t = threading.Thread(target=self.handle_peer, args=[client_socket])
                t.start()

        s.close()

    def exit(self):
        self.shutdown = True
        # FIXME perhaps this cleanup should be handled by the mainloop?
        # Or rename to something like "teardown"
        for remote_peer in self.remote_peers:
            self.send_quit_message(remote_peer)


class PeerConnection:
    def __init__(self, remote_peer, sock=None, debug=False):
        self.debug = debug
        self.remote_peer = remote_peer

        if not sock:
            self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.s.connect(self.remote_peer.tuple)
        else:
            self.s = sock

        self.sd = self.s.makefile("rwb", 0)

    @property
    def id(self):
        return f"{self.remote_peer.host}:{self.remote_peer.port}"

    def make_msg(self, msg_type, msg_data):
        # application formatting uses strings, networking uses bytestrings
        if msg_data.encode:
            msg_data = msg_data.encode()
        if msg_type.encode:
            msg_type = msg_type.encode()

        msg_len = len(msg_data)
        # FIXME how to do this with an f-string
        msg = struct.pack("!4sL%ds" % msg_len, msg_type, msg_len, msg_data)
        return msg

    def send_message(self, msg_type, msg_data):
        msg = self.make_msg(msg_type, msg_data)
        self.sd.write(msg)
        self.sd.flush()

    def receive_data(self):
        msg_type = self.sd.read(4)
        if not msg_type:
            return None, None

        lenstr = self.sd.read(4)
        msg_len = int(struct.unpack("!L", lenstr)[0])
        msg = b""

        while len(msg) != msg_len:
            data = self.sd.read(min(2048, msg_len - len(msg)))
            if not len(data):
                break
            msg += data

        if len(msg) != msg_len:
            return None, None

        # application logic uses strings
        return (msg_type.decode(), msg.decode())

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

    def handle_peer_name(self, peer_connection, data):
        peer_connection.send_message(REPLY, self.id)

    def handle_insert_peer(self, peer_connection, data):
        with self.peer_lock:
            try:
                remote_peer = RemotePeer.from_id(data)
                if self.max_peers_reached():
                    self.log(
                        f"maxpeers {self.maxpeers} reached: connection terminating"
                    )
                    peer_connection.send_message(ERROR, "Join: too many peers")
                    return
                if self.add_peer(remote_peer):
                    self.log(f"added peer: {remote_peer.id}")
                    peer_connection.send_message(
                        REPLY, f"Join: peer added: {remote_peer.id}"
                    )
                else:
                    peer_connection.send_message(
                        ERROR, f"Join: peer already inserted {remote_peer.id}"
                    )
            except:
                self.log(f"invalid insert {str(peer_connection)}: {data}")
                peer_connection.send_message(ERROR, "Join: incorrect arguments")

    def handle_list_peers(self, peer_connection, data):
        with self.peer_lock:
            self.log(f"Listing peers {self.num_peers}")
            peer_connection.send_message(REPLY, str(self.num_peers))  # FIXME str
            for remote_peer in self.remote_peers:
                peer_connection.send_message(REPLY, self.id)

    def handle_query(self, peer_connection, data):
        try:
            peerid, key, ttl = data.split()
            peer_connection.send_message(REPLY, f"Query ACK: {key}")
        except:
            self.log(f"invalid query {str(peer_connection)}: {data}")
            peer_connection.send_message(ERROR, "Query: incorrect arguments")
            # FIXME returning b/c can't open thread without args defined ...
            return

        remote_peer = RemotePeer.from_id(peerid)
        t = threading.Thread(
            target=self.process_query, args=[remote_peer, key, int(ttl)]
        )
        t.start()

    def process_query(self, remote_peer, key, ttl):
        for file_name in self.files.keys():
            if key in file_name:
                file_peer_id = self.files[file_name]
                if not file_peer_id:  # local files mapped to None
                    file_peer_id = self.id
                self.connect_and_send(
                    remote_peer, QRESPONSE, f"{file_name} {file_peer_id}"
                )
                return
        # if no match and ttl > 0, propagate query to neighbors
        if ttl > 0:
            msg_data = f"{remote_peer.id} {key} {ttl - 1}"
            for remote_peer in self.remote_peers:
                self.connect_and_send(remote_peer, QUERY, msg_data)

    def handle_qresponse(self, peer_connection, data):
        with self.peer_lock:
            file_name, file_peer_id = data.split()
            if file_name in self.files:
                self.log(f"Can't add duplicate file {file_name} {file_peer_id}")
            else:
                self.files[file_name] = file_peer_id

    def handle_file_get(self, peer_connection, data):
        file_name = data
        if file_name not in self.files:
            self.log(f"File not found {file_name}")
            peer_connection.send_message(ERROR, "File not found")
            return
        try:
            fd = open(file_name, "r")
            filedata = ""
            while True:
                data = fd.read(2048)
                if not len(data):
                    break
                filedata += data
            fd.close()
        except:
            self.log(f"Error reading file {file_name}")
            peer_connection.send_message(ERROR, "Error reading file")
            return

        peer_connection.send_message(REPLY, filedata)

    def handle_quit(self, peer_connection, data):
        with self.peer_lock:
            peerid = data.lstrip().rstrip()
            remote_peer = RemotePeer.from_id(peerid)
            if remote_peer in self.remote_peers:
                msg = f"Quit: peer removed: {peerid}"
                self.log(msg)
                peer_connection.send_message(REPLY, msg)
                self.remove_peer(remote_peer)
            else:
                msg = f"Quit: peer not found: {peerid}"
                self.log(msg)
                peer_connection.send_message(ERROR, msg)

    def build_peers(self, remote_peer, hops=1):
        # precondition: may be a good idea to hold the lock before going
        #               into this function
        if self.max_peers_reached() or not hops:
            return

        self.log(f"Building peers from {remote_peer.id}")

        try:
            self.send_join_message(remote_peer)

            # do recursive depth first search to add more peers
            resp = self.connect_and_send(remote_peer, LIST_PEERS, "")
            if len(resp) > 1:
                resp.reverse()
            resp.pop()  # get rid of header count reply
            while len(resp):
                next_remote_peer_id = resp.pop()[1]
                if next_remote_peer_id != self.id:
                    next_remote_peer = RemotePeer.from_id(next_remote_peer_id)
                    self.build_peers(next_remote_peer, hops - 1)
        except:
            self.remove_peer(remote_peer)
            raise

    def add_local_file(self, filename):
        self.files[filename] = None
        self.log(f"Added local file {filename}")

    def remove_local_file(self, filename):
        self.files.pop(filename)
        self.log(f"Removed local file {filename}")

    def send_join_message(self, remote_peer):
        resp = self.connect_and_send(remote_peer, INSERT_PEER, self.id)[0]
        self.log(str(resp))  # FIXME

        if (resp[0] != REPLY) or self.peer_is_connected(remote_peer):
            # FIXME this "error state" isn't really an "error state"
            if "already inserted" not in resp[1]:
                # FIXME this return statements was used to prematurely stop
                # execution of build_peers, but it no longer does this. Raising
                # an exception would be more appropriate
                return

        self.add_peer(remote_peer)  # FIXME leaving here for now ...

    def send_quit_message(self, remote_peer):
        resp = self.connect_and_send(remote_peer, PEER_QUIT, self.id)

        # FIXME: why is this double nested??
        if resp[0][0] == REPLY:
            self.remove_peer(remote_peer)

    def query_peers(self, key, ttl="10"):
        # FIXME: a string ttl is weird
        for remote_peer in self.remote_peers:
            data = " ".join([self.id, key, ttl])
            self.connect_and_send(remote_peer, QUERY, data)
