import utils
import argparse
from btpeer import BTPeer

# Requests a peer to reply with its "official" peer id.
# FIXME: This only makes sense if pid != host:peer ... long way down the road
NAME = b'NAME'

NAME_RESP = b'NAME_RESP'  # FIXME

# Requests a peer to reply with the list of peers that it knows about.
LIST = b'LIST'

LIST_RESP = b'LIST_RESP'  # FIXME

# (pid host port): Requests a peer to add the supplied host/port combination,
# associated with the node identified by pid, to its list of known peers.
JOIN = b'JOIN'


def send_name(peer, host, port):
    peer.connectandsend(host, port, NAME, b'')


def handle_name(peerconn, msgdata, receiving_node):
    peerconn.senddata(NAME_RESP, str(receiving_node.myid).encode())


def send_list(peer, host, port):
    r = peer.connectandsend(host, port, LIST, b'')
    response = r[0][1]
    print(utils.deserialize(response))


def handle_list(peerconn, msgdata, receiving_node):
    peerconn.senddata(LIST_RESP, utils.serialize(receiving_node.peers))


def handle_join(peerconn, msgdata, receiving_node):
    pass


parser = argparse.ArgumentParser(description='testing btpeer.py')
parser.add_argument('--send', choices=['NAME', 'LIST', 'JOIN'])
parser.add_argument('--peerport', type=int)
parser.add_argument('--run', action='store_true')
parser.add_argument('--port', type=int, required=True)
args = parser.parse_args()

port = args.port
host = '0.0.0.0'
peer = BTPeer(5, port, serverhost=host)
peer.debug = True

peer.addhandler(NAME, handle_name)
peer.addhandler(LIST, handle_list)

if args.send:
    if args.send == 'NAME':
        peerport = args.peerport
        send_name(peer, host, peerport)
    if args.send == 'LIST':
        peerport = args.peerport
        send_list(peer, host, peerport)

if args.run:
    # FIXME: hard code one peer to see that the list is properly sent ...
    peer.addpeer(f'{host}:5000', host, 5000)
    peer.mainloop()
