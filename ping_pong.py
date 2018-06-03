import sys
from btpeer import BTPeer

_, port, peer_port = sys.argv

host = '0.0.0.0'
peer = BTPeer(5, port)
peer.debug = True

PING = b'PING'
PONG = b'PONG'


def handle_ping(peerconn, msgdata):
    peerconn.senddata(PONG, b'msgdata')


def handle_pong(peerconn, msgdata):
    print(msgdata)


peer.addhandler(PING, handle_ping)

if port == '5001':
    response = peer.connectandsend(host, peer_port, PING, b'msgdata')
    print(response)

peer.mainloop()
