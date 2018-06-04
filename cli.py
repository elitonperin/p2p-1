import argparse
from p2p import FileSharingPeer

parser = argparse.ArgumentParser(description='testing btpeer.py')
parser.add_argument('--send', choices=['NAME', 'LIST', 'JOIN'])
parser.add_argument('--peerport', type=str)
parser.add_argument('--run', action='store_true')
parser.add_argument('--build-from', type=int)
parser.add_argument('--port', type=int, required=True)
args = parser.parse_args()

port = args.port
host = '0.0.0.0'
peer = FileSharingPeer(5, port, serverhost=host)
peer.debug = True

if args.run:
    # FIXME: hard code one peer to see that the list is properly sent ...
    if args.build_from:
        peer.build_peers(host, args.build_from, hops=10)
    peer.mainloop()
