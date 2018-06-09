import argparse
from p2p import FileSharingPeer
import gui


parser = argparse.ArgumentParser(description='testing btpeer.py')
parser.add_argument(
    '--send', choices=['NAME', 'LIST', 'JOIN', 'QUER', 'RESP', 'FGET', 'QUIT'])
parser.add_argument('--peer-port', type=str)
parser.add_argument('--run', action='store_true')
parser.add_argument('--build-from', type=int)
parser.add_argument('--port', type=int, required=True)
parser.add_argument('--add-file')
parser.add_argument('--file-get')
parser.add_argument('--query')
parser.add_argument('--gui', action='store_true')
args = parser.parse_args()

port = args.port
host = '0.0.0.0'
peer = FileSharingPeer(5, port, host)
peer.debug = True


if args.add_file:
    peer.add_local_file(args.add_file)
# FIXME: hard code one peer to see that the list is properly sent ...
if args.build_from:
    peer.build_peers(host, args.build_from, hops=10)
if args.file_get:
    file_name = args.file_get
    peer.connect_and_send(host, args.peer_port, 'FGET', file_name)
if args.query:
    file_name = args.query
    my_id = f'{host}:{port}'
    ttl = '5'
    data = ' '.join((my_id, file_name, ttl))
    print('querying')
    peer.connect_and_send(host, args.peer_port, 'QUER', data)
if args.gui:
    gui.main_loop(host=host, port=args.port)
if args.run:
    peer.main_loop()
