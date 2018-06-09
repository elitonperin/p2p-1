import threading
import tkinter as tk
import p2p


class App(tk.Frame):
    def __init__(self,
                 port,
                 host,
                 add_local=False,
                 build_from=None,
                 debug=False):
        self.root = tk.Tk()

        super().__init__(self.root)

        self.peer = p2p.FileSharingPeer(8, port, host, debug)

        if add_local:
            self.peer.addlocalfile('hello.txt')
        if build_from:
            self.peer.build_peers(build_from.host, build_from.port, hops=8)

        self.thread = threading.Thread(target=self.peer.main_loop, args=([]))
        self.thread.start()

        self.pack()
        self.create_widgets()
        self.on_timer()

    def on_timer(self):
        self.on_refresh()
        self.after(3000, self.on_timer)

    def on_refresh(self):
        self.refresh_peer_list()

    def refresh_peer_list(self):
        # Delete previous, stale peers
        if self.peer_list.size() > 0:
            self.peer_list.delete(0, self.peer_list.size() - 1)
        # Copy over current peers
        for remote_peer in self.peer.remote_peers:
            self.peer_list.insert(tk.END, remote_peer.id)

    def create_widgets(self):
        # FIXME: I don't think I need to tack all these onto self ...
        self.peer_list = tk.Listbox(self)
        self.peer_list.grid(row=0)

        add_peer_frame = tk.Frame(self)
        add_peer_frame.grid(row=1)
        self.remove_peer_button = tk.Button(
            add_peer_frame, text="Delete", command=self.remove_peer)
        self.remove_peer_button.grid(row=0)
        self.add_peer_label = tk.Label(
            add_peer_frame, text='Add peer as "host:port"')
        self.add_peer_label.grid(row=1)
        self.add_peer_entry = tk.Entry(add_peer_frame)
        self.add_peer_entry.grid(row=2)
        self.add_peer_button = tk.Button(
            add_peer_frame, text='Add', command=self.add_peer)
        self.add_peer_button.grid(row=3)

        self.quit = tk.Button(
            self, text="QUIT", fg="red", command=self._destroy)
        self.quit.grid(row=2)

    def add_peer(self):
        address = self.add_peer_entry.get()
        host, port = address.split(':')
        self.peer.send_join_message(host, port)

    def remove_peer(self):
        index = self.peer_list.curselection()[0]
        rp = self.peer.remote_peers[index]
        self.peer.send_quit_message(rp.host, rp.port)

    def _destroy(self):
        self.root.destroy()
        self.peer.exit()
