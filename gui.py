import threading
import tkinter as tk
import p2p


class App(tk.Frame):
    def __init__(self, port, host, add_local=False, build_from=None, debug=False):
        self.root = tk.Tk()

        super().__init__(self.root)

        self.peer = p2p.FileSharingPeer(8, port, host, debug)

        if add_local:
            self.peer.addlocalfile("hello.txt")
        if build_from:
            self.peer.build_peers(build_from.host, build_from.port, hops=8)

        self.thread = threading.Thread(target=self.peer.main_loop, args=([]))
        self.thread.start()

        self.pack()
        self.create_widgets()
        self.on_timer()
        self.on_pruning_timer()

    def on_timer(self):
        self.on_refresh()
        self.after(3000, self.on_timer)

    def on_pruning_timer(self):
        self.peer.prune_peers()
        self.after(600000, self.on_pruning_timer)  # ping every 10 minutes

    def on_refresh(self):
        self.refresh_peer_list()
        self.refresh_file_list()

    def refresh_peer_list(self):
        # Delete previous, stale peers
        if self.peer_list.size() > 0:
            self.peer_list.delete(0, self.peer_list.size() - 1)
        # Copy over current peers
        for remote_peer in self.peer.remote_peers:
            self.peer_list.insert(tk.END, remote_peer.id)

    def refresh_file_list(self):
        # Delete previous, stale files
        if self.file_list.size() > 0:
            self.file_list.delete(0, self.file_list.size() - 1)
        # Copy over current files
        for filename, address in self.peer.files.items():
            if address is None:
                address = "local"
            self.file_list.insert(tk.END, f"{filename}@{address}")

    def create_widgets(self):
        # FIXME: I don't think I need to tack all these onto self ...

        # Root frames
        peer_frame = tk.Frame(self)
        peer_frame.grid(row=0, column=0)
        file_frame = tk.Frame(self)
        file_frame.grid(row=0, column=1)
        controls_frame = tk.Frame(self)
        controls_frame.grid(row=1, column=1)

        # Peer frame
        self.peer_list = tk.Listbox(peer_frame)
        self.peer_list.grid(row=0)
        self.remove_peer_button = tk.Button(
            peer_frame, text="Remove Peer", command=self.remove_peer
        )
        self.remove_peer_button.grid(row=1)
        self.add_peer_entry = tk.Entry(peer_frame)
        self.add_peer_entry.grid(row=2)
        self.add_peer_button = tk.Button(
            peer_frame, text="Add Peer (host:port)", command=self.add_peer
        )
        self.add_peer_button.grid(row=3)

        # Files frame
        self.file_list = tk.Listbox(file_frame)
        self.file_list.grid(row=0)
        self.remove_file_button = tk.Button(
            file_frame, text="Remove File", command=self.remove_file
        )
        self.remove_file_button.grid(row=1)
        self.add_file_entry = tk.Entry(file_frame)
        self.add_file_entry.grid(row=2)
        self.add_file_button = tk.Button(
            file_frame, text="Add Local File", command=self.add_file
        )
        self.add_file_button.grid(row=3)
        self.search_entry = tk.Entry(file_frame)
        self.search_entry.grid(row=4)
        self.search_button = tk.Button(file_frame, text="Search", command=self.search)
        self.search_button.grid(row=5)

        # Controls frame
        self.quit = tk.Button(
            controls_frame, text="QUIT", fg="red", command=self._destroy
        )
        self.quit.pack(anchor=tk.CENTER)

    def add_peer(self):
        address = self.add_peer_entry.get()
        host, port = address.split(":")
        self.peer.send_join_message(host, port)

    def remove_peer(self):
        # FIXME: multi-select
        index = self.peer_list.curselection()[0]
        rp = self.peer.remote_peers[index]
        self.peer.send_quit_message(rp.host, rp.port)

    def add_file(self):
        self.peer.add_local_file(self.add_file_entry.get())

    def remove_file(self):
        # FIXME: multi-select
        index = self.file_list.curselection()
        file_entry = self.file_list.get(index)
        filename, address = file_entry.split("@")
        self.peer.remove_local_file(filename)

    def search(self):
        self.peer.query_peers(self.search_entry.get())

    def _destroy(self):
        self.root.destroy()
        self.peer.exit()
