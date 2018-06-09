import threading
import tkinter as tk
import p2p

root = tk.Tk()


class App(tk.Frame):
    def __init__(self, port, host, master=None, add_local=False):
        super().__init__(master)

        self.peer = p2p.FileSharingPeer(8, port, host)
        if add_local:
            self.peer.addlocalfile('hello.txt')

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

        self.quit = tk.Button(
            self, text="QUIT", fg="red", command=root.destroy)
        self.quit.grid(row=1)

        add_peer_frame = tk.Frame(self)
        add_peer_frame.grid(row=2)
        self.add_peer_label = tk.Label(
            add_peer_frame, text='Add peer as "host:port"')
        self.add_peer_label.grid(row=0)
        self.add_peer_entry = tk.Entry(add_peer_frame)
        self.add_peer_entry.grid(row=1)
        self.add_peer_button = tk.Button(
            add_peer_frame, text='Add', command=self.add_peer)
        self.add_peer_button.grid(row=2)

    def add_peer(self):
        address = self.add_peer_entry.get()
        print('attempting to add peer: ', self.add_peer_entry.get())

        host, port = address.split(':')
        self.peer.send_join_message(host, port)


def main_loop(host, port):
    app = App(host=host, port=port, master=root)
    app.mainloop()
