import threading
import tkinter as tk
from p2p import FileSharingPeer

root = tk.Tk()


class App(tk.Frame):
    def __init__(self, port, host, master=None, add_local=False):
        super().__init__(master)
        self.peer = FileSharingPeer(8, port, host)
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
        self.peer_list = tk.Listbox()
        self.peer_list.pack()

        self.quit = tk.Button(
            self, text="QUIT", fg="red", command=root.destroy)
        self.quit.pack(side="bottom")


def main_loop(host, port):
    app = App(host=host, port=port, master=root)
    app.mainloop()
