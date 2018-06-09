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

        self.thread = threading.Thread(
            target=self.peer.main_loop, args=([]))
        self.thread.start()

        self.pack()
        self.create_widgets()
        self.on_timer()

    def on_timer(self):
        self.on_refresh()
        self.after(3000, self.on_timer)

    def on_refresh(self):
        """copy state from `self.state.some_va r` over to `self.some_var"""
        self.num_peers.set(self.peer.num_peers())

    def create_widgets(self):
        self.num_peers = tk.IntVar()
        self.num_peers.text = self.peer.num_peers
        self.num_peers_label = tk.Label(root, textvariable=self.num_peers)
        self.num_peers_label.pack(side='top')

        self.quit = tk.Button(self, text="QUIT", fg="red",
                              command=root.destroy)
        self.quit.pack(side="bottom")


def main_loop(host, port):
    app = App(host=host, port=port, master=root)
    app.mainloop()
