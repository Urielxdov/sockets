import socket
import threading
import queue
import tkinter as tk
from tkinter import scrolledtext, messagebox

BUFFER = 4096


class ChatClient:
    def __init__(self):
        self.sock = None
        self.recv_queue: queue.Queue = queue.Queue()
        self.username = ""
        self.connected = False

        self.root = tk.Tk()
        self.root.title("Chat Sockets")
        self.root.resizable(False, False)
        self._build_login()
        self.root.mainloop()

    # ── Login ──────────────────────────────────────────────────────────────

    def _build_login(self):
        self.root.geometry("320x230")
        frame = tk.Frame(self.root, padx=20, pady=20)
        frame.pack(expand=True)

        tk.Label(frame, text="Chat Sockets", font=("Arial", 16, "bold")).grid(
            row=0, columnspan=2, pady=(0, 15)
        )

        fields = [
            ("IP Servidor:", "127.0.0.1", "ip_var"),
            ("Puerto:",      "5000",      "port_var"),
            ("Nombre:",      "",          "name_var"),
        ]
        for i, (label, default, attr) in enumerate(fields, start=1):
            tk.Label(frame, text=label).grid(row=i, column=0, sticky="e", pady=4)
            var = tk.StringVar(value=default)
            setattr(self, attr, var)
            entry = tk.Entry(frame, textvariable=var, width=18)
            entry.grid(row=i, column=1, sticky="w")
            if attr == "name_var":
                entry.focus()
                entry.bind("<Return>", lambda e: self._connect())

        tk.Button(
            frame, text="Conectar", command=self._connect,
            width=20, bg="#4CAF50", fg="white", font=("Arial", 10, "bold"),
        ).grid(row=4, columnspan=2, pady=(15, 0))

    def _connect(self):
        ip = self.ip_var.get().strip()
        port_str = self.port_var.get().strip()
        username = self.name_var.get().strip()

        if not ip or not port_str or not username:
            messagebox.showerror("Error", "Todos los campos son requeridos")
            return

        try:
            port = int(port_str)
        except ValueError:
            messagebox.showerror("Error", "Puerto debe ser un número")
            return

        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((ip, port))
            self.sock.sendall(f"USERNAME:{username}\n".encode())

            self.sock.settimeout(5.0)
            try:
                data = self.sock.recv(BUFFER).decode("utf-8", errors="replace")
            finally:
                self.sock.settimeout(None)

            lines = [l.strip() for l in data.split("\n") if l.strip()]
            if not lines:
                raise ConnectionError("Sin respuesta del servidor")
            if lines[0].startswith("ERROR:"):
                messagebox.showerror("Error", lines[0][6:])
                self.sock.close()
                self.sock = None
                return

            self.username = username
            self.connected = True
            self._build_chat()

            for line in lines:
                self._process_message(line)

            threading.Thread(target=self._recv_loop, daemon=True).start()
            self.root.after(100, self._check_queue)

        except Exception as e:
            messagebox.showerror("Error de conexión", str(e))
            if self.sock:
                self.sock.close()
                self.sock = None

    # ── Chat UI ────────────────────────────────────────────────────────────

    def _build_chat(self):
        for w in self.root.winfo_children():
            w.destroy()

        self.root.geometry("720x520")
        self.root.resizable(True, True)
        self.root.title(f"Chat — {self.username}")

        top = tk.Frame(self.root)
        top.pack(fill="both", expand=True, padx=8, pady=(8, 0))

        self.msg_area = scrolledtext.ScrolledText(
            top, state="disabled", wrap="word",
            font=("Consolas", 10), cursor="arrow",
        )
        self.msg_area.pack(side="left", fill="both", expand=True)
        self.msg_area.tag_config("system", foreground="#888888",
                                  font=("Consolas", 10, "italic"))
        self.msg_area.tag_config("own",    foreground="#1565C0",
                                  font=("Consolas", 10, "bold"))
        self.msg_area.tag_config("other",  foreground="#212121")

        right = tk.Frame(top, width=150, relief="sunken", bd=1)
        right.pack(side="right", fill="y", padx=(6, 0))
        right.pack_propagate(False)
        tk.Label(right, text="Conectados", font=("Arial", 10, "bold"),
                 bg="#E3F2FD").pack(fill="x")
        self.users_list = tk.Listbox(right, font=("Arial", 10), bd=0,
                                      selectbackground="#BBDEFB")
        self.users_list.pack(fill="both", expand=True)

        bottom = tk.Frame(self.root)
        bottom.pack(fill="x", padx=8, pady=8)
        self.msg_entry = tk.Entry(bottom, font=("Consolas", 11))
        self.msg_entry.pack(side="left", fill="x", expand=True)
        self.msg_entry.bind("<Return>", lambda e: self._send())
        self.msg_entry.focus()
        tk.Button(
            bottom, text="Enviar", command=self._send,
            bg="#1565C0", fg="white", width=10, font=("Arial", 10, "bold"),
        ).pack(side="right", padx=(6, 0))

    # ── Networking ─────────────────────────────────────────────────────────

    def _send(self):
        text = self.msg_entry.get().strip()
        if not text or not self.connected:
            return
        try:
            self.sock.sendall((text + "\n").encode())
            self._append_message(f"[Tú]: {text}", "own")
            self.msg_entry.delete(0, "end")
        except Exception:
            self._on_disconnect()

    def _recv_loop(self):
        buf = ""
        try:
            while True:
                chunk = self.sock.recv(BUFFER)
                if not chunk:
                    break
                buf += chunk.decode("utf-8", errors="replace")
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    if line.strip():
                        self.recv_queue.put(line.strip())
        except Exception:
            pass
        finally:
            self.recv_queue.put("__DISCONNECTED__")

    def _check_queue(self):
        try:
            while True:
                msg = self.recv_queue.get_nowait()
                if msg == "__DISCONNECTED__":
                    self._on_disconnect()
                    return
                self._process_message(msg)
        except queue.Empty:
            pass
        self.root.after(100, self._check_queue)

    def _process_message(self, msg: str):
        if msg.startswith("MSG:"):
            parts = msg[4:].split(":", 1)
            if len(parts) == 2:
                sender, text = parts
                self._append_message(f"[{sender}]: {text}", "other")
        elif msg.startswith("JOIN:"):
            self._append_message(f"  {msg[5:]} se unió al chat", "system")
        elif msg.startswith("LEAVE:"):
            self._append_message(f"  {msg[6:]} salió del chat", "system")
        elif msg.startswith("USERS:"):
            users = [u for u in msg[6:].split(",") if u]
            self.users_list.delete(0, "end")
            for u in users:
                self.users_list.insert("end", f"  {u}")
        elif msg.startswith("ERROR:"):
            messagebox.showerror("Error del servidor", msg[6:])

    def _append_message(self, text: str, tag: str):
        self.msg_area.config(state="normal")
        self.msg_area.insert("end", text + "\n", tag)
        self.msg_area.see("end")
        self.msg_area.config(state="disabled")

    def _on_disconnect(self):
        self.connected = False
        if hasattr(self, "msg_entry"):
            self.msg_entry.config(state="disabled")
            self._append_message("  Desconectado del servidor.", "system")


if __name__ == "__main__":
    ChatClient()
