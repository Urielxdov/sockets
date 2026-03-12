import socket

HOST = "0.0.0.0" # Acepta conexiones de cualquier IP
PORT = 5000


server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

server.bind((HOST, PORT))
server.listen()

print("Servidor activo")

conn, addr = server.accept()

print("Conectado por: ", addr)

data = conn.recv(1024)
print("Mensaje recibido: ", data.decode())

conn.sendall("Hola cliente".encode())
conn.close()