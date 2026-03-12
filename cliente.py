import socket

HOST = "127.0.0.1" # IP DEL SERVIDOR
PORT = 5000

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

client.connect((HOST, PORT))

client.sendall("Hola servidor".encode())

data = client.recv(1024)

print("Respuesta: ", data.decode())

client.close()