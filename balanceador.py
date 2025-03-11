import socket
import threading
import json

# Diccionario global para mantener la asignación sticky:
# Clave: IP del cliente, Valor: configuración del servidor asignado
sticky_sessions = {}

# Variable global para round-robin para nuevos clientes
server_index = 0

def handle_client(client_socket, server_config, client_key):
    """
    Redirige la conexión del cliente al servidor correspondiente,
    usando la configuración dada. 'client_key' es la clave identificadora
    del cliente (en este caso, su IP).
    """
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Conectar al servidor usando host y puerto (convertido a entero)
        server_socket.connect((server_config["host"], int(server_config["port"])))
    except Exception as e:
        print(f"Error al conectar con el servidor para {client_key}: {e}")
        client_socket.close()
        return

    def forward(source, destination):
        try:
            while True:
                data = source.recv(4096)
                if not data:
                    break
                destination.sendall(data)
        except Exception:
            pass
        finally:
            source.close()
            destination.close()

    # Crear dos hilos para reenvío bidireccional
    t1 = threading.Thread(target=forward, args=(client_socket, server_socket))
    t2 = threading.Thread(target=forward, args=(server_socket, client_socket))
    t1.daemon = True
    t2.daemon = True
    t1.start()
    t2.start()

def load_active_servers():
    """
    Lee el archivo active_servers.json y devuelve una lista
    con la configuración de cada servidor activo.
    Se espera que active_servers.json sea un diccionario, por ejemplo:
    {
      "Servidor1": {"host": "localhost", "port": 5000},
      "Servidor2": {"host": "localhost", "port": 5001}
    }
    """
    try:
        with open("active_servers.json", "r") as f:
            servers = json.load(f)
        return list(servers.values())
    except Exception as e:
        print("Error al leer active_servers.json:", e)
        return []

def main():
    global server_index
    listen_port = 9000  # Puerto en el que el balanceador escucha

    balancer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    balancer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    balancer_socket.bind(("", listen_port))
    balancer_socket.listen(5)
    print("Balanceador escuchando en el puerto", listen_port)

    while True:
        client_socket, addr = balancer_socket.accept()
        client_ip = addr[0]
        client_key = client_ip  # Aquí usamos la IP como identificador (sticky)
        print("Conexión entrante de", addr)

        # Cargar la lista actual de servidores activos
        active_servers = load_active_servers()
        if not active_servers:
            print("No hay servidores activos disponibles. Cerrando conexión.")
            client_socket.close()
            continue

        # Verificar si este cliente ya tiene una asignación sticky
        if client_key in sticky_sessions:
            assigned_server = sticky_sessions[client_key]
            # Comprobar que el servidor asignado sigue activo
            if any(
                s["host"] == assigned_server["host"] and int(s["port"]) == int(assigned_server["port"])
                for s in active_servers
            ):
                server_config = assigned_server
            else:
                # Si ya no está activo, se reasigna uno nuevo con round-robin
                server_config = active_servers[server_index % len(active_servers)]
                server_index += 1
                sticky_sessions[client_key] = server_config
        else:
            # Para un nuevo cliente, asignar un servidor mediante round-robin
            server_config = active_servers[server_index % len(active_servers)]
            server_index += 1
            sticky_sessions[client_key] = server_config

        print(f"Redirigiendo al cliente {client_key} al servidor: {server_config}")

        # Lanzar un hilo para manejar la conexión
        handler_thread = threading.Thread(target=handle_client, args=(client_socket, server_config, client_key))
        handler_thread.daemon = True
        handler_thread.start()

if __name__ == "__main__":
    main()
