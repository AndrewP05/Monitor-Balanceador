import json
import socket
import threading

class LoadBalancer:
    def __init__(self, json_path="active_servers.json", port=9000):
        self.json_path = json_path
        self.port = port
        self.lock_file = threading.Lock()

    def seleccionar_servidor_least_connections(self):
        """
        Lee el archivo JSON y selecciona el servidor con la menor cantidad
        de conexiones activas (campo 'activos'). Incrementa ese contador y devuelve
        la clave y datos del servidor seleccionado.
        """
        with self.lock_file:
            try:
                with open(self.json_path, "r") as f:
                    data = json.load(f)
            except Exception as e:
                raise Exception("Error al leer el archivo JSON: " + str(e))
            disponibles = []
            for key, servidor in data.items():
                # Si el campo 'activos' no existe, se inicializa en 0
                if "activos" not in servidor:
                    servidor["activos"] = 0
                disponibles.append((key, servidor))
            if not disponibles:
                raise Exception("No hay servidores disponibles.")
            # Selecciona el servidor con el menor número de conexiones activas
            chosen_key, chosen_server = min(disponibles, key=lambda item: item[1].get("activos", 0))
            data[chosen_key]["activos"] = data[chosen_key].get("activos", 0) + 1
            chosen_server["activos"] = data[chosen_key]["activos"]
            try:
                with open(self.json_path, "w") as f:
                    json.dump(data, f)
            except Exception as e:
                print("Error al actualizar el archivo JSON:", e)
            return chosen_key, chosen_server

    def finalizar_conexion(self, servidor_key):
        """
        Decrementa el contador de conexiones activas para el servidor seleccionado.
        Se llama al finalizar la conexión proxy.
        """
        with self.lock_file:
            try:
                with open(self.json_path, "r") as f:
                    data = json.load(f)
            except Exception as e:
                print("Error al leer el archivo JSON en finalizar_conexion:", e)
                return
            if servidor_key in data:
                data[servidor_key]["activos"] = max(data[servidor_key].get("activos", 1) - 1, 0)
            try:
                with open(self.json_path, "w") as f:
                    json.dump(data, f)
            except Exception as e:
                print("Error al escribir en el archivo JSON en finalizar_conexion:", e)

    def forward(self, src, dst):
        try:
            while True:
                data = src.recv(4096)
                if not data:
                    try:
                        dst.shutdown(socket.SHUT_WR)
                    except Exception as e:
                        print("Error al hacer shutdown:", e)
                    break
                dst.sendall(data)
        except Exception as e:
            print("Error en el forward:", e)
        # No cerramos los sockets aquí para dejar que el otro hilo los maneje

    def manejar_conexion(self, cliente_socket):
        """
        Selecciona el servidor backend utilizando Least Connections y establece
        una conexión proxy bidireccional entre el cliente y el servidor seleccionado.
        """
        servidor_key = None
        try:
            servidor_key, servidor = self.seleccionar_servidor_least_connections()
            print(f"Conectando cliente a {servidor['host']}:{servidor['port']} (conexiones activas: {servidor['activos']})")
            # Se conecta al servidor backend
            backend_socket = socket.create_connection((servidor["host"], servidor["port"]))
            
            # Crea dos hilos para reenviar datos en ambas direcciones
            t1 = threading.Thread(target=self.forward, args=(cliente_socket, backend_socket))
            t2 = threading.Thread(target=self.forward, args=(backend_socket, cliente_socket))
            t1.start()
            t2.start()
            t1.join()
            t2.join()
        except Exception as e:
            print("Error al manejar la conexión:", e)
            try:
                cliente_socket.close()
            except:
                pass
        finally:
            if servidor_key:
                try:
                    self.finalizar_conexion(servidor_key)
                except Exception as e:
                    print("Error al finalizar la conexión:", e)

    def iniciar(self):
        """
        Inicia el balanceador en el puerto especificado. Por cada conexión entrante,
        se crea un hilo para manejar la conexión proxy entre el cliente y el servidor backend.
        """
        balanceador = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        balanceador.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        balanceador.bind(("0.0.0.0", self.port))
        balanceador.listen(5)
        print(f"Balanceador escuchando en el puerto {self.port}...")
        while True:
            cliente, addr = balanceador.accept()
            print(f"Conexión entrante de {addr}")
            threading.Thread(target=self.manejar_conexion, args=(cliente,)).start()

if __name__ == "__main__":
    lb = LoadBalancer()
    lb.iniciar()
