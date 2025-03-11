import configparser
import schedule
import time
import subprocess
import socket
import json

class Monitor:
    def __init__(self, config_file):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        try:
            self.current_interval = int(self.config['DEFAULT']['monitor_interval'])
        except ValueError:
            print("El valor de monitor_interval debe ser un entero. Seusará 1 minuto por defecto.")
            self.current_interval = 1

        # Programar el primer job con el intervalo inicial
        schedule.every(self.current_interval).minutes.do(self.job)
        print(f"Iniciando monitoreo del servidor cada {self.current_interval} minuto(s).")

    def servidor_corriendo(self, host, port, timeout=2):
        try:
            with socket.create_connection((host, port), timeout):
                return True
        except Exception:
            return False

    def restablecer_servidor(self, command):
        try:
            subprocess.Popen(command, shell=True, cwd=r"C:\Cliente-Servidor\Servidor")
            print("Servidor reiniciado.")
        except Exception as e:
            print("Error al reiniciar el servidor:", e)

    def actualizar_estado_servidores(self):
        servidores_activos_count = 0
        active_servers = {}
        # Iteramos por cada sección (excepto DEFAULT) del config
        for section in self.config.sections():
            host = self.config[section]['host']
            port = int(self.config[section]['port'])
            command = self.config[section]['server_command']
            if self.servidor_corriendo(host, port):
                print(f"Servidor {section} en línea.")
                servidores_activos_count += 1
                active_servers[section] = {"host": host, "port": port}
            else:
                print(f"Servidor {section} no responde. Reiniciando...")
                self.restablecer_servidor(command)
        # Guardamos la lista de servidores activos en un archivo JSON
        with open("active_servers.json", "w") as f:
            json.dump(active_servers, f)

        # Nuevo intervalo: 5 minutos si todos los servidores estánactivos, de lo contrario 1 minuto
        new_interval = 5 if servidores_activos_count == len(self.config.sections()) else 1

        # Si el intervalo ha cambiado, actualizamos la programación
        if new_interval != self.current_interval:
            self.current_interval = new_interval
            schedule.clear()  # Limpia todos los jobs programados
            schedule.every(self.current_interval).minutes.do(self.job)
            print(f"Intervalo de monitoreo cambiado a {self.current_interval} minuto(s).")

    def job(self):
        self.actualizar_estado_servidores()

    def run(self):
        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    monitor = Monitor("config.ini")
    monitor.run()