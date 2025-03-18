import os
import subprocess

# Ruta del script de Python
SCRIPT_PATH = os.path.abspath("crypto_fetcher.py")
LOG_PATH = os.path.abspath("crypto_cron.log")

# Comando CRON para ejecutar el script diariamente a las 3 AM
CRON_JOB = f"0 3 * * * /usr/bin/python3 {SCRIPT_PATH} >> {LOG_PATH} 2>&1"


def add_cron_job():
    """ Agrega la tarea CRON si no existe """
    try:
        # Obtener la lista actual de tareas CRON
        cron_jobs = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        cron_list = cron_jobs.stdout.strip().split("\n") if cron_jobs.stdout else []
        
        # Verificar si la tarea ya existe
        if CRON_JOB in cron_list:
            print("✅ La tarea CRON ya está configurada.")
            return
        
        # Agregar la nueva tarea CRON
        cron_list.append(CRON_JOB)
        new_cron_jobs = "\n".join(cron_list) + "\n"
        subprocess.run(["crontab"], input=new_cron_jobs, text=True)
        print("✅ Tarea CRON añadida con éxito.")
    
    except Exception as e:
        print(f"❌ Error al configurar CRON: {e}")


if __name__ == "__main__":
    add_cron_job()
