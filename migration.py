import os
import paramiko

# Constants
LOCAL_DUMP_FILE = "airbyte_dump.sql"
PSQL  = "/usr/bin/psql"
# Database Configuration
class Database:
    def __init__(self, host, name, user_name, password, port=5432):
        self.host = host
        self.name = name
        self.user_name = user_name
        self.password = password
        self.port = port

# Server Configuration
class Server:
    def __init__(self, host, port, username, password, key):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.key = key

airbyte_prod = Server(host="143.198.55.148", port=22, username="kpoojary", password="vasant63", key="/Users/kshitij.poojary/.ssh/id_rsa")
core_worker = Server(host="143.244.183.84", port=22, username="kpoojary", password="vasant63", key="/Users/kshitij.poojary/.ssh/id_rsa")
airbyte_prod_replica = Server(host="146.190.124.164", port=22, username="kpoojary", password="vasant63", key="/Users/kshitij.poojary/.ssh/id_rsa")

core_worker_db = Database(host='localhost', name='airbyte', user_name='postgres', password='devops')

def ssh_to_server(server: Server):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server.host, server.port, username=server.username, password=server.password, key_filename=server.key)
    return ssh

def dump_database_on_airbyte_prod():
    print("Dumping database on Airbyte production server...")
    ssh = ssh_to_server(airbyte_prod)
    
    dump_command = "echo 'vasant63' | sudo -S -p '' docker exec -e PGPASSWORD='docker' airbyte-db pg_dump -U docker -d airbyte > /home/kpoojary/airbyte_dump.sql"
    _, stdout, stderr = ssh.exec_command(dump_command)
    error = stderr.read().decode('utf-8')
    
    if error:
        print("Error dumping database on Airbyte production server:")
        print(error)
        ssh.close()
        raise Exception("Database dump failed on Airbyte production server.")
    
    sftp = ssh.open_sftp()
    sftp.get("/home/kpoojary/airbyte_dump.sql", LOCAL_DUMP_FILE)
    sftp.close()
    print(f"Database dump saved locally to {LOCAL_DUMP_FILE}.")
    ssh.close()

def transfer_dump_to_core_worker():
    print(f"Transferring dump file to Core Worker server at {core_worker.host}...")
    ssh = ssh_to_server(core_worker)
    sftp = ssh.open_sftp()
    
    remote_directory = "/home/kpoojary/"
    remote_file_path = os.path.join(remote_directory, LOCAL_DUMP_FILE)
    
    # Ensure remote directory exists
    try:
        sftp.chdir(remote_directory)
    except IOError:
        sftp.mkdir(remote_directory)
        sftp.chdir(remote_directory)
    
    # Transfer the dump file
    sftp.put(LOCAL_DUMP_FILE, remote_file_path)
    sftp.close()
    ssh.close()
    print(f"Dump file transferred to Core Worker server at {remote_file_path}.")

# Database Import on Core Worker
def import_dump_on_core_worker():
    ssh = ssh_to_server(core_worker)

    try :
        delete_command = f"PGPASSWORD='{core_worker_db.password}' psql -U {core_worker_db.user_name} -h {core_worker_db.host} -p {core_worker_db.port} -c 'DROP DATABASE IF EXISTS {core_worker_db.name} WITH (FORCE);'"
        ssh.exec_command(delete_command)
    except Exception as e :
        print('Delete database error',e)
        
    # Create a new database
    create_command = f"PGPASSWORD='{core_worker_db.password}' psql -U {core_worker_db.user_name} -h {core_worker_db.host} -p {core_worker_db.port} -c 'CREATE DATABASE {core_worker_db.name};'"
    ssh.exec_command(create_command)
    
    # Import the dump into the new database
    import_command = f"PGPASSWORD='{core_worker_db.password}' psql -U {core_worker_db.user_name} -d {core_worker_db.name} -h {core_worker_db.host} -p {core_worker_db.port} -f /home/kpoojary/{LOCAL_DUMP_FILE}"
    _, stdout, stderr = ssh.exec_command(import_command)
    
    error = stderr.read().decode('utf-8')
    if error:
        print(f"Error importing database on Core Worker: {error}")
        ssh.close()
        raise Exception("Database import failed on Core Worker.")
    
    print("Database import completed successfully.")
    ssh.close()

# Restart Airbyte on Airbyte Production Server
def restart_airbyte_on_prod():
    print("Restarting Airbyte on Airbyte production server...")
    ssh = ssh_to_server(airbyte_prod_replica)

    commands = [
        "cd /home/kpoojary",
        "echo 'vasant63' | sudo -S abctl local install --secret ./secret.yaml",
        "echo 'vasant63' | sudo -S abctl local install --values ./values.yaml",
        "echo 'vasant63' | sudo -S abctl local install --secret ./secret.yaml",
        
    ]
    for command in commands:
        _, stdout, stderr = ssh.exec_command(command)
        print(stdout.read().decode())
        error = stderr.read().decode()
        if error:
            print(f"Error restarting Airbyte service: {error}")
    
    print("Airbyte restarted successfully.")
    ssh.close()

# Cleanup Local Dump File
def cleanup_local_dump():
    try:
        os.remove(LOCAL_DUMP_FILE)
        print(f"Local dump file {LOCAL_DUMP_FILE} removed successfully.")
    except Exception as e:
        print(f"Failed to remove local dump file: {e}")

# Main Execution Flow
try:
    # dump_database_on_airbyte_prod()
    # transfer_dump_to_core_worker()
    # import_dump_on_core_worker()
    restart_airbyte_on_prod()
    # cleanup_local_dump()
except Exception as e:
    print(f"An error occurred during the execution: {e}")
