import json
import os
import psycopg2
import sys
import time

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432
DEFAULT_CONF = "/etc/logtunnel/config.json"
DEFAULT_INTR = 0.5

interval = None
connection = None
tunnels = None


class LgtlTunnel:
    def __init__(self, **kw):
        self.file = kw["file"]
        self.table = kw["table"]
        self.format = kw["format"]
        self.delimiters = []
        self.columns = []
        self.mtime = 0
    
    def validate(self):
        if not os.path.exists(self.file):
            print(f"Couldn't find file to tunnel {self.file} (skipping)", file=sys.stderr)
            return False
        try:
            open(self.file, "w").close()
        except OSError:
            print(f"LogTunnel doesn't have read&write access to {self.file} (skipping)", file=sys.stderr)
            return False
        return True
    
    def create_format(self):
        column_name = False
        buffer = ""
        for c in self.format:
            if c == "$":
                if len(buffer) > 0:
                    (self.columns if column_name else self.delimiters).append(buffer)
                    buffer = ""
                column_name = not column_name
            else:
                buffer += c
        if len(buffer) > 0:
            (self.columns if column_name else self.delimiters).append(buffer)
    
    def send_logs(self):
        with open(self.file, "r+") as f:
            data = f.read()
            f.truncate(0)
        self.mtime = os.path.getmtime(self.file)
        
        lines = data.split("\n")
        cur = connection.cursor()
        for line in lines:
            column_data = "', '".join(self.get_log_line(line))
            column_names = ", ".join(self.columns)
            try:
                cur.execute(f"INSERT INTO {self.table} ({column_names}) VALUES ('{column_data}')")
            except psycopg2.errors.SyntaxError:
                pass
            except psycopg2.errors.InFailedSqlTransaction:
                connection.rollback()
            except Exception as e:
                print(f"Error rasied: {type(e).__name__}")
        connection.commit()
        
    def get_log_line(self, line):
        column_data = []
        buffer = ""
        delimiter = 0
        i = len(line)
        while i > 0:
            if delimiter >= len(self.delimiters):
                column_data.append(line)
                break
            buffer += line[0]
            line = line[1:]
            i -= 1
            if buffer.endswith(self.delimiters[delimiter]):
                buffer = buffer[0:len(buffer)-len(self.delimiters[delimiter])]
                delimiter += 1
                if len(buffer) > 0:
                    column_data.append(buffer)
                    buffer = ""
        return column_data


def main_loop():
    while True:
        if len(tunnels) == 0:
            print("No remaining connected tunnels (exiting)")
            exit(0)
        for tunnel in tunnels:
            try:
                if tunnel.mtime != os.path.getmtime(tunnel.file):
                    tunnel.send_logs()
            except OSError:
                print(f"Tunnel broken [{tunnel.file} -> {tunnel.table}]")
                tunnels.remove(tunnel)
                continue
            time.sleep(interval / len(tunnels))


def connect_db(obj):
    return psycopg2.connect(**obj)


def establish_tunnels(lst):
    established = []
    for entry in lst:
        tunnel = None
        try:
            tunnel = LgtlTunnel(**entry)
        except KeyError:
            print("A tunnel is configured wrong (skipping)", file=sys.stderr)
            continue
        
        if tunnel.validate():
            tunnel.mtime = os.path.getmtime(tunnel.file)
            tunnel.create_format()
            established.append(tunnel)
            print(f"Tunnel established [{tunnel.file} -> {tunnel.table}]")
    
    if len(established) == 0:
        print("No tunnels established", file=sys.stderr)
        exit(-1)
    
    return established


def load_config():
    configpath = None
    try:
        configpath = sys.argv[sys.argv.index("--config")+1]
    except ValueError:
        configpath = DEFAULT_CONF
    except IndexError:
        print("No configuration path after --config", file=sys.stderr)
        exit(-1)
    
    if not os.path.exists(configpath):
        print(f"Couldn't find configuration file {configpath}", file=sys.stderr)
        exit(-1)
    
    f = open(configpath, "r")
    config = None
    try:
        config = json.load(f)
    except json.JSONDecodeError:
        print("Failed to decode json config file", file=sys.stderr)
        exit(-1)
    finally:
        f.close()
    
    return config


if __name__ == "__main__":
    config = load_config()
    interval = config["interval"]/1000 if "interval" in config else DEFAULT_INTR
    tunnels = establish_tunnels(config["tunnels"])
    connection = connect_db(config["connection"])
    main_loop()