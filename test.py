from __future__ import print_function
import argparse
import sys
import os
import sqlite3
import ipaddr
from datetime import timedelta, datetime
import socket
from tornado.gen import Task, engine
from tornado.ioloop import IOLoop
import errno
import fcntl
import dateutil.parser
from StringIO import StringIO


DB_PATH = 'history.db'
RESET_WAIT = timedelta(seconds=(120 + 10))
STOP_DELAY = 200


class SQLiteControlledExecution(object):
    def __init__(self, db_path):
        self.db_path = db_path

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path, isolation_level=None)
        self.conn.text_factory = str
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, type, value, traceback):
        # self.conn.commit()
        self.conn.close()
        return False


class ExceptionIgnored(object):
    def __enter__(self):
        return None

    def __exit__(self, type, value, traceback):
        return True


def log(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def set_nonblocking(fd):
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)


def create_database():
    with SQLiteControlledExecution(DB_PATH) as c:
        c.execute('''CREATE TABLE IF NOT EXISTS history
                    (id  INTEGER PRIMARY KEY AUTOINCREMENT, sent text,
                    received text, status text, connected_at datetime,
                    disconnected_at datetime, ip text, port integer)''')
        c.execute('''CREATE INDEX IF NOT EXISTS
                    id_history_host ON history (ip ASC)''')
        c.execute('''CREATE INDEX IF NOT EXISTS
                    id_history_port ON history (port ASC)''')

        c.execute('''CREATE TABLE IF NOT EXISTS hosts
                    (id  INTEGER PRIMARY KEY AUTOINCREMENT,
                     ip text, port integer)''')
        c.execute('''CREATE INDEX IF NOT EXISTS
                    id_hosts_ip ON history (ip ASC)''')
        c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS
                    id_hosts_host_port ON hosts(ip, port)''')


def seed_addresses(f):
    with SQLiteControlledExecution(DB_PATH) as c:
        for line in f:
            line = line.strip().split("#")[-1]
            if line:
                network = ipaddr.ip_network(line.strip())
                hosts = list(network.iterhosts())
                log("[I]", "Adding %d hosts from %s" % (len(hosts), network))

                rows = [(None, host.exploded, 80) for host in hosts]
                c.executemany('INSERT OR IGNORE INTO hosts VALUES (?,?,?)',
                    rows)


def dirty_addresses():
    with SQLiteControlledExecution(DB_PATH) as c:
        c.execute('''SELECT ip, port, disconnected_at FROM history
            WHERE STATUS = "RESET"''')
        rows = c.fetchall()

        now = datetime.utcnow()
        dirty = []
        for ip, port, disconnected_at in rows:
            disconnected_at = dateutil.parser.parse(disconnected_at)
            if disconnected_at + RESET_WAIT > now:
                dirty.append((ip, port))
    return dirty


def pop_clean():
    dirty = dirty_addresses()
    ret = None
    with SQLiteControlledExecution(DB_PATH) as c:
        for ip, port in c.execute(
            '''SELECT ip, port FROM hosts ORDER BY ip ASC'''):
            if (ip, port) not in dirty:
                ret = (ip, port)
                break
    return ret


def run_socket(datasource):
    io_loop = IOLoop()
    io_loop.install()

    host = pop_clean()
    if host is None:
        log("[E]",
            '''No clean host found. Use "--seed default.txt" to add more.''')
        return
    log("[I]", "Connecting to (%s:%d)" % host)

    with SQLiteControlledExecution(DB_PATH) as c:
        c.execute('INSERT INTO history VALUES (?,?,?,?,?,?,?,?)',
            (None, "", "", "CONNECTING", None, None, host[0], host[1]))
        c.execute('SELECT id FROM history WHERE ROWID = ?', (c.lastrowid, ))
        row_id = c.fetchone()[0]

        received = StringIO()
        sent = StringIO()

        def update_connected():
            c.execute('''UPDATE history SET status = ?, connected_at = ?
                         WHERE id = ?''',
                         ("CONNECTED", datetime.utcnow(), row_id, ))

        def update_status(status):
            c.execute('UPDATE history SET status = ? WHERE id = ?',
                    (status, row_id, ))

        def update_final_status(status):
            c.execute('''UPDATE history SET disconnected_at = ?, status = ?,
                         sent = ?, received = ?
                         WHERE id = ?''',
                    (datetime.utcnow(), status, sent.getvalue(),
                        received.getvalue(), row_id, ))

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            s.connect(host)
        except socket.error as e:
            if e.errno == errno.ECONNRESET:
                update_final_status("RESET")
            else:
                raise

        s.setblocking(0)
        set_nonblocking(datasource)

        log("[I]", "Connected.")
        update_connected()

        def read_till_block(fd, step=1):
            data = ""
            while True:
                try:
                    tmp = os.read(fd, step)
                    if not tmp:
                        return data if data else None
                    data += tmp
                except OSError as e:
                    if e.args[0] == errno.EWOULDBLOCK:
                        break
                    else:
                        raise
            return data

        @engine
        def stop():
            yield Task(io_loop.add_timeout, timedelta(milliseconds=STOP_DELAY))
            io_loop.stop()

        def process_socket_errno(no):
            log("[E]", errno.errorcode[no] + ":", os.strerror(no))
            if no == errno.ECONNRESET:
                update_final_status("RESET")
            else:
                update_final_status(errno.errorcode[no])
            io_loop.remove_handler(s.fileno())
            io_loop.remove_handler(datasource.fileno())
            stop()

        def on_socket_events(fd, events):
            if events & io_loop.READ:
                try:
                    data = read_till_block(fd)
                except OSError as e:
                    process_socket_errno(e.args[0])
                    return

                if data is not None:
                    with ExceptionIgnored():
                        sys.stdout.write(data)
                    received.write(data)
                else:
                    log("[I]", "Socket closed.")
                    update_final_status("DISCONNECTED")
                    io_loop.remove_handler(s.fileno())
                    io_loop.remove_handler(datasource.fileno())
                    stop()

            if events & io_loop.ERROR:
                no = s.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                process_socket_errno(no)
                return

        def on_file_events(fd, events):
            if events & io_loop.READ:
                data = read_till_block(fd)
                if data is not None:
                    s.send(data)
                    sent.write(data)
                else:
                    log("[I]", "Input file closed.")
                    io_loop.remove_handler(fd)

        io_loop.add_handler(s.fileno(), on_socket_events, io_loop.READ)
        io_loop.add_handler(datasource.fileno(), on_file_events, io_loop.READ)

        try:
            io_loop.start()
        except KeyboardInterrupt:
            s.close()
            log("[I]", "Disconnected.")
            update_final_status("DISCONNECTED")
            stop()

if __name__ == '__main__':
    create_database()

    parser = argparse.ArgumentParser(description='')

    parser.add_argument('input', nargs='?', type=argparse.FileType('r'),
                default=sys.stdin, help="Input file for testing payload. "
                "Default: stdin")

    parser.add_argument('--seed', type=argparse.FileType('r'),
                help="Seed hosts from file, e.g. google_network.txt")
    args = parser.parse_args()

    if args.seed:
        seed_addresses(args.seed)

    run_socket(args.input)
