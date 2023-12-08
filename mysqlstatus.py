#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This is monitoring tool for MySQL 
"""

import argparse
import curses
import getpass
import logging
import os
import sys
import threading
import time
from datetime import datetime
from pytz import timezone

import mysql.connector as Database

__title__ = 'mysqlstatus'
__version__ = '1.0.0-DEV'
__original_author__ = 'Shoma Suzuki'
__modified_author__ = 'khkwon01'
__license__ = 'MIT'
__copyright__ = 'Copyright 2023'


def get_args_parser():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-h", "--host",
        default="localhost",
        nargs='?',
        type=str,
        help="Connect to host.")
    parser.add_argument("-p", "--port",
        default=3306,
        nargs='?',
        type=int,
        help="Port number to use for connection.")
    parser.add_argument("-u", "--user",
        default=getpass.getuser(),
        nargs='?',
        type=str,
        help="User for login if not current user.")
    parser.add_argument("-P", "--password",
        default='',
        nargs='?',
        type=str,
        help="Password to use when connecting to server.")
    parser.add_argument("-i", "--interval",
        default=1,
        nargs='?',
        type=int,
        help="Interval second of monitoring.")
    parser.add_argument("-o", "--outfile",
        default=sys.stdout,
        nargs='?',
        type=argparse.FileType('w'),
        help="Output result file. avairable for non-interactive.")
    parser.add_argument("-n", "--nonint",
        default=False,
        action='store_true',
        help="Non-interactive.")
    parser.add_argument("-m", "--mode",
        default='status',
        nargs='?',
        choices=['status', 'process', 'global'],
        help="monitoring Mode")
    parser.add_argument("--debug",
        default=False,
        action='store_true',
        help="Debug log enable.")
    parser.add_argument("--help",
        default=False,
        action='store_true',
        help="show this help message and exit.")
    return parser


class QueryThread(threading.Thread):
    _stop = False
    _update = False
    _mysql_variables = None
    _mysql_status = None
    _mysql_procesesslist = None
    _mysql_global = None

    def __init__(self, **kwargs):

        self.mysql_last_status = None

        self._db = kwargs.get('db')
        self._cursor = self._db.cursor(dictionary=True)
        self._interval = kwargs.get('interval', 1)
        self._mode = 'status'

        self.lock = threading.Lock()

        threading.Thread.__init__(self, name="QueryThread")
        self.setDaemon(True)

    @property
    def mysql_variables(self):
        """SHOW VARIABLES"""
        if self._mysql_variables is None:
            result = self.query("SHOW VARIABLES")
            self._mysql_variables = self.to_dict(result)
            logging.debug(self._mysql_variables)
        return self._mysql_variables

    @property
    def mysql_status(self):
        return self._mysql_status

    @property
    def mode(self):
        return self._mode

    @property
    def update(self):
        return self._update

    @update.setter
    def update(self, value):
        self._update = value

    @mode.setter
    def mode(self, value):
        if value == 'process':
            self._mode = 'process'
        elif value == 'status':
            self._mode = 'status'
        else:
            self._mode = 'global'

    @property
    def stop(self):
        return self._stop

    @stop.setter
    def stop(self, value):
        self._stop = value

    @property
    def mysql_procesesslist(self):
        return self._mysql_procesesslist

    @property
    def mysql_global(self):
        return self._mysql_global

    def run(self):
        while self._stop == False:
            if self._mode == 'process':
                self.get_procesesslist()
            elif self._mode == 'status':
                self.get_status()
            else:
                self.get_global()
            time.sleep(self._interval)
        self.cleanup_mysql()

    def cleanup_mysql(self):
        self._cursor.close()
        self._db.close()

    def query(self, sql):
        result = ()
        try:
            self.lock.acquire()
            self._cursor.execute(sql)
            result = self._cursor.fetchall()
            self.lock.release()
        except Exception as err:
            logging.exception(err)
        return result

    def get_status(self):
        """ SHOW GLOBAL STATUS """
        if self._mysql_status is not None:
            self.mysql_last_status = self._mysql_status
        result = self.query("SHOW GLOBAL STATUS")
        self._mysql_status = self.to_dict(result)
        #logging.debug(self._mysql_status)
        self.get_query_per_second()
        self._update = True
        return self._mysql_status

    def get_procesesslist(self):
        """SHOW PROCESSLIST"""
        result = self.query("SELECT ID, HOST, DB, TIME, STATE, INFO FROM INFORMATION_SCHEMA.PROCESSLIST ORDER BY TIME DESC")
        if result is not None:
            self._mysql_procesesslist = result
            self._update = True

        logging.debug(result)
        return self._mysql_procesesslist

    def get_global(self):
        """ """
        result = self.query("select total_allocated from sys.memory_global_total")
        logging.debug(result)

        return self._mysql_global

    def get_query_per_second(self):
        if self._mysql_status is None:
            return 0.0
        if self.mysql_last_status is not None:
            [current, last] = map(lambda x: float(x),
                (self._mysql_status.get('Uptime'),
                 self.mysql_last_status.get('Uptime')))
            elapsed_time = last - current

            [current, last] = map(lambda x: float(x),
                (self._mysql_status.get('Questions', 0),
                self.mysql_last_status.get('Questions', 0)))
            inc_query = last - current
        else:
            [elapsed_time, inc_query] = map(lambda x: float(x),
                (self._mysql_status.get('Uptime', 0),
                self._mysql_status.get('Questions', 0)))
        try:
            qps = inc_query / elapsed_time
            buffhit = ((float(self._mysql_status.get('Innodb_buffer_pool_read_requests')) / (float(self._mysql_status.get('Innodb_buffer_pool_read_requests')) + float(self._mysql_status.get('Innodb_buffer_pool_reads')))) * 100)
        except:
            qps = 0.0
            buffhit = 0.0

        self._mysql_status.update({'QPS': "%0.2f" % qps})
        self._mysql_status.update({'Buffer_hit': "%.2f" % float(buffhit)})

        del self._mysql_status['Innodb_buffer_pool_read_requests']
        del self._mysql_status['Innodb_buffer_pool_reads']

        return qps, buffhit

    def to_dict(self, dictset):
        return dict(
            map(
                lambda x: (x.get('Variable_name'), x.get('Value')),
                dictset))


class MySQLStatus:
    keywords = (
        "Buffer_hit",    
        "QPS",
        "Aborted_connects",
        "Binlog_cache_disk_use",
        "Bytes_received",
        "Bytes_sent",
        "Connections",
        "Created_tmp_disk_tables",
        "Created_tmp_files",
        "Created_tmp_tables",
        "Handler_delete",
        "Handler_read_first",
        "Handler_read_rnd",
        "Handler_read_rnd_next",
        "Handler_update",
        "Handler_write",
        # "Key_read_requests",
        # "Key_reads",
        "Max_used_connections",
        "Open_files",
        "Opened_table_definitions",
        "Opened_tables",
        "Opened_tables",
        "Questions",
        "Select_full_join",
        "Select_full_range_join",
        "Select_range",
        "Select_range_check",
        "Select_scan",
        "Slave_running",
        "Slow_queries",
        "Sort_merge_passes",
        "Sort_scan",
        "Table_locks_immediate",
        "Table_locks_waited",
        "Threads_connected",
        "Threads_created",
        "Threads_running",
        "Uptime",
    )

    def __init__(self, options):
        self.options = options

        try:
            db = Database.connect(
                host=self.options.host,
                user=self.options.user,
                port=self.options.port,
                passwd=self.options.password)
        except Exception as err:
            logging.exception(err)
            print(err)
            sys.exit()

        self.qthread = QueryThread(
            db=db,
            interval=options.interval,
        )
        self.qthread.mode = options.mode
        self.qthread.start()


class IntractiveMode(MySQLStatus):
    def run(self):
        logging.debug('starting IntractiveMode')
        self.window = curses.initscr()
        self.window.nodelay(1)
        self.set_window_size()
        curses.nl()
        curses.noecho()
        curses.cbreak()

        try:
            self.mainloop()
        except (KeyboardInterrupt, SystemExit):
            self.cleanup()
        except Exception as err:
            logging.exception(err)
            self.cleanup()
            print(err)
        finally:
            self.cleanup()

    def mainloop(self):
        while True:
            c = self.window.getch()
            if c == ord('q'):
                break
            elif c == ord('p'):
                self.qthread.mode = 'process'
            elif c == ord('s'):
                self.qthread.mode = 'status'
            elif c == ord('g'):
                self.qthread.mode = 'global'
            elif c == ord('h') or c == ord('?'):
                self.show_help()
            elif c == curses.KEY_RESIZE:
                self.set_window_size()
            if self.qthread.update == True:
                self.show_update()
            time.sleep(0.1)

    def set_window_size(self):
        (self.window_max_y, self.window_max_x) = self.window.getmaxyx()

    def show_header(self):
        variables = self.qthread.mysql_variables
        data = {
            'hostname': variables.get('hostname'),
            'currenttime': datetime.now(timezone('Asia/Seoul')).strftime("%Y-%m-%d %H:%M:%S"),
            'mysql_version': variables.get('version'),
            'innodb_buffer': int(variables.get('innodb_buffer_pool_size'))/1024/1024,
        }
        data = "%(hostname)s, %(currenttime)s, %(mysql_version)s, %(innodb_buffer)d MB" % data
        self.window.addstr(0, 0, data)
        self.window.addstr(1, 0, "-" * 70)

    def show_update(self):
        self.qthread.update = False
        self.window.erase()
        self.show_header()
        if self.qthread.mode == 'process':
            self.show_update_process()
        elif self.qthread.mode == 'status':
            self.show_update_status()
        else:
            self.show_update_global()
            

    def show_update_status(self):
        status = self.qthread.mysql_status
        y = 2
        for k in self.keywords:
            data = "%-35s: %12s" % (k, status.get(k))
            if y + 1 < self.window_max_y:
                self.window.addstr(y, 0, data)

            y = y + 1
        if len(self.keywords) + 1 > self.window_max_y:
            omits = len(self.keywords) + 1 - self.window_max_y
            self.window.addstr(self.window_max_y - 1, 0,
                "[%d items were truncated.]" % omits)

    def show_update_process(self):
        """
        Id, Host, db, User, Time, State, Type(Command), Query(Info)
        """
        process = self.qthread.mysql_procesesslist
        y = 2
        header_format = '%-5s, %-8s, %8s, %7s, %6s, %12s,'
        header_item = ('ID', 'HOST', 'DB', 'TIME', 'STATE', 'INFO')
        header = header_format % header_item
        data_format = '%(ID)-5s, %(HOST)-8s, %(DB)8s, %(TIME)7s, %(STATE)6s, %(INFO)12s,'
        self.window.addstr(y, 0, header, curses.A_BOLD)
        y = y + 1
        for item in process:
            data = data_format % item
            # TODO truncate if variables to display is too long.
            if len(data) > self.window_max_x:
                data = data[0:self.window_max_x]

            if y + 1 < self.window_max_y:
                self.window.addstr(y, 0, data)
            else: 
                omits = len(process) + (y - 1) - self.window_max_y
                self.window.addstr(self.window_max_y - 1, 0, "[%d items were truncated.]" %omits)
            y = y + 1

    def show_update_global(self):
        pass

    def cleanup(self):
        self.window.erase()
        curses.nocbreak()
        self.window.keypad(0)
        curses.echo()
        curses.endwin()
        self.qthread.stop = True

        #self.qthread.join(timeout=5)

    def show_help(self):
        """Help:
           s : switch to status mode
           p : switch to process mode
           g : switch to server info mode
           h : show this help message
           ? : alias of help
           q : quit
           [Press any key to continue]"""

        self.window.erase()
        self.window.addstr(1, 0, IntractiveMode.show_help.__doc__)
        self.window.nodelay(0)
        self.window.getch()

        self.window.erase()
        self.window.nodelay(1)


class CliMode(MySQLStatus):
    def run(self):
        logging.debug('starting CliMode')
        self.output = self.options.outfile
        try:
            self.mainloop()
        except (KeyboardInterrupt, SystemExit) as event:
            logging.exception(event)
            self.cleanup()
        except Exception as err:
            logging.exception(err)
            self.cleanup()
            print(err)
        finally:
            self.cleanup()

    def mainloop(self):
        while True:
            if self.qthread.update == True:
                self.output_action()
                time.sleep(0.1)

    def output_action(self):
        self.qthread.update = False
        if self.qthread.mode == 'process':
            self.show_update_process()
        else:
            self.show_update_status()
        self.output.write("\n")

    def show_update_status(self):
        status = self.qthread.mysql_status
        self.output.write(str(status))

    def show_update_process(self):
        process = self.qthread.mysql_procesesslist
        self.output.write(str(process))

    def cleanup(self):
        self.qthread.stop = True
        while self.qthread.is_Alive():
            pass


if __name__ == '__main__':
    parser = get_args_parser()
    options = parser.parse_args()
    if options.help:
        parser.print_help()
        parser.exit()

    if options.debug:
        if not os.path.isdir("logs"):
            os.mkdir("logs")
        logging.basicConfig(
            format='%(asctime)s - (%(threadName)s) - %(message)s in %(funcName)s() at %(filename)s : %(lineno)s',
            level=logging.DEBUG,
            filename="logs/debug.log",
            filemode='w',
        )
        logging.debug(options)
    else:
        nl_hanlder = logging.NullHandler(logging.INFO)
        logging.basicConfig(handlers = [ nl_hanlder ])

    if(options.nonint):
        monitor = CliMode(options)
    else:
        monitor = IntractiveMode(options)
    monitor.run()
