#!/usr/bin/env python

import argparse
import signal
import sqlite3
from aurora_sqlite import AuroraSQLite as ccd
import sys
import logging as log
import daemon

# This holds our Aurora to SQLite class
myWorker = None


def argProcessing():
    '''Processes command line arguments'''
    parser = argparse.ArgumentParser(
        description="Aurora / BenQ CCD solar micro-inverter base-station to SQLite",
    )
    parser.add_argument("--host", "-H",
        help="The IP/hostname of the CCD",
    )
    parser.add_argument("-v",
        help="Increase output verbosity",
        action="count",
        default=0,
    )
    parser.add_argument("--logfile", "-l",
        help="Log to the specified file rather than STDERR",
        default=None,
        type=str,
    )
    parser.add_argument("--daemon",
        help="Fork and run in background",
        default=False,
        action="store_true",
    )
    parser.add_argument("--pidfile", "-p",
        help="PID file when run with --daemon (ignored otherwise)",
        default="/var/run/aurora_script.pid",
    )
    parser.add_argument("--database", "-f",
        help="SQLite database file to write to",
    )
    parser.add_argument("--init-database",
        help="Initialise the database, and then quit (ignores most options)",
        default=False,
        action="store_true",
    )
    return parser.parse_args()


def exit_safely(signum, frame):
    '''SIGINT (Ctrl+C) handler'''
    global myWorker
    if myWorker is not None:
        myWorker.close()
    log.info("Ctrl+C pressed. Exiting.")
    exit(0)


def main():
    # Process cmd line arguments
    programArgs = argProcessing()

    # Setup logging
    if programArgs.v > 5:
        verbosityLevel = 5
    else:
        verbosityLevel = programArgs.v
    verbosityLevel = (5 - verbosityLevel)*10
    if programArgs.logfile is not None:
        log.basicConfig(
            format='%(asctime)s %(message)s',
            filename=programArgs.logfile,
            level=verbosityLevel
        )
    else:
        log.basicConfig(
            format='%(asctime)s %(message)s',
            level=verbosityLevel
        )

    if programArgs.database is None:
        log.fatal("No database file specified")
        sys.exit(1)

    if programArgs.init_database:
        log.warning("Initialising the database")
        database = sqlite3.connect(programArgs.database)
        cursor = database.cursor()
        cursor.execute('''
            CREATE TABLE system (
                timestamp INTEGER PRIMARY KEY,
                pout_W REAL,
                etot_Wh REAL,
                status TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE panels (
                timestamp INTEGER NOT NULL,
                macrf TEXT NOT NULL,
                fwmicro TEXT,
                p1_ids TEXT,
                p1_idi TEXT,
                fwdsp TEXT,
                fwrf TEXT,
                Etot_Wh REAL,
                Vout_V REAL,
                Pout_W REAL,
                Freq_Hz REAL,
                Vin_V REAL,
                Tdsp_degC REAL,
                Tmos_degC REAL,
                invstat TEXT,
                booststat TEXT,
                alarmstat TEXT,
                rssi INTEGER,
                rssicnt INTEGER,
                rssiavg INTEGER,
                msgcnt INTEGER,
                samples INTEGER,
                dspalarm TEXT,
                mcualarm TEXT,
                mcuwarning TEXT, 
                PRIMARY KEY (timestamp, macrf)
            )
        ''')
        database.commit()
        cursor.close()
        database.close()
        sys.exit(127)

    # Should we be daemonising?
    if programArgs.daemon:
        dMon = daemon.DaemonContext()
        dMon.pidfile = programArgs.pidfile
        dMon.signal_map = {
            signal.SIGTERM: exit_safely,
        }

    # Initialise the class
    global myWorker
    myWorker = ccd(programArgs.host, programArgs.database)
    if not myWorker.open():
        log.critical(
            "Couldn't access resources needed. Check logs for more"
            "information."
        )
    else:
        # Register exit handler (only if in fg)
        if programArgs.daemon:
            with dMon:
                myWorker.run()
        else:
            signal.signal(signal.SIGINT, exit_safely)
            myWorker.run()

if __name__ == '__main__':
    main()
