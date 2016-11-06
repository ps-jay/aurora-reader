import sqlite3
import time
import threading
import urllib2
import logging as log
import xml.etree.ElementTree as ET
 
class AuroraSQLite:

    def __init__(self, cdd_host, db_file):
        self._SECONDS = 60
        self._timer_expired = threading.Event()
        self._timer = None

        self.host = cdd_host
        self.database_file = db_file
        self._URL = "http://%s/plant.xml" % self.host
        self.database = None
        self.cursor = None

    def __del__(self):
        '''This will close all connections'''
        self.close()

    def _openSQLite(self):
        '''This function will open a connection with the SQLite database'''
        self.database = sqlite3.connect(self.database_file)
        self.cursor = self.database.cursor()
        return True

    def _closeSQLite(self):
        '''This function will close the SQLite connection'''
        if self.database is not None:
            self.database.commit()
            self.cursor.close()
            self.database.close()
            log.info("SQLite connection was closed.")
        else:
            log.debug(
                "Asking to close SQLite connection,"
                "but it was never open."
            )

    def open(self):
        if not self._openSQLite():
            log.critical(
                "SQLite connection was not opened due to an error."
            )
            return False
        else:
            return True

    def close(self):
        '''This function will close all previously opened connections & cancel
        timers'''
        self._timer.cancel()
        if self.database is not None:
            self._closeSQLite()

    def _isReady(self):
        '''This function is used to check if this object has been initialised
        correctly and is ready to process data'''
        return (self.database is not None)

    def _process_cdd(self, data):
        try:
            log.info("Power (instant): %.1fW; Energy (total) %.0fWh; Status: %s" % (
                float(data['pout_W']),
                float(data['etot_Wh']),
                data['status'],
            ))
            self.cursor.execute('''
                INSERT INTO system
                VALUES (%d, %f, %f, "%s")
            ''' % (
                time.time(),
                float(data['pout_W']),
                float(data['etot_Wh']),
                data['status'],
            ))
            log.debug("Inserted system values into database")
            self.database.commit()
        except Exception as e:
            log.error("Exception handling database write for system info: %s" % str(e))

    def _process_panel(self, data):
        try:
            self.cursor.execute('''
                INSERT INTO panels
                VALUES (
                    %d,
                    "%s", "%s", "%s", "%s", "%s", "%s",
                    %f, %f, %f, %f, %f, %f, %f,
                    "%s", "%s", "%s",
                    %d, %d, %d, %d, %d,
                    "%s", "%s", "%s"
                )
            ''' % (
                time.mktime(time.strptime(data['ts'], "%d-%m-%y %H:%M:%S")),
                data['macrf'],
                data['fwmicro'],
                data['p1_ids'],
                data['p1_idi'],
                data['fwdsp'],
                data['fwrf'],
                float(data['Etot_Wh']),
                float(data['Vout_V']),
                float(data['Pout_W']),
                float(data['Freq_Hz']),
                float(data['Vin_V']),
                float(data['Tdsp_degC']),
                float(data['Tmos_degC']),
                data['invstat'],
                data['booststat'],
                data['alarmstat'],
                int(data['rssi']),
                int(data['rssicnt']),
                int(data['rssiavg']),
                int(data['msgcnt']),
                int(data['samples']),
                data['dspalarm'],
                data['mcualarm'],
                data['mcuwarning'],
            ))
            log.debug("Panel %s - Power: %.2fW; Energy: %.0fWh; "
                "Voltage In: %.1fVDC; Voltage Out: %.0fVAC; "
                "DSP Temp: %.0fC; MOSFET Temp: %.0fC" % (
                    data['macrf'],
                    float(data['Pout_W']),
                    float(data['Etot_Wh']),
                    float(data['Vin_V']),
                    float(data['Vout_V']),
                    float(data['Tdsp_degC']),
                    float(data['Tmos_degC']),
            ))
            log.debug("Inserted system values into database")
            self.database.commit()
        except sqlite3.IntegrityError:
            log.debug("Entry for panel %s already exists at time %s" % (
                data['macrf'],
                data['ts'],
            ))
            self.database.commit()
        except Exception as e:
            log.error("Exception handling database write for panel info: %s" % str(e))

    def run(self):
        if not self._isReady():
            log.error(
                "Was asked to begin reading/writing data without opening"
                "connections."
            )
            return False

        self._timer_expired.set()

        while True:
            if self._timer_expired.is_set():

                # Handle timing
                self._timer_expired.clear()
                self._timer = threading.Timer(
                    self._SECONDS,
                    self._timer_expired.set
                )
                self._timer.start()

                try:
                    try:
                        # 'http://solar.pine.vumnoo.org/plant.xml',
                        fh = urllib2.urlopen(
                            'http://10.10.1.13/plant.xml',
                            None,
                            self._SECONDS * 0.75
                        )
                        xml_string = fh.read()
                    except Exception as e:
                        log.warning("Exception on url open: %s" % str(e))
                        continue
                finally:
                    fh.close() 

                try:
                    xml = ET.fromstring(xml_string)
                    for cdd in xml:
                        if cdd.tag == 'cdd':
                            self._process_cdd(cdd.attrib)

                            for panel in cdd:
                                if panel.tag == 'edd':
                                    self._process_panel(panel.attrib)
                except Exception as e:
                    log.error("Error processing XML: %s" % str(e))

            # Have a rest, and wait for the timer to expire
            time.sleep(1)

        else:
            log.error(
                "Was asked to begin reading/writing data without opening"
                "connections."
            )
