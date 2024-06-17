#!/usr/bin/env python3

import paho.mqtt.client as mqtt
import time, signal, json, atexit, logging, socket, os, numpy
import traceback
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from enum import Enum
from typing import * 
from multitimer import MultiTimer
from threading import Event
from pyModbusTCP.client import ModbusClient

class POWER(mqtt.Client):
    """Communicates and Uploads Power Meter Content"""

    @dataclass_json
    @dataclass
    class config:
        name: str
        mqtt_prefix: str
        description: str
        ll_name: str
        # long_checkup_freq: int
        mqtt_broker: str
        mqtt_port: int
        mqtt_timeout: int
        reg_config: List[Tuple[str, int]]
        mqtt_max_reconnects: Optional[int] = 10
        mqtt_max_startup: Optional[int] = 10
        mqtt_max_loop_reconnect: Optional[int] = 10
        loglevel: Optional[str] = None

    #overloaded MQTT functions
    def on_log(self, client, userdata, level, buf):
        if level == mqtt.MQTT_LOG_DEBUG:
            logging.debug("PAHO MQTT DEBUG: " + buf)
        elif level == mqtt.MQTT_LOG_INFO:
            logging.info("PAHO MQTT INFO: " + buf)
        elif level == mqtt.MQTT_LOG_NOTICE:
            logging.info("PAHO MQTT NOTICE: " + buf)
        elif level == mqtt.MQTT_LOG_WARNING:
            logging.warn("PAHO MQTT WARN: " + buf)
        else:
            logging.error("PAHO MQTT ERROR: " + buf)

    def on_connect(self, client, userdata, flags, rc):
        logging.info("MQTT Connected: " + str(rc))
        self.subscribe("reporter/checkup_req")

    def on_message(self, client, userdata, message):
        if (message.topic == "reporter/checkup_req"):
            logging.info("Checkup requested.")
            self.checkup()

    def on_disconnect(self, client, userdata, rc):
        logging.warning("MQTT Disconnected: " + str(rc))
        if rc != 0:
            logging.error("Unexpected disconnection.  Attempting reconnection.")
            reconnect_count = 0
            while (reconnect_count < self.config.mqtt_max_reconnects):
                try:
                    reconnect_count += 1
                    self.reconnect()
                    break
                except OSError:
                    logging.error("Connection error while trying to reconnect.")
                    logging.error(traceback.format_exc())
                    self.tEvent.wait(30)
            if (reconnect_count >= self.config.mqtt_max_reconnects):
                logging.critical("Too many reconnect tries.  Exiting.")
                os._exit(1)

    # power functions
    def checkup(self):
        checks = {}
        # copy the data from the input set to the output
        for idx in range(len(self.config.reg_config)):
            if self.config.reg_config[idx][1] & 2:
                checks[self.config.mqtt_prefix + "_" + self.config.reg_config[idx][0]] = self.mbReg[idx]

            if self.config.reg_config[idx][1] & 4:
                data = format(self.mbReg[idx], ".3f")
                self.publish(self.config.ll_name+"/"+self.config.reg_config[idx][0], data)

        self.notify('checkup', checks)

    def notify(self, path, params, retain=False):
        params['time'] = str(time.time())
        logging.debug(params)

        topic = self.config.name + '/' + path
        self.publish(topic, json.dumps(params), retain=retain)
        logging.info("Published " + topic)

    def modbus(self):
        # modbus commands
        def reg_read(start, end) -> List:
            regs = self.mbClient.read_input_registers(2*start, 2*(end - start + 1))

            if regs:
                npa = numpy.array(regs[::-1], numpy.uint16)
                npa.dtype = numpy.float32
                npa = npa[::-1]
                return npa.tolist()
            else:
                return []

        # the time since the last read is kept as the 0th element
        now = time.time()
        if self.mbState != 0:
            self.mbReg[0] = float(now - self.mbTime)
        else:
            self.mbReg[0] = 0.0
        self.mbTime = now

        reg_new = reg_read(0, 43)
        if reg_new != []:
            self.mbReg[1:45] = reg_new
        
        if self.mbState != 1:
            reg_new = reg_read(50, 55)
            if reg_new != []:
                self.mbReg[51:57] = reg_new
        
        reg_new = reg_read(100, 134)
        if reg_new != []:
            self.mbReg[101:136] = reg_new
        
        if self.mbState <= 1:
            reg_new = reg_read(167, 197)
            if reg_new != []:
                self.mbReg[168:199] = reg_new

        if (self.mbState > 1):
            self.mbState = 1
        else:
            self.mbState += 1

        runData = {}
        for idx in range(len(self.config.reg_config)):
            if self.config.reg_config[idx][1] & 1:
                runData[self.config.mqtt_prefix + "_" + self.config.reg_config[idx][0]] = self.mbReg[idx]

        self.notify('run', runData);

    def signal_handler(self, signum, frame):
        logging.warning("Caught a deadly signal: " + str(signum) + "!")
        if self.modbusPolling:
            self.modbusPolling.stop()
        self.running = False

    # run function, main function?
    def run(self):
        self.tEvent = Event()
        self.running = True
        startup_count = 0
        self.modbus_check_count = 0
        self.loop_count = 0
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        try:
            if type(logging.getLevelName(self.config.loglevel.upper())) is int:
                logging.basicConfig(level=self.config.loglevel.upper())
            else:
                logging.warning("Log level not configured.  Defaulting to WARNING.")
        except (KeyError, AttributeError) as e:
            logging.warning("Log level not configured.  Defaulting to WARNING.  Caught: " + str(e))
        if self.config.mqtt_max_loop_reconnect < 1:
            logging.error("MQTT maximum reconnect tries can not be less than 1.")
            exit(1)
        if self.config.mqtt_max_startup < 1:
            logging.error("MQTT maximum startup count can not be less than 1.")
            exit(1)
        if self.config.mqtt_max_reconnects < 1:
            logging.error("MQTT maximum reconnect count can not be less than 1.")
            exit(1)

        while startup_count < self.config.mqtt_max_startup:
            try:
                startup_count += 1
                self.connect(self.config.mqtt_broker, self.config.mqtt_port,  self.config.mqtt_timeout)
                atexit.register(self.disconnect)
                self.mbClient = ModbusClient(host="localhost", port=502, unit_id=1, auto_open=True)
                self.mbTime = time.time()
                self.mbReg = [0] * 198
                self.mbState = 0
                self.modbusPolling = MultiTimer(interval = 1, function=self.modbus)
                self.modbusPolling.start()
                atexit.register(self.modbusPolling.stop)
                break
            except OSError:
                logging.error("Error connecting on bootup.")
                logging.error(traceback.format_exc())
                self.tEvent.wait(30)

        if startup_count >= self.config.mqtt_max_startup:
            logging.critical("Too many startup tries.  Exiting.")
            exit(1)

        logging.info("Startup success.")
        reconnect_flag = False
        loop_reconnect_count = 0;
        while self.running and (loop_reconnect_count < self.config.mqtt_max_loop_reconnect):
            if self.loop_count >= 65535:
                self.loop_count = 0
            else:
                self.loop_count += 1
            try:
                if reconnect_flag == True:
                    self.reconnect()
                    self.reconnect_flag = False

                self.loop()
                # reset the reconnect counter if our last was successful
                loop_reconnect_count = 0
            except SystemExit:
                # exiting.  stop the loop.
                break
            except (socket.timeout, TimeoutError, ConnectionError):
                loop_reconnect_count += 1
                reconnect_flag = True
                logging.error("MQTT loop error.  Attempting to reconnect. " + loop_reconnect_count + "of " + self.config.mqtt_max_loop_reconnect)
            except:
                logging.critical("Exception in MQTT loop.")
                logging.critical(traceback.format_exc())
                logging.critical("Exiting.")
                exit(2)
        if loop_reconnect_count >= self.config.mqtt_max_loop_reconnect:
            logging.critical("Too many loop reconnects.  Exiting")
            exit(1)
        logging.info("Successfully reached exit.")
        exit(0)

# code that is run on direct invocation of this file
if __name__ == "__main__":
    power = POWER()
    file_path = os.path.dirname(os.path.abspath(__file__))
    with open(file_path + "/power_config.json", "r") as configFile:
        power.config = POWER.config.from_json(configFile.read())
    power.run()
