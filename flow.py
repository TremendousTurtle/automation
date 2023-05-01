#!/usr/bin/python
from gpiozero import DigitalInputDevice
from time import perf_counter
from datetime import datetime
from threading import Lock
from os import path
from csv import DictWriter
import paho.mqtt.client as mqtt
from . import config


class FlowMeter:

    def __init__(self, pin=24, sample_duration=1, min_inactive_time=2, mqtt_server='192.168.1.234'):
        # GPIO Pin the flow meter is attached to
        self.FLOW_SENSOR_GPIO = pin
        
        # minimum time in seconds between pulses before we consider the flow stopped
        self.MIN_TIME_INACTIVE = min_inactive_time
        
        # Sample duration in seconds
        self.sample_duration = sample_duration

        # MQTT Server IP
        self.MQTT_SERVER = mqtt_server
        
        # Flow meter calibration factor
        # Sensor given value = 23
        self.F_VALUE = 36.5
        
        # CSV file to write to
        self.csv_path = '/home/chris/'
        self.csv_file = ''
        self.fieldnames = ['startTime', 'endTime', 'duration', 'pulses', 'flow']
        
        

        self.sensor = DigitalInputDevice(self.FLOW_SENSOR_GPIO, pull_up=True)
 
        self.is_flowing = False
        self.flow_start = 0
        self.flow_start_time = datetime.now()
        self.last_pulse = 0
        self.last_pulse_time = datetime.now()
        
        self.idle_publish = 0
        self.last_publish = 0
        
        self.current_count = 0
        self.publish_count = 0
        self.activate_count = 0
        self.deactivate_count = 0
        
        self.mqtt_client = mqtt.Client(clean_session=False)
        self.mqtt_client.username_pw_set(username=config.mqtt_client_username, password=config.mqtt_client_password)
        
        self.lock = Lock()
        self.write_lock = Lock()
        
    
    def writeCsv(self, start:float, end:float, duration:float, pulses:int, flow:float):
        with self.write_lock:
            with open(self.csv_file, 'w', newline='') as csvfile:
                writer = DictWriter(csvfile, fieldnames=self.fieldnames)
                writer.writerow({'startTime': start, 'endTime': end, 'duration': duration, 'pulses': pulses, 'flow': flow})
        
    def reset(self):
        with self.lock:
            if self.is_flowing:
                self.flow_ended(this_start_time=self.flow_start_time, this_flow_start=self.flow_start, this_last_pulse=self.last_pulse, this_last_pulse_time=self.last_pulse_time, this_count=self.current_count)
            
            self.current_count = 0
            self.activate_count = 0
            self.deactivate_count = 0
            self.is_flowing = False
            self.flow_start = 0
            self.flow_start_time = datetime.now()
            self.last_pulse = 0
            self.last_pulse_time = datetime.now()
            self.sensor.when_activated = None
            
    def flow_ended(self, this_start_time:datetime, this_flow_start:float, this_last_pulse:float, this_last_pulse_time:datetime, this_count:int):
        duration = this_last_pulse - this_flow_start
        avg_frequency = this_count / duration
        flow = avg_frequency / self.F_VALUE
        print(f'Flow ended at {this_last_pulse_time.ctime()} -> duration: {round(duration, 2)}s pulses: {this_count} Hz: {round(avg_frequency, 2)} Flow: {round(flow, 2)} L/min Volume: {round(flow * (duration/60), 2)} L')
        self.writeCsv(start=this_start_time.timestamp(), end=this_last_pulse_time.timestamp(), duration=duration, pulses=this_count, flow=flow)
    
    def start(self):
        self.csv_file = path.join(self.csv_path, f'flow_data_{datetime.now().timestamp()}.csv')
        
        with open(self.csv_file, 'w', newline='') as csvfile:
            writer = DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()
                
        self.reset()
        self.mqtt_client.connect(self.MQTT_SERVER)
        self.mqtt_client.loop_start()
        self.sensor.when_activated = self.sensor_activated
        with self.lock:
            self.idle_publish = perf_counter()
    
    def sensor_activated(self):
        with self.lock:
            now = perf_counter()
            now_time = datetime.now()
            self.last_pulse = now
            self.last_pulse_time = now_time
            self.current_count += 1
            
            if not self.is_flowing:
                self.flow_start = now
                self.flow_start_time = now_time
                self.last_publish = now
                self.is_flowing = True
                print(f'Flow started at {self.flow_start_time.ctime()} -> pulses: {self.current_count}')
                
            
    
    def check_flow(self):
        now = perf_counter()
        if self.current_count > 0 and self.is_flowing:
            with self.lock:
                this_last_pulse = self.last_pulse
                this_last_pulse_time = self.last_pulse_time
                this_flow_start = self.flow_start
                this_start_time = self.flow_start_time
                this_count = self.current_count
                this_last_publish = self.last_publish
                publish_duration = now - this_last_publish
                idle_duration = now - this_last_pulse
                
                if publish_duration >= 1:
                    self.last_publish = now
                    this_publish_count = this_count - self.publish_count
                    self.publish_count = this_count
                    
                if idle_duration >= self.MIN_TIME_INACTIVE:
                    self.is_flowing = False
                    self.current_count = 0
                    self.flow_start = 0
                    self.last_pulse = 0
                    self.last_publish = 0
                    self.publish_count = 0
            
            if publish_duration >= 1:
                # publish current flow rate to MQTT
                self.publish_flow(duration=publish_duration, count=this_publish_count)
                
            if idle_duration >= self.MIN_TIME_INACTIVE:
                self.flow_ended(this_start_time=this_start_time, this_flow_start=this_flow_start, this_last_pulse=this_last_pulse, this_last_pulse_time=this_last_pulse_time, this_count=this_count)
                
        elif now - self.idle_publish >= 1:
            with self.lock:
                self.idle_publish = now
            
            self.publish_no_flow()
            
        
    def calculate_flow(self, duration, count) -> float:
        return round(((count / duration) / self.F_VALUE), 2)
    
    def publish_flow(self, duration, count):
        self.mqtt_client.publish('flow/water_cooler', self.calculate_flow(duration, count))
        
    def publish_no_flow(self):
        self.mqtt_client.publish('flow/water_cooler', "0")
    
    def print_activate(self):
        with self.lock:
            self.activate_count += 1
            print(f'Activated: {self.activate_count}')
        
    def print_deactivate(self):
        with self.lock:
            self.deactivate_count += 1
            print(f'Deactivated: {self.deactivate_count}')
    
    def test(self):
        self.sensor.when_activated = self.print_activate
        self.sensor.when_deactivated = self.print_deactivate

if __name__ == '__main__':
    flow_meter = FlowMeter()
    flow_meter.start()
    #flow_meter.test()
    
    while True:
        flow_meter.check_flow()
    