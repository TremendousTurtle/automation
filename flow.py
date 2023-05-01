#!/usr/bin/python
from gpiozero import DigitalInputDevice
from time import perf_counter
from datetime import datetime
from threading import Lock
from os import path
from csv import DictWriter
import sys
import paho.mqtt.client as mqtt
import config


class FlowMeter:

    def __init__(self, pin=24, sample_duration=1, min_inactive_time=2, mqtt_server='192.168.1.234'):
        # GPIO Pin the flow meter is attached to
        self.FLOW_SENSOR_GPIO = pin
        
        # minimum time in seconds between pulses before we consider the flow stopped
        self.MIN_TIME_INACTIVE = min_inactive_time
        
        # Sample duration in seconds
        self.SAMPLE_DURATION = sample_duration

        # MQTT Server IP
        self.MQTT_SERVER = mqtt_server
        
        # Flow meter calibration factor
        # Sensor given value = 23
        self.F_VALUE = 36.5
        
        # Seconds to wait between publishing flow data during an active/inactive flow
        self.ACTIVE_FLOW_PUBLISH_PERIOD = 1
        self.INACTIVE_FLOW_PUBLISH_PERIOD = 5
        
        # MQTT Topics
        self.VOLUME_TOPIC = 'flow/water_cooler_volume'
        self.FLOW_TOPIC = 'flow/water_cooler_flow'
        
        # CSV file to write to
        self.csv_path = '.'
        self.csv_file = ''
        self.fieldnames = ['startTime', 'endTime', 'duration', 'pulses', 'flow', 'volume']
        
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
        
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.username_pw_set(username=config.mqtt_client_username, password=config.mqtt_client_password)
        
        self.lock = Lock()
        self.write_lock = Lock()
        
    
    def writeCsv(self, start:float, end:float, duration:float, pulses:int, flow:float, volume:float):
        # Write data to CSV file
        with self.write_lock:
            with open(self.csv_file, 'w', newline='') as csvfile:
                writer = DictWriter(csvfile, fieldnames=self.fieldnames)
                writer.writerow({'startTime': start, 'endTime': end, 'duration': duration, 'pulses': pulses, 'flow': flow, 'volume': volume})
        
        self.print_flush(f'Data written to CSV: {self.csv_file}')
        
    def reset(self):
        # Reset the flow meter internal values for startup or reset
        with self.lock:
            # Make sure to end any active flow
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
        
        self.print_flush('Flow meter reset')
            
    def flow_ended(self, this_start_time:datetime, this_flow_start:float, this_last_pulse:float, this_last_pulse_time:datetime, this_count:int):
        # Function called when end of flow is detected
        # Calculate flow duration in seconds
        duration = this_last_pulse - this_flow_start
        
        # Calculate average frequency in Hz over flow duration
        avg_frequency = this_count / duration
        flow = self.calculate_flow(duration=duration, count=this_count)
        
        # Calculate volume from flow rate and duration
        volume = flow * (duration/60)
        
        self.publish_volume(volume=volume)
        self.writeCsv(start=this_start_time.timestamp(), end=this_last_pulse_time.timestamp(), duration=duration, pulses=this_count, flow=flow, volume=volume)
        self.print_flush(f'Flow ended at {this_last_pulse_time.ctime()} -> duration: {round(duration, 2)}s pulses: {this_count} Hz: {round(avg_frequency, 2)} Flow: {round(flow, 2)} gal/min Volume: {round(volume, 2)} gal')
    
    def start(self):
        # Called to start the monitoring of the flow meter
        # Generate the CSV file using current timestamp
        self.csv_file = path.join(self.csv_path, f'flow_data_{datetime.now().timestamp()}.csv')
        with open(self.csv_file, 'w', newline='') as csvfile:
            writer = DictWriter(csvfile, fieldnames=self.fieldnames)
            writer.writeheader()
        self.print_flush(f'CSV file created: {self.csv_file}')
        
        # Ensure values are properly set for startup
        self.reset()
        
        # Connect to MQTT Broker and start background MQTT loop
        self.mqtt_client.connect(host=self.MQTT_SERVER)
        self.mqtt_client.loop_start()
        self.print_flush(f'Connected to MQTT server: {self.MQTT_SERVER}')
        
        # Activate the sensor callback function
        self.sensor.when_activated = self.sensor_activated
        
        # Set initial idle publish time
        with self.lock:
            self.idle_publish = perf_counter()
        self.print_flush('Flow meter started')
    
    def sensor_activated(self):
        # Function called when a pulse is detected by the flow meter
        # Grab time data and increment pulse count
        with self.lock:
            now = perf_counter()
            now_time = datetime.now()
            self.last_pulse = now
            self.last_pulse_time = now_time
            self.current_count += 1
            
            # If a flow was not already active then activate the flow and set start time and last publish time
            if not self.is_flowing:
                self.flow_start = now
                self.flow_start_time = now_time
                self.last_publish = now
                self.is_flowing = True
                self.print_flush(f'Flow started at {self.flow_start_time.ctime()} -> pulses: {self.current_count}')
                
    
    def check_flow(self):
        # Function called in loop to perform neccessary actions at correct times
        
        # Get current "time"
        now = perf_counter()
        
        # If flow is active (is_flowing == True and count is at least 1) then get current values
        # Publish flow rate every ACTIVE_FLOW_PUBLISH_PERIOD second(s) during active flow
        # If flow is inactive for MIN_TIME_INACTIVE then end the flow and publish the volume
        if self.current_count > 0 and self.is_flowing:
            # Grab and set data with lock
            with self.lock:
                this_last_pulse = self.last_pulse
                this_last_pulse_time = self.last_pulse_time
                this_flow_start = self.flow_start
                this_start_time = self.flow_start_time
                this_count = self.current_count
                this_last_publish = self.last_publish
                publish_duration = now - this_last_publish
                idle_duration = now - this_last_pulse
                
                # If it is time to publish then set publish time and count appropriately
                if publish_duration >= self.ACTIVE_FLOW_PUBLISH_PERIOD:
                    self.last_publish = now
                    # Calculate the number of pulses since last publish
                    this_publish_count = this_count - self.publish_count
                    # Save the current count for start of next publish period
                    self.publish_count = this_count
                    
                # If flow is inactive then reset values to wait for next flow
                if idle_duration >= self.MIN_TIME_INACTIVE:
                    self.is_flowing = False
                    self.current_count = 0
                    self.flow_start = 0
                    self.last_pulse = 0
                    self.last_publish = 0
                    self.publish_count = 0
            
            # Check and publish grabbed data without lock
            if publish_duration >= self.ACTIVE_FLOW_PUBLISH_PERIOD:
                # publish current flow rate to MQTT
                self.publish_flow(duration=publish_duration, count=this_publish_count)
                
            # If flow is inactive then end the flow
            if idle_duration >= self.MIN_TIME_INACTIVE:
                self.flow_ended(this_start_time=this_start_time, this_flow_start=this_flow_start, this_last_pulse=this_last_pulse, this_last_pulse_time=this_last_pulse_time, this_count=this_count)
        
        # If flow is inactive then publish 0 flow rate every INACTIVE_FLOW_PUBLISH_PERIOD second(s)        
        elif now - self.idle_publish >= self.INACTIVE_FLOW_PUBLISH_PERIOD:
            with self.lock:
                self.idle_publish = now
            
            self.publish_flow()
            
        
    def calculate_flow(self, duration, count) -> float:
        # Calculate flow rate in L/min then convert to gal/min (divide by 3.78541)
        return ((count / duration) / self.F_VALUE) / 3.78541
    
    def publish_flow(self, duration=0, count=0):
        # If count is 0 (or not provided) then skip calculation and publish 0
        if count == 0:
            flow = 0
        else:
            flow = self.calculate_flow(duration, count)
        self.mqtt_client.publish(topic=self.FLOW_TOPIC, payload=flow, qos=2)
        self.print_flush(f'Flow published to "{self.FLOW_TOPIC}": {flow}')
        
    def publish_volume(self, volume):
        self.mqtt_client.publish(topic=self.VOLUME_TOPIC, payload=volume, qos=2)
        self.print_flush(f'Volume published to "{self.VOLUME_TOPIC}": {volume}')
    
    def print_activate(self):
        # Debugging function to print when the sensor is activated
        with self.lock:
            self.activate_count += 1
            print(f'Activated: {self.activate_count}')
            sys.stdout.flush()
        
    def print_deactivate(self):
        # Debugging function to print when the sensor is deactivated
        with self.lock:
            self.deactivate_count += 1
            self.print_flush(f'Deactivated: {self.deactivate_count}')
            
    def print_flush(self, message):
        # Simple wraapper to print then flush stdout for systemd journal
        print(message)
        sys.stdout.flush()
    
    def test(self):
        # Debugging function to test sensor activation/deactivation
        self.sensor.when_activated = self.print_activate
        self.sensor.when_deactivated = self.print_deactivate

if __name__ == '__main__':
    flow_meter = FlowMeter()
    flow_meter.start()
    #flow_meter.test()
    
    # Loop forever checking flow and publishing when needed
    while True:
        flow_meter.check_flow()
    