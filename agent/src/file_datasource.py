from csv import reader
from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.aggregated_data import AggregatedData
from domain.parking import Parking
import config


class FileDatasource:
    def __init__(self, accelerometer_filename: str,gps_filename: str, parking_filename: str) -> None:
        self.accelerometer_filename = accelerometer_filename;
        self.gps_filename = gps_filename;
        self.parking_filename = parking_filename;
        self.accelerometer_file = None;
        self.gps_file = None;
        self.parking_file = None;

    def read(self) -> AggregatedData:
        #"""Метод повертає дані отримані з датчиків"""
        while True:  # Infinite loop for continuous reading
            try:
                if not (self.accelerometer_file and self.gps_file):
                    raise ValueError("Files are not opened. Call startReading() first.");
        
        
                column_accelerometr = next(reader(self.accelerometer_file));
                accelerometer = Accelerometer(*map(float, column_accelerometr));

                column_gps = next(reader(self.gps_file));
                gps = Gps(*map(float, column_gps));

                column_parking = next(reader(self.parking_file));
                empty_count, latitude, longitude = map(float, column_parking);
                parking_gps = Gps(latitude, longitude);
                parking = Parking(empty_count, parking_gps);
 
                return AggregatedData(
                    accelerometer,
                    gps,
                    parking,
                    datetime.now(),
                    config.USER_ID,
                )
            except StopIteration:
                self.startReading()  # Reopen files when end of file is reached making that infinite loop

    def startReading(self, *args, **kwargs):
        #"""Метод повинен викликатись перед початком читання даних"""
        self.accelerometer_file = open(self.accelerometer_filename, 'r');
        self.gps_file = open(self.gps_filename, 'r');
        self.parking_file = open(self.parking_filename, 'r');

        # Continuing to next column
        next(self.accelerometer_file);
        next(self.gps_file);
        next(self.parking_file);

    def stopReading(self, *args, **kwargs):
        #"""Метод повинен викликатись для закінчення читання даних"""
        if self.accelerometer_file:
            self.accelerometer_file.close()
        if self.gps_file:
            self.gps_file.close()
        if self.parking_file:
            self.parking_file.close()