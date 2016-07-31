import csv
import datetime
from sqlalchemy.types import Date, DateTime, Time


class SimpleWriterConfig(object):
    pass

class DataSink(object):
    def open(self):
        raise NotImplementedError
    
    def write(self, obj):
        raise NotImplementedError
    
    def close(self):
        raise NotImplementedError
    
class CsvDataSink(object):
    def __init__(self, writer_config, writer):
        self.writer = writer
        self.writer_config = writer_config
        self.file = None
        self.csv_writer = None
    
    def open(self):
        self.file = open(self.writer_config.filename)
        self.csv_writer = csv.writer(self.file, **self.writer_config.csv_config)
        if self.writer_config.header:
            header = [col.name for col in self.writer.klass.columns]
            self.csv_writer.writerow(header)
            
    def write(self, obj):
        row = self.writer_config.translate(self.writer, obj)
        self.csv_writer.writerow(row)
    
    def close(self):
        self.file.close()

class SqaDataSink(object):
    def __init__(self, writer_config, writer):
        self.writer = writer
        self.writer_config = writer_config
        self.file = None
        self.csv_writer = None
        
    def open(self):
        pass
            
    def write(self, obj):
        row = self.writer_config.translate(self.writer, obj)
        try:
            self.writer_config.table.insert().execute(row)
        except:
            self.writer_config.table.insert().execute(row)
        
    def close(self):
        pass

class CsvWriterConfig(SimpleWriterConfig):
    def __init__(self, filename, header, csv_config={}):
        self.filename = filename
        self.header = header
        self.csv_config = csv_config
    
    def translate(self, writer, obj):
        if obj is None:
            return None
        return [col.unconvert(getattr(obj, col.name, None)) for col in writer.klass.columns]
    
    def start_sink(self, writer):
        return CsvDataSink(self, writer)
    
    def stop_sink(self, sink):
        sink.close()

class SqaWriterConfig(SimpleWriterConfig):
    def __init__(self, table):
        self.table = table
    
    def translate(self, writer, obj):
        if obj is None:
            return None
        return {col.name: getattr(obj, col.name, None) for col in writer.klass.columns}
    
    def start_sink(self, writer):
        return SqaDataSink(self, writer)
    
    def stop_sink(self, sink):
        sink.close()
        