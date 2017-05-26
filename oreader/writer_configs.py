import csv
import datetime
from sqlalchemy.types import Date, DateTime, Time
from gzip import GzipFile


class SimpleWriterConfig(object):
    pass

class DataSink(object):
    def open(self):
        raise NotImplementedError
    
    def write(self, obj):
        raise NotImplementedError
    
    def close(self):
        raise NotImplementedError

def uncompressed(filename):
    return open(filename, 'wb')

def gzipped(filename):
    return GzipFile(filename + '.gz', 'wb')

class CsvDataSink(object):
    def __init__(self, writer_config, writer, opener=uncompressed):
        self.writer = writer
        self.writer_config = writer_config
        self.file = None
        self.csv_writer = None
        self.opener = opener
    
    def open(self):
        self.file = self.opener(self.writer_config.filename)
        self.csv_writer = csv.writer(self.file, **self.writer_config.csv_config)
        if self.writer_config.header:
            header = [col.name for col in self.writer.klass.columns]
            self.csv_writer.writerow(header)
        return self
            
    def write(self, obj):
        row = self.writer_config.translate(self.writer, obj)
        self.csv_writer.writerow(row)
    
    def close(self):
        self.file.close()

class SqaDataSink(object):
    def __init__(self, writer_config, writer):
        self.writer = writer
        self.writer_config = writer_config
        if self.writer_config.create_table_if_not_exist:
            self.writer_config.table.metadata.create_all(tables=[self.writer_config.table], checkfirst=True)
        
    def open(self):
        return self
            
    def write(self, obj):
        row = self.writer_config.translate(self.writer, obj)
        try:
            self.writer_config.table.insert().execute(row)
        except:
            self.writer_config.table.insert().execute(row)
        
    def close(self):
        pass

class CsvWriterConfig(SimpleWriterConfig):
    def __init__(self, filename, header, csv_config={}, opener=uncompressed):
        self.filename = filename
        self.header = header
        self.csv_config = csv_config
        self.opener = opener
    
    def translate(self, writer, obj):
        if obj is None:
            return None
        return [col.unconvert(getattr(obj, col.name, None)) for col in writer.klass.columns]
    
    def start_sink(self, writer):
        return CsvDataSink(self, writer, opener=self.opener).open()
    
    def stop_sink(self, sink):
        sink.close()

class SqaWriterConfig(SimpleWriterConfig):
    def __init__(self, table, create_table_if_not_exist=False):
        self.table = table
        self.create_table_if_not_exist = create_table_if_not_exist
    
    def translate(self, writer, obj):
        if obj is None:
            return None
        return {col.name: getattr(obj, col.name, None) for col in writer.klass.columns}
    
    def start_sink(self, writer):
        return SqaDataSink(self, writer).open()
    
    def stop_sink(self, sink):
        sink.close()
        