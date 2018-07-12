import csv
import os
from six import string_types, Iterator
from toolz.functoolz import curry, identity
from functools import partial

def sortcsv(input_filename, output_filename, on_cols, input_file_callable=lambda x: open(x, 'r'), 
                output_file_callable=lambda x: open(x, 'w'), input_csv_config={}, 
                output_csv_config={}, conversions=None, 
                input_header=True, output_header=True, tmp_reader_callable=lambda x: open(x, 'r'), 
                tmp_writer_callable=lambda x: open(x, 'w'), tmpdir=None, tmp_size=100000, tmp_csv_config={}):
    input_file = input_file_callable(input_filename)
    reader = csv.reader(input_file, **input_csv_config)
    if input_header:
        header = next(reader)
    else:
        header = None
    
    header_to_idx = dict(zip(header, range(len(header))))
    sort_key_idx = []
    for col in on_cols:
        if isinstance(col, string_types):
            sort_key_idx.append(header_to_idx[col])
        else:
            sort_key_idx.append(col)
    if conversions is None:
        conversions = lambda x: identity
    else:
        conversions = dict(zip(sort_key_idx, conversions)).__getitem__
    key = rowkey(conversions, sort_key_idx)
    
    if tmpdir is None:
        tmpdir = os.path.dirname(os.path.abspath(output_filename))
    if not os.path.exists(tmpdir):
        os.mkdir(tmpdir)
        tmpdir_is_tmp = True
    else:
        tmpdir_is_tmp = False
    tmppaths = splitcsv(reader, tmpdir, tmp_size, tmp_writer_callable, tmp_csv_config)
    
    for tmppath in tmppaths:
        sortpart(tmppath, tmp_reader_callable, tmp_writer_callable, tmp_csv_config, key)
    
    mergeparts(tmppaths, tmp_reader_callable, tmp_csv_config, output_filename, output_file_callable, 
               output_csv_config, key, header if output_header else None)
    
    for tmppath in tmppaths:
        os.remove(tmppath)
    if tmpdir_is_tmp:
        os.removedirs(tmpdir)
    
@curry
def rowkey(conversions, indices, row):
    result = []
    for i in indices:
        try:
            result.append(conversions(i)(row[i]))
        except:
            raise
    return result

def sortpart(path, read_callable, write_callable, csv_config, key):
    infile = read_callable(path)
    reader = csv.reader(infile, **csv_config)
    data = list(reader)
    infile.close()
    data.sort(key=key)
    outfile = write_callable(path)
    writer = csv.writer(outfile, **csv_config)
    writer.writerows(data)
    outfile.close()

def splitcsv(reader, tmpdir, tmp_size, file_callable, csv_config):
    outrownum = tmp_size
    outfilenum = 0
    outfile = None
    result = []
    for row in reader:
        if outrownum >= tmp_size:
            if outfile is not None:
                outfile.close()
                
            outpath = os.path.join(tmpdir, 'tmp_%d.csv'%outfilenum)
            if os.path.exists(outpath):
                raise ValueError('Path %s already exists!' % outpath)
            outfile = file_callable(outpath)
            writer = csv.writer(outfile, **csv_config)
            result.append(outpath)
            outrownum = 0
            outfilenum += 1
        writer.writerow(row)
        outrownum += 1
    try:
        outfile.close()
    except:
        pass
    return result

def next_or_none(it):
    try:
        return next(it)
    except StopIteration:
        return None
    
class MergeIterator(Iterator):
    def __init__(self, readers, key):
        self.readers = readers
        self.key = key
        self.current_rows = list(map(next_or_none, readers))
        self.current_keys = list(map(key, self.current_rows))
        
    def __iter__(self):
        return self
    
    def __next__(self):
        lowest_key = None
        lowest_key_idx = None
        for i in range(len(self.current_keys)):
            current_key = self.current_keys[i]
            if current_key is None:
                continue
            if lowest_key is None or current_key < lowest_key:
                lowest_key = self.current_keys[i]
                lowest_key_idx = i
        if lowest_key_idx is None:
            raise StopIteration()
        result = self.current_rows[lowest_key_idx]
        self.current_rows[lowest_key_idx] = next_or_none(self.readers[lowest_key_idx])
        self.current_keys[lowest_key_idx] = self.key(self.current_rows[lowest_key_idx]) if self.current_rows[lowest_key_idx] is not None else None
        return result

def mergeparts(input_paths, input_callable, input_csv_config, output_path, output_callable, output_csv_config, key, header):
    infiles = list(map(input_callable, input_paths))
    readers = list(map(partial(csv.reader, **input_csv_config), infiles))
    merger = MergeIterator(readers, key)
    if os.path.exists(output_path):
        raise ValueError('Path %s already exists!' % output_path)
    outfile = output_callable(output_path)
    writer = csv.writer(outfile, **output_csv_config)
    if header is not None:
        writer.writerow(header)
    writer.writerows(merger)
        
    
    