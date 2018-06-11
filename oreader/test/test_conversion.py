from oreader.base import DateTimeColumn
from datetime import datetime
from nose.tools import assert_equal
from dateutil.tz.tz import tzutc

def test_datetime_conversion_distant_past():
    # 
    col = DateTimeColumn(name='datetime')
    d1 = '2018-06-11 10:02:10'
    d2 = '1018-06-11 10:02:10'
    d3 = '0001-06-11 10:02:10'
    d4 = '0000-06-11 10:02:10'
    assert_equal(col.convert(d1), datetime(2018, 6, 11, 10, 2, 10, tzinfo=tzutc()))
    assert_equal(col.convert(d2), datetime(1018, 6, 11, 10, 2, 10, tzinfo=tzutc()))
    assert_equal(col.convert(d3), datetime(1, 6, 11, 10, 2, 10, tzinfo=tzutc()))
    assert_equal(col.convert(d4), None)

if __name__ == '__main__':
    # This code will run the test in this file.'
    import sys
    import nose
    module_name = sys.modules[__name__].__file__

    result = nose.run(argv=[sys.argv[0],
                            module_name,
                            '-s','-v'])
