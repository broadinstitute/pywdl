import pytest

from wdl.binding import coerce
from wdl.types import parse as parse_type
from wdl.values import *

valid_string_values = [
  ("foobar", WdlString("foobar")),
  (WdlString("foobar"), WdlString("foobar")),
  (1, WdlString("1")),
  (WdlInteger(1), WdlString("1")),
  (WdlFloat(2.3), WdlString("2.3"))
]

valid_file_values = [
  ("/foobar", WdlFile("/foobar"))
]

bad_string_values = [
  (True),
  (WdlBoolean(False))
]

valid_int_values = [
  (WdlInteger(1), WdlInteger(1)),
  ("1", WdlInteger(1)),
  (1, WdlInteger(1))
]

valid_float_values = [
  (WdlFloat(1.2), WdlFloat(1.2)),
  ("1.2", WdlFloat(1.2)),
  (1.2, WdlFloat(1.2))
]

valid_boolean_values = [
  (WdlBoolean(True), WdlBoolean(True)),
  (WdlBoolean(False), WdlBoolean(False)),
  (True, WdlBoolean(True)),
  (False, WdlBoolean(False))
]

int_array = WdlArray(parse_type("Int"), [WdlInteger(x) for x in [1, 2, 3]])
float_array = WdlArray(parse_type("Float"), [WdlFloat(x) for x in [1.0, 2.00001, 3.1415]])
str_array = WdlArray(parse_type("String"), [WdlString(x) for x in ['a', 'b', 'c']])

valid_array_values = [
  (int_array, parse_type("Int"), int_array),
  ([1, 2, 3], parse_type("Int"), int_array),
  ([1, 2.00001, 3.1415], parse_type("Float"), float_array),
  (float_array, parse_type("Float"), float_array),
  (['a', 'b', 'c'], parse_type("String"), str_array),
  (str_array, parse_type("String"), str_array)
]

@pytest.mark.parametrize("uncoerced,coerced", valid_string_values)
def test_coerce_string(uncoerced, coerced):
    assert WdlString.coerce(uncoerced) == coerced

@pytest.mark.parametrize("uncoerced,coerced", valid_file_values)
def test_coerce_file(uncoerced, coerced):
    assert WdlFile.coerce(uncoerced) == coerced

@pytest.mark.parametrize("uncoerced", bad_string_values)
def test_coerce_bad_string(uncoerced):
    with pytest.raises(CoercionException):
        assert WdlString.coerce(uncoerced)

@pytest.mark.parametrize("uncoerced,coerced", valid_int_values)
def test_coerce_int(uncoerced, coerced):
    assert WdlInteger.coerce(uncoerced) == coerced

@pytest.mark.parametrize("uncoerced,coerced", valid_float_values)
def test_coerce_float(uncoerced, coerced):
    assert WdlFloat.coerce(uncoerced) == coerced

@pytest.mark.parametrize("uncoerced,coerced", valid_boolean_values)
def test_coerce_boolean(uncoerced, coerced):
    assert WdlBoolean.coerce(uncoerced) == coerced

@pytest.mark.parametrize("uncoerced,coerce_to_type,coerced", valid_array_values)
def test_coerce_array(uncoerced, coerce_to_type, coerced):
    assert WdlArray.coerce(uncoerced, coerce_to_type) == coerced
