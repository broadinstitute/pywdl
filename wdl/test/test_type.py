import wdl.types

def test_from_wdl_string():
    wdl_int = wdl.types.parse("Int")
    wdl_string = wdl.types.parse("String")
    wdl_float = wdl.types.parse("Float")
    wdl_bool = wdl.types.parse("Boolean")
    wdl_file = wdl.types.parse("File")
    wdl_arr_int = wdl.types.parse("Array[Int]")
    wdl_map_int_string = wdl.types.parse("Map[Int, String]")

    assert wdl_int == wdl.types.WdlIntegerType()
    assert wdl_string == wdl.types.WdlStringType()
    assert wdl_float == wdl.types.WdlFloatType()
    assert wdl_bool == wdl.types.WdlBooleanType()
    assert wdl_file == wdl.types.WdlFileType()
    assert wdl_arr_int == wdl.types.WdlArrayType(wdl.types.WdlIntegerType())
    assert wdl_map_int_string == wdl.types.WdlMapType(wdl.types.WdlIntegerType(), wdl.types.WdlStringType())
