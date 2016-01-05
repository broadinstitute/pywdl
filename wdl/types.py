import wdl.binding

def parse(string):
    ctx = wdl.parser.ParserContext(wdl.parser.lex(string, 'string'), wdl.parser.DefaultSyntaxErrorHandler())
    return wdl.binding.parse_type(wdl.parser.parse_type_e(ctx).ast())

class WdlType: pass

class WdlPrimitiveType(WdlType):
    def __init__(self): pass
    def is_primitive(self): return True
    def __eq__(self, other): return isinstance(other, self.__class__)
    def __str__(self): return repr(self)

class WdlCompoundType(WdlType):
    def is_primitive(self): return False
    def __str__(self): return repr(self)

class WdlBooleanType(WdlPrimitiveType):
    def wdl_string(self): return 'Boolean'
    def __eq__(self, other): return isinstance(other, WdlBooleanType)

class WdlIntegerType(WdlPrimitiveType):
    def wdl_string(self): return 'Int'

class WdlFloatType(WdlPrimitiveType):
    def wdl_string(self): return 'Float'

class WdlStringType(WdlPrimitiveType):
    def wdl_string(self): return 'String'

class WdlFileType(WdlPrimitiveType):
    def wdl_string(self): return 'File'

class WdlArrayType(WdlCompoundType):
    def __init__(self, subtype):
        self.subtype = subtype
    def __eq__(self, other):
        return isinstance(other, WdlArrayType) and other.subtype == self.subtype
    def wdl_string(self): return 'Array[{0}]'.format(self.subtype.wdl_string())

class WdlMapType(WdlCompoundType):
    def __init__(self, key_type, value_type):
        self.__dict__.update(locals())
    def __eq__(self, other):
        return isinstance(other, WdlMapType) and other.key_type == self.key_type and other.value_type == self.value_type
    def wdl_string(self): return 'Map[{0}, {1}]'.format(self.key_type.wdl_string(), self.value_type.wdl_string())

class WdlObjectType(WdlCompoundType):
    def wdl_string(self): return 'Object'
