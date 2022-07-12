from sqlglot import exp
from sqlglot.dialects.dialect import (
    Dialect,
    format_time_lambda,
    rename_func,
    if_sql,
    no_ilike_sql,
    struct_extract_sql,
    no_tablesample_sql,
)
from sqlglot.dialects.mysql import MySQL
from sqlglot.generator import Generator
from sqlglot.helper import csv, list_get
from sqlglot.parser import Parser


def _approx_distinct_sql(self, expression):
    accuracy = expression.args.get("accuracy")
    accuracy = ", " + self.sql(accuracy) if accuracy else ""
    return f"APPROX_DISTINCT({self.sql(expression, 'this')}{accuracy})"


def _concat_ws_sql(self, expression):
    sep, *args = expression.args["expressions"]
    sep = self.sql(sep)
    if len(args) > 1:
        return f"ARRAY_JOIN(ARRAY[{csv(*(self.sql(e) for e in args))}], {sep})"
    return f"ARRAY_JOIN({self.sql(args[0])}, {sep})"


def _datatype_sql(self, expression):
    sql = self.datatype_sql(expression)
    if expression.this == exp.DataType.Type.TIMESTAMPTZ:
        sql = f"{sql} WITH TIME ZONE"
    return sql


def _date_parse_sql(self, expression):
    return f"DATE_PARSE({self.sql(expression, 'this')}, '%Y-%m-%d %H:%i:%s')"


def _explode_to_unnest_sql(self, expression):
    if isinstance(expression.this, (exp.Explode, exp.Posexplode)):
        return self.sql(
            exp.Join(
                this=exp.Unnest(
                    expressions=[expression.this.this],
                    table=expression.args.get("table"),
                    columns=expression.args.get("columns"),
                    ordinality=isinstance(expression.this, exp.Posexplode),
                ),
                kind="cross",
            )
        )
    return self.lateral_sql(expression)


def _initcap_sql(self, expression):
    regex = r"(\w)(\w*)"
    return f"REGEXP_REPLACE({self.sql(expression, 'this')}, '{regex}', x -> UPPER(x[1]) || LOWER(x[2]))"


def _no_sort_array(self, expression):
    if expression.args.get("asc") == exp.FALSE:
        comparator = "(a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END"
    else:
        comparator = None
    args = csv(self.sql(expression, "this"), comparator)
    return f"ARRAY_SORT({args})"


def _schema_sql(self, expression):
    if isinstance(expression.parent, exp.Property):
        columns = ", ".join(
            f"'{c.text('this')}'" for c in expression.args["expressions"]
        )
        return f"ARRAY[{columns}]"

    for schema in expression.parent.find_all(exp.Schema):
        if isinstance(schema.parent, exp.Property):
            expression = expression.copy()
            expression.args["expressions"].extend(schema.args["expressions"])

    return self.schema_sql(expression)


def _quantile_sql(self, expression):
    self.unsupported("Presto does not support exact quantiles")
    return f"APPROX_PERCENTILE({self.sql(expression, 'this')}, {self.sql(expression, 'quantile')})"


def _str_position_sql(self, expression):
    this = self.sql(expression, "this")
    substr = self.sql(expression, "substr")
    position = self.sql(expression, "position")
    if position:
        return f"STRPOS(SUBSTR({this}, {position}), {substr}) + {position} - 1"
    return f"STRPOS({this}, {substr})"


def _ts_or_ds_to_date_str_sql(self, expression):
    this = self.sql(expression, "this")
    return f"DATE_FORMAT(DATE_PARSE(SUBSTR(CAST({this} AS VARCHAR), 1, 10), {Presto.date_format}), {Presto.date_format})"


def _ts_or_ds_to_date_sql(self, expression):
    this = self.sql(expression, "this")
    return f"CAST(DATE_PARSE(SUBSTR(CAST({this} AS VARCHAR), 1, 10), {Presto.date_format}) AS DATE)"


def _ts_or_ds_add_sql(self, expression):
    this = self.sql(expression, "this")
    e = self.sql(expression, "expression")
    unit = self.sql(expression, "unit") or "'day'"
    return f"DATE_FORMAT(DATE_ADD({unit}, {e}, DATE_PARSE(SUBSTR({this}, 1, 10), {Presto.date_format})), {Presto.date_format})"


class Presto(Dialect):
    index_offset = 1
    time_format = "'%Y-%m-%d %H:%i:%S'"
    time_mapping = MySQL.time_mapping

    class Parser(Parser):
        FUNCTIONS = {
            **Parser.FUNCTIONS,
            "APPROX_DISTINCT": exp.ApproxDistinct.from_arg_list,
            "CARDINALITY": exp.ArraySize.from_arg_list,
            "CONTAINS": exp.ArrayContains.from_arg_list,
            "DATE_ADD": lambda args: exp.DateAdd(
                this=list_get(args, 2),
                expression=list_get(args, 1),
                unit=list_get(args, 0),
            ),
            "DATE_DIFF": lambda args: exp.DateDiff(
                this=list_get(args, 2),
                expression=list_get(args, 1),
                unit=list_get(args, 0),
            ),
            "DATE_FORMAT": format_time_lambda(exp.TimeToStr, "presto"),
            "DATE_PARSE": format_time_lambda(exp.StrToTime, "presto"),
            "FROM_UNIXTIME": exp.UnixToTime.from_arg_list,
            "STRPOS": exp.StrPosition.from_arg_list,
            "TO_UNIXTIME": exp.TimeToUnix.from_arg_list,
        }

    class Generator(Generator):
        TYPE_MAPPING = {
            exp.DataType.Type.INT: "INTEGER",
            exp.DataType.Type.FLOAT: "REAL",
            exp.DataType.Type.BINARY: "VARBINARY",
            exp.DataType.Type.TEXT: "VARCHAR",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP",
        }

        TRANSFORMS = {
            **Generator.TRANSFORMS,
            exp.ApproxDistinct: _approx_distinct_sql,
            exp.Array: lambda self, e: f"ARRAY[{self.expressions(e, flat=True)}]",
            exp.ArrayContains: rename_func("CONTAINS"),
            exp.ArraySize: rename_func("CARDINALITY"),
            exp.BitwiseAnd: lambda self, e: f"BITWISE_AND({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.BitwiseLeftShift: lambda self, e: f"BITWISE_ARITHMETIC_SHIFT_LEFT({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.BitwiseNot: lambda self, e: f"BITWISE_NOT({self.sql(e, 'this')})",
            exp.BitwiseOr: lambda self, e: f"BITWISE_OR({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.BitwiseRightShift: lambda self, e: f"BITWISE_ARITHMETIC_SHIFT_RIGHT({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.BitwiseXor: lambda self, e: f"BITWISE_XOR({self.sql(e, 'this')}, {self.sql(e, 'expression')})",
            exp.ConcatWs: _concat_ws_sql,
            exp.DataType: _datatype_sql,
            exp.DateAdd: lambda self, e: f"""DATE_ADD({self.sql(e, 'unit') or "'day'"}, {self.sql(e, 'expression')}, {self.sql(e, 'this')})""",
            exp.DateDiff: lambda self, e: f"""DATE_DIFF({self.sql(e, 'unit') or "'day'"}, {self.sql(e, 'expression')}, {self.sql(e, 'this')})""",
            exp.DateStrToDate: lambda self, e: f"CAST(DATE_PARSE({self.sql(e, 'this')}, {Presto.date_format}) AS DATE)",
            exp.DateToDateStr: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, {Presto.date_format})",
            exp.DateToDi: lambda self, e: f"CAST(DATE_FORMAT({self.sql(e, 'this')}, {Presto.dateint_format}) AS INT)",
            exp.DiToDate: lambda self, e: f"CAST(DATE_PARSE(CAST({self.sql(e, 'this')} AS VARCHAR), {Presto.dateint_format}) AS DATE)",
            exp.If: if_sql,
            exp.ILike: no_ilike_sql,
            exp.Initcap: _initcap_sql,
            exp.Lateral: _explode_to_unnest_sql,
            exp.Levenshtein: rename_func("LEVENSHTEIN_DISTANCE"),
            exp.Quantile: _quantile_sql,
            exp.Schema: _schema_sql,
            exp.SortArray: _no_sort_array,
            exp.StrPosition: _str_position_sql,
            exp.StrToTime: lambda self, e: f"DATE_PARSE({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.StrToUnix: lambda self, e: f"TO_UNIXTIME(DATE_PARSE({self.sql(e, 'this')}, {self.format_time(e)}))",
            exp.StructExtract: struct_extract_sql,
            exp.TableSample: no_tablesample_sql,
            exp.TimeStrToDate: _date_parse_sql,
            exp.TimeStrToTime: _date_parse_sql,
            exp.TimeStrToUnix: lambda self, e: f"TO_UNIXTIME(DATE_PARSE({self.sql(e, 'this')}, {Presto.time_format}))",
            exp.TimeToStr: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimeToTimeStr: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, {Presto.time_format})",
            exp.TimeToUnix: rename_func("TO_UNIXTIME"),
            exp.TsOrDiToDi: lambda self, e: f"CAST(SUBSTR(REPLACE(CAST({self.sql(e, 'this')} AS VARCHAR), '-', ''), 1, 8) AS INT)",
            exp.TsOrDsAdd: _ts_or_ds_add_sql,
            exp.TsOrDsToDateStr: _ts_or_ds_to_date_str_sql,
            exp.TsOrDsToDate: _ts_or_ds_to_date_sql,
            exp.UnixToStr: lambda self, e: f"DATE_FORMAT(FROM_UNIXTIME({self.sql(e, 'this')}), {self.format_time(e)})",
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
            exp.UnixToTimeStr: lambda self, e: f"DATE_FORMAT(FROM_UNIXTIME({self.sql(e, 'this')}), {Presto.time_format})",
        }