import typing
import struct
from datetime import datetime, timedelta, time
from typing import Optional

from pymssql import Cursor

from .log_record import LogRecord
from .column_schema import ColumnSchema


class Parser:
    CURSOR: Optional[Cursor] = None
    """Class that parses the transaction log."""

    def __init__(self, cursor: Cursor, database: str = "sachen") -> None:
        """Creates a new parser for the specified database."""

        self.CURSOR = cursor
        self.database = database

    def _fetch_table_schema(self) -> dict[str, list[ColumnSchema]]:
        """Fetches the schema of the table. Sorts the column in how is it expected to be in the transaction log."""
        if not self.CURSOR:
            return {}

        self.CURSOR.execute(f"USE {self.database}")
        self.CURSOR.execute(
            "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS;"
        )
        tables: dict[str, list[ColumnSchema]] = {}

        for row in self.CURSOR.fetchall():
            table_name: str = row[0]
            self.CURSOR.execute(
                f"""SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, DATETIME_PRECISION, NUMERIC_SCALE, CHARACTER_OCTET_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}';"""
            )

            if table_name not in tables:
                tables[table_name] = []

            # Types that are put to the end of the RowLog because of their variable size.
            final_types = []
            for column_row in self.CURSOR.fetchall():
                (
                    column_name,
                    data_type,
                    char_max_length,
                    numeric_precision,
                    datetime_precision,
                    numeric_scale,
                    char_octet_length,
                ) = column_row

                if data_type in ["varchar", "nvarchar", "nchar"]:
                    final_types.append(
                        ColumnSchema(
                            COLUMN_NAME=column_name,
                            DATA_TYPE=data_type,
                            NUMERIC_PRECISION=numeric_precision,
                            DATETIME_PRECISION=datetime_precision,
                            NUMERIC_SCALE=numeric_scale,
                            CHARACTER_MAXIMUM_LENGTH=char_max_length,
                            CHARACTER_OCTET_LENGTH=char_octet_length,
                        )
                    )
                else:
                    if data_type == "decimal":
                        t = f"decimal({numeric_precision},{numeric_scale})"
                        print(t)
                        tables[table_name].append(
                            ColumnSchema(
                                COLUMN_NAME=column_name,
                                DATA_TYPE=t,
                                NUMERIC_PRECISION=numeric_precision,
                                DATETIME_PRECISION=datetime_precision,
                                NUMERIC_SCALE=numeric_scale,
                                CHARACTER_MAXIMUM_LENGTH=char_max_length,
                                CHARACTER_OCTET_LENGTH=char_octet_length,
                            )
                        )
                        continue

                    tables[table_name].append(
                        ColumnSchema(
                            COLUMN_NAME=column_name,
                            DATA_TYPE=data_type,
                            NUMERIC_PRECISION=numeric_precision,
                            DATETIME_PRECISION=datetime_precision,
                            NUMERIC_SCALE=numeric_scale,
                            CHARACTER_MAXIMUM_LENGTH=char_max_length,
                            CHARACTER_OCTET_LENGTH=char_octet_length,
                        )
                    )

            tables[table_name].extend(final_types)

        return tables

    def _fetch_transaction_log(self) -> list[LogRecord]:
        if (not self.CURSOR) or (not self.database):
            return []

        self.CURSOR.execute(f"USE {self.database};")

        self.CURSOR.execute(
            """SELECT 
    Operation,
    Context,
    [Transaction ID],
    AllocUnitName,
    [RowLog Contents 0],
    [RowLog Contents 1],
    [RowLog Contents 2],
    [RowLog Contents 3],
    *
FROM fn_dblog(NULL, NULL)
WHERE AllocUnitName IS NOT NULL 
  AND AllocUnitName NOT LIKE 'sys.%'
  AND AllocUnitName NOT LIKE 'Unknown Alloc%';
"""
        )

        logs: list[LogRecord] = []
        for row in self.CURSOR.fetchall():
            logs.append(
                LogRecord(
                    operation=row[0],
                    context=row[1],
                    transaction_id=row[2],
                    alloc_unit=row[3],
                    raw_data=row[4],
                    raw_data2=row[5],
                )
            )

        return logs

    def parse_bytes(
        self, data: bytes, table_schema: list[ColumnSchema]
    ) -> dict[str, typing.Any]:
        """Parse a raw byte array."""

        # Skip first bytes
        useful_data = data[4:]

        operation_data: dict[str, typing.Any] = {}
        for idx, col in enumerate(table_schema):
            data_type = col.DATA_TYPE
            column_name = col.COLUMN_NAME

            if col.DATA_TYPE not in ["varchar", "nvarchar", "nchar"]:
                if data_type.lower() == "int":
                    value = int.from_bytes(useful_data[:4], "little")
                    operation_data[column_name] = value
                    useful_data = useful_data[4:]

                elif data_type.lower() == "smallint":
                    value = int.from_bytes(useful_data[:2], "little")
                    operation_data[column_name] = value
                    useful_data = useful_data[2:]

                elif data_type.lower() == "tinyint":
                    value = int.from_bytes(useful_data[:1], "little")
                    operation_data[column_name] = value
                    useful_data = useful_data[1:]

                elif data_type.lower() == "bigint":
                    value = int.from_bytes(useful_data[:8], "little")
                    operation_data[column_name] = value
                    useful_data = useful_data[8:]

                elif data_type.lower() == "real":
                    value = struct.unpack("<f", useful_data[:4])[0]
                    exact_value = format(value, ".7g")
                    operation_data[column_name] = float(exact_value)
                    useful_data = useful_data[4:]

                elif data_type.lower() == "float":
                    value = struct.unpack("<d", useful_data[:8])[0]
                    exact_value = format(value, ".15g")
                    operation_data[column_name] = float(exact_value)
                    useful_data = useful_data[8:]

                elif "decimal" in data_type.lower() or data_type.lower() == "numeric":
                    # Obtener precisión y escala desde el esquema
                    precision = col.NUMERIC_PRECISION
                    scale = col.NUMERIC_SCALE

                    if precision is None or scale is None:
                        raise ValueError(
                            f"Precisión o escala no definida para {column_name}"
                        )

                    # Determinar la longitud según la precisión
                    if precision <= 9
                        bytes_for_value = 5
                    elif precision <= 19:
                        bytes_for_value = 9
                    elif precision <= 28:
                        bytes_for_value = 13
                    else:
                        bytes_for_value = 17

                    # Leer los bytes binarios para el valor
                    raw_value = useful_data[1:bytes_for_value]
                    is_negative = useful_data[0] & 0x80 != 0
                    value = int.from_bytes(raw_value, byteorder="little")
                    if is_negative:
                        value = -value

                    # Aplicar escala
                    operation_data[column_name] = value / (10**scale)
                    useful_data = useful_data[bytes_for_value:]

                elif data_type.lower() == "char":
                    # Determinar la longitud exacta

                    # * ver aca el data_type y cosas asi (esto es nota1)
                    length_in_bytes = next(
                        (
                            col.CHARACTER_MAXIMUM_LENGTH
                            for col in table_schema
                            if col.COLUMN_NAME == column_name
                        ),
                        None,
                    )

                    if length_in_bytes is None:
                        raise ValueError(
                            f"No se pudo determinar la longitud de {column_name}"
                        )
                    val = useful_data[:length_in_bytes].decode("latin1").strip()
                    operation_data[column_name] = val

                    useful_data = useful_data[length_in_bytes:]

                elif data_type.lower() == "money":
                    val = struct.unpack("<q", useful_data[:8])[0] / 10000.0
                    operation_data[column_name] = val
                    useful_data = useful_data[8:]

                elif data_type.lower() == "smallmoney":
                    value = struct.unpack("<i", useful_data[:4])[0] / 10000.0
                    operation_data[column_name] = value
                    useful_data = useful_data[4:]

                elif data_type.lower() == "date":
                    try:
                        days = int.from_bytes(
                            useful_data[:3],
                            "little",
                        )
                        if not (-693593 <= days <= 2958465):
                            raise ValueError(
                                f"[date] El valor de 'days' está fuera de rango: {days}"
                            )
                        value = datetime(1, 1, 1) + timedelta(days=days)

                        formatted_date = value.strftime("%Y-%m-%d")
                        operation_data[column_name] = formatted_date
                    except ValueError as e:
                        print(f"Error decodificando date para {column_name}: {e}")
                        operation_data[column_name] = None
                    finally:
                        useful_data = useful_data[3:]

                elif data_type.lower() == "time":
                    tick_bytes = 3  # Valor por defecto si algo falla
                    datetimeprecision = col.DATETIME_PRECISION
                    try:
                        # Validar si datetimeprecision es válido
                        if datetimeprecision is None:
                            raise ValueError(
                                f"No se pudo obtener la precisión para la columna {column_name}"
                            )

                        # Calcular el número de bytes necesarios según la precisión
                        tick_bytes = 3 + (datetimeprecision // 2)
                        if len(useful_data) < tick_bytes:
                            raise ValueError(
                                f"Se requieren al menos {tick_bytes} bytes para TIME con precisión {datetimeprecision}"
                            )

                        # Extraer los ticks desde los bytes
                        ticks = int.from_bytes(
                            useful_data[:tick_bytes],
                            "little",
                        )

                        # Ajustar los ticks a precisión máxima usando datetimeprecision
                        scale_factor = 10 ** (
                            7 - datetimeprecision
                        )  # Escalar a la precisión máxima
                        ticks *= scale_factor

                        # Convertir ticks a segundos
                        time_seconds = ticks / 10**7  # Ticks están en 1/10^7 segundos

                        # Calcular horas, minutos, segundos y microsegundos
                        hours = int(time_seconds // 3600)
                        minutes = int((time_seconds % 3600) // 60)
                        seconds = int(time_seconds % 60)
                        microseconds = int((time_seconds - int(time_seconds)) * 1e6)

                        decoded_time = time(
                            hour=hours,
                            minute=minutes,
                            second=seconds,
                            microsecond=microseconds,
                        )

                        formatted_time = decoded_time.strftime("%H:%M:%S.%f")
                        operation_data[column_name] = formatted_time

                    except Exception as e:
                        print(f"Error decodificando TIME para {column_name}: {e}")
                        operation_data[column_name] = None

                    finally:
                        # Asegurar que avanzamos el puntero, incluso si ocurre un error
                        useful_data = useful_data[tick_bytes:]

                elif data_type.lower() == "datetime":
                    try:
                        # Validar que haya al menos 8 bytes para DATETIME
                        if len(useful_data) < 8:
                            raise ValueError(
                                f"Se requieren al menos 8 bytes para DATETIME, se encontraron {len(useful_data)} bytes."
                            )

                        # Leer días y ticks
                        ticks = int.from_bytes(
                            useful_data[:4],
                            "little",
                        )
                        days = int.from_bytes(
                            useful_data[4:8],
                            "little",
                        )

                        # Validar rango de días
                        if not (0 <= days <= 366000):  # Asegurar un rango razonable
                            raise ValueError(f"Días fuera de rango: {days}")

                        # Calcular la fecha y hora
                        date = datetime(1900, 1, 1) + timedelta(days=days)
                        time_seconds = ticks / 300  # Ticks son 1/300 de segundo
                        hours = int(time_seconds // 3600)
                        minutes = int((time_seconds % 3600) // 60)
                        seconds = int(time_seconds % 60)
                        milliseconds = int((time_seconds - int(time_seconds)) * 1000)

                        # Crear el objeto datetime
                        decoded_datetime = datetime.combine(
                            date, datetime.min.time()
                        ) + timedelta(
                            hours=hours,
                            minutes=minutes,
                            seconds=seconds,
                            milliseconds=milliseconds,
                        )
                        formatted_datetime = decoded_datetime.strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        operation_data[column_name] = formatted_datetime

                    except Exception as e:
                        print(f"Error decodificando DATETIME para {column_name}: {e}")
                        operation_data[column_name] = None

                    finally:
                        useful_data = useful_data[8:]

                elif data_type.lower() == "smalldatetime":
                    minutes = int.from_bytes(useful_data[:2], "little")
                    days = int.from_bytes(useful_data[2:4], "little")

                    # Validar rango de días
                    if not (0 <= days <= 65535):
                        raise ValueError(
                            f"[smalldatetime] El valor de 'days' está fuera de rango: {days}"
                        )

                    # Calcular fecha y hora
                    date = datetime(1900, 1, 1) + timedelta(days=days)
                    hours = minutes // 60
                    remaining_minutes = minutes % 60

                    # Combinar fecha y hora
                    smalldatetime_value = datetime.combine(
                        date, datetime.min.time()
                    ) + timedelta(hours=hours, minutes=remaining_minutes)
                    operation_data[column_name] = smalldatetime_value
                    useful_data = useful_data[4:]

                elif data_type.lower() == "nchar":
                    length_in_bytes = next(
                        (
                            col.CHARACTER_MAXIMUM_LENGTH
                            for col in table_schema
                            if col.COLUMN_NAME == column_name
                        ),
                        None,
                    )

                    if length_in_bytes is None:
                        raise ValueError(
                            f"No se pudo determinar la longitud de {column_name}"
                        )
                    val = useful_data[:length_in_bytes].decode(
                        "utf-16le", errors="ignore"
                    )
                    operation_data[column_name] = val.strip()

                    useful_data = useful_data[length_in_bytes:]

                elif data_type.lower() == "binary":
                    length_in_bytes = next(
                        (
                            col.CHARACTER_MAXIMUM_LENGTH
                            for col in table_schema
                            if col.COLUMN_NAME == column_name
                        ),
                        None,
                    )
                    if length_in_bytes is None:
                        raise ValueError(
                            f"No se pudo determinar la longitud de {column_name}"
                        )

                    new_val = useful_data[:length_in_bytes]
                    hex_representation = f"0x{new_val.hex().upper()}"
                    operation_data[column_name] = hex_representation
                    useful_data = useful_data[length_in_bytes:]

                elif data_type.lower() == "rowversion":
                    val = useful_data[:8].hex().upper()
                    operation_data[column_name] = value
                    useful_data = useful_data[8:]
                else:
                    print(f"Tipo no manejado: {data_type}")
                    useful_data = useful_data[4:]

        # ================================================================================
        #                       Parse now variable length columns
        # ================================================================================

        # Filter only the variable length columns from the table schema
        variable_colummns = filter(
            lambda col: col.DATA_TYPE in ["varchar", "nvarchar", "nchar"],
            table_schema,
        )

        # Declare here the dictionary to store the offsets so we can use the same offsets for the next column
        variable_offsets: dict[int, int] = {}

        for idx, (col) in enumerate(variable_colummns):

            column_name = col.COLUMN_NAME
            data_type = col.DATA_TYPE

            raw = data  # Reset everything to the start of the data

            # This offset represents the start position of the columns from the start of the data
            column_offset = int.from_bytes(raw[2:4], "little")

            if column_offset >= len(raw):
                print("Error: Offset al número de columnas fuera del rango de datos.")
                return {}

            # The amount of columns is located 2 bytes after the column offset
            total_columns = int.from_bytes(
                raw[column_offset : column_offset + 2], "little"
            )

            print(f"Column count: {total_columns}")

            # Calculate the amount of columns that have a variable size
            null_bitmap_size = (total_columns + 7) // 8
            variable_column_count_offset = column_offset + 2 + null_bitmap_size

            # The amount of columns that have a variable size is located 2 bytes after the variable column count offset
            variable_column_count = int.from_bytes(
                raw[variable_column_count_offset : variable_column_count_offset + 2],
                "little",
            )

            print(f"Variable column count: {variable_column_count}")

            variable_data_start = (
                variable_column_count_offset + 2 + variable_column_count * 2
            )

            offset_start = variable_column_count_offset + 2  # Here is where data starts

            for i in range(variable_column_count):
                start_pos = offset_start + (i * 2)
                var_offset = int.from_bytes(raw[start_pos : start_pos + 2], "little")

                variable_offsets[i] = var_offset

            print("Variable offsets:", variable_offsets)

            last_offset = variable_offsets.get(idx - 1)
            start = last_offset if last_offset else variable_data_start
            end = variable_offsets[idx] if idx < len(variable_offsets) else len(raw)

            print(f"Start: {start}, End: {end}")

            data_chunk = raw[start:end]
            decoded_value = self.try_decode(data_chunk)
            operation_data[column_name] = decoded_value.strip()

        return operation_data

    def parse_online_transaction_log(self) -> dict[str, typing.Any]:
        """Just parse it. NOTE: Disposes the Cursor"""

        if (not self.CURSOR) or (not self.database):
            return {}

        schema = self._fetch_table_schema()
        log = self._fetch_transaction_log()

        parsed_transactions: dict[str, typing.Any] = {}
        for record in log:
            # Parse the table name from alloc_unit
            # dbo.Something.[...]

            # Realistically, nobody is naming a table with a dot in the name.
            # If you are one of those, consider yourself an opp 🫵
            # - JH, 2024
            table_name = record.alloc_unit.split(".")[1]
            table_schema = schema.get(table_name, [])

            if not table_schema:
                raise TypeError(f"Table {table_name} not found in schema.")

            if (
                record.operation == "LOP_INSERT_ROWS"
                or record.operation == "LOP_DELETE_ROWS"
            ):

                if not parsed_transactions.get(record.operation):
                    parsed_transactions[record.operation] = {}
                    parsed_transactions[record.operation][table_name] = []

                elif not parsed_transactions[record.operation].get(table_name):
                    parsed_transactions[record.operation][table_name] = []

                parsed_transactions[record.operation][table_name].append(
                    {
                        "data": self.parse_bytes(record.raw_data, table_schema),
                        "transaction_id": record.transaction_id,
                        "schema": record.alloc_unit.split(".")[0],
                        "table": table_name,
                    }
                )

            # elif (
            #     record.operation == "LOP_MODIFY_ROW"
            #     and record.context == "LCX_CLUSTERED"
            # ):
            #     if not parsed_transactions.get(record.operation):
            #         parsed_transactions[record.operation] = {}
            #         parsed_transactions[record.operation][table_name] = []

            #     elif not parsed_transactions[record.operation].get(table_name):
            #         parsed_transactions[record.operation][table_name] = []

            #     parsed_transactions[record.operation][table_name].append(
            #         {
            #             "data_before": self.parse_bytes(record.raw_data, table_schema),
            #             "data_after": self.parse_bytes(record.raw_data2, table_schema),
            #             "transaction_id": record.transaction_id,
            #             "schema": table_name.split(".")[0],
            #         }
            #     )

        self.CURSOR.close()
        return parsed_transactions

    def try_decode(self, data):
        """
        Detecta automáticamente la codificación de los datos.
        Decodifica en UTF-8 primero, pero si encuentra un patrón típico de UTF-16, cambia a UTF-16.
        """

        if all(data[i] == 0 for i in range(1, len(data), 2)):
            try:
                return data.decode("utf-16", errors="strict")
            except UnicodeDecodeError:
                pass

        try:
            return data.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            return data.decode("utf-8", errors="replace")
