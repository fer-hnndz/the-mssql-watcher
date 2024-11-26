from .log_record import LogRecord
import typing
from typing import Optional
from pymssql import Cursor


class Parser:
    CURSOR: Optional[Cursor] = None
    """Class that parses the transaction log."""

    def __init__(self, cursor: Cursor, database: str = "sachen") -> None:
        """Creates a new parser for the specified database."""

        self.CURSOR = cursor
        self.database = database

    def _fetch_table_schema(self) -> dict[str, list[tuple[str, str]]]:
        """Fetches the schema of the table. Sorts the column in how is it expected to be in the transaction log."""
        if not self.CURSOR:
            return {}

        self.CURSOR.execute(f"USE {self.database}")
        self.CURSOR.execute(
            "SELECT DISTINCT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS;"
        )
        tables: dict[str, list[tuple[str, str]]] = {}

        for row in self.CURSOR.fetchall():
            table_name: str = row[0]
            self.CURSOR.execute(
                f"""SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}';"""
            )

            if table_name not in tables:
                tables[table_name] = []

            # Types that are put to the end of the RowLog because of their variable size.
            final_types = []
            for column_row in self.CURSOR.fetchall():
                column_name, data_type, numeric_precision, numeric_scale = column_row

                if data_type in ["char", "varchar", "nvarchar", "nchar"]:
                    final_types.append((column_name, data_type))
                else:
                    if data_type == "decimal":
                        t = f"decimal({numeric_precision},{numeric_scale})"
                        print(t)
                        tables[table_name].append((column_name, t))
                        continue

                    tables[table_name].append((column_name, data_type))

            tables[table_name].extend(final_types)

        return tables

    def _fetch_transaction_log(self) -> list[LogRecord]:
        # TODO: Usar base de datos que quiera usar;
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

    def parse_online_transaction_log(self) -> dict[str, typing.Any]:
        """Just parse it. NOTE: Disposes the Cursor"""

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

            if record.operation == "LOP_INSERT_ROWS":

                if not parsed_transactions.get(record.operation):
                    parsed_transactions[record.operation] = {}
                    parsed_transactions[record.operation][table_name] = []

                elif not parsed_transactions[record.operation].get(table_name):
                    parsed_transactions[record.operation][table_name] = []

                # Skip first bytes
                useful_data = record.raw_data[4:]

                operation_data: dict[str, typing.Any] = {}
                for idx, (column_name, data_type) in enumerate(table_schema):
                    if data_type not in ["char", "varchar", "nvarchar", "nchar"]:

                        if data_type == "int":
                            read_int = int.from_bytes(useful_data[:4], "little")
                            operation_data[column_name] = read_int
                            useful_data = useful_data[4:]  # Skip parsed bytes

                        elif "decimal" in data_type:
                            # Parse precision and scale from decimal(x,y)

                            precision = int(
                                data_type.split(",")[0].replace("decimal(", "")
                            )
                            bytes_for_value = 0

                            # Determine amount of bytes to read
                            if 1 <= precision <= 9:
                                bytes_for_value = 5
                            elif 10 <= precision <= 19:
                                bytes_for_value = 9
                            elif 20 <= precision <= 28:
                                bytes_for_value = 13
                            elif 29 <= precision <= 38:
                                bytes_for_value = 17
                            else:
                                raise ValueError(
                                    "La precisión debe estar entre 1 y 38."
                                )

                            # Verifica que la entrada tenga suficiente longitud
                            if len(useful_data) < bytes_for_value + 1:
                                raise ValueError(
                                    f"La entrada tiene solo {len(useful_data)} bytes, pero se esperaban al menos {bytes_for_value + 1}."
                                )

                            # Read the scale from the first byte
                            scale = useful_data[0]
                            if not (0 <= scale <= precision):
                                raise ValueError(
                                    f"Escala inválida: {scale}. Debe estar entre 0 y {precision}."
                                )

                            # Read the rest as an integer
                            value_bytes = useful_data[1:bytes_for_value]

                            # Weird check to see if the most significant bit is 1
                            is_negative = value_bytes[-1] & 0x80 != 0

                            print("Value bytes:", value_bytes)
                            print("Is negative:", is_negative)

                            if is_negative:
                                # Apply two's complement
                                # In a couple months, only God and GPT will know how this works

                                value = int.from_bytes(
                                    value_bytes, byteorder="little", signed=False
                                )
                                value = -(~value + 1)
                            else:
                                value = int.from_bytes(
                                    value_bytes, byteorder="little", signed=False
                                )

                            decimal_value = value / (10**scale)
                            operation_data[column_name] = decimal_value

                # ================================================================================
                #                       Parse now variable length columns
                # ================================================================================

                # Filter only the variable length columns from the table schema
                variable_colummns = filter(
                    lambda col: col[1] in ["char", "varchar", "nvarchar", "nchar"],
                    table_schema,
                )

                # Declare here the dictionary to store the offsets so we can use the same offsets for the next column
                variable_offsets: dict[int, int] = {}

                for idx, (column_name, data_type) in enumerate(variable_colummns):

                    raw = record.raw_data  # Reset everything to the start of the data

                    # This offset represents the start position of the columns from the start of the data
                    column_offset = int.from_bytes(raw[2:4], "little")

                    if column_offset >= len(raw):
                        print(
                            "Error: Offset al número de columnas fuera del rango de datos."
                        )
                        return None

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
                        raw[
                            variable_column_count_offset : variable_column_count_offset
                            + 2
                        ],
                        "little",
                    )

                    print(f"Variable column count: {variable_column_count}")

                    variable_data_start = (
                        variable_column_count_offset + 2 + variable_column_count * 2
                    )

                    offset_start = (
                        variable_column_count_offset + 2
                    )  # Here is where data starts

                    for i in range(variable_column_count):
                        start_pos = offset_start + (i * 2)
                        var_offset = int.from_bytes(
                            raw[start_pos : start_pos + 2], "little"
                        )

                        variable_offsets[i] = var_offset

                    print("Variable offsets:", variable_offsets)

                    last_offset = variable_offsets.get(idx - 1)
                    start = last_offset if last_offset else variable_data_start
                    end = (
                        variable_offsets[idx]
                        if idx < len(variable_offsets)
                        else len(raw)
                    )

                    print(f"Start: {start}, End: {end}")

                    data_chunk = raw[start:end]
                    decoded_value = self.try_decode(data_chunk)
                    operation_data[column_name] = decoded_value.strip()

                parsed_transactions[record.operation][table_name].append(operation_data)

        self.CURSOR.close()
        print("Parsed Transactions:", parsed_transactions)
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
