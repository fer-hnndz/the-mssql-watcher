sizes = {
    "char": -1,
    "varchar": -1,
    # First 4 represent amount of days since 1/1/1900
    # Next 4 bytes represent ticks from midnight.
    "datetime": 8,
    # Same as datetime but last 4 bytes represent minutes from midnight.
    "smalldatetime": 4,
    "int": 4,  # little-endian
    "bigint": 8,  # little-endian
    "tinyint": 1,
    "decimal": -1,
    # First 4 bytes represent the amount and the next 4 represent the scale (4 decimals)
    # Should always read the value and then divide it by 10,000 (10^4), kind of what decimal does.
    "money": 8,
}
