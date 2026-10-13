"""RESP2 (Redis Serialization Protocol) Parser and Encoder.

This module implements the RESP2 specification:
- Simple Strings: +<string>\r\n
- Errors: -<error_message>\r\n
- Integers: :<integer>\r\n
- Bulk Strings: $<length>\r\n<data>\r\n  (or $-1\r\n for null)
- Arrays: *<count>\r\n<element_1>...<element_n>  (or *-1\r\n for null)
- Inline commands: <command> <arg1> ...\r\n (fallback for telnet/redis-cli)

Safety limits:
- Max bulk string size: 512 MB
- Max array length: 1,000,000
- Max nesting depth: 32
- Max inline line length: 64 KB

Standard library only. All data handling is binary-safe using bytes.
"""

from dataclasses import dataclass
from typing import Any, Optional, Union

# Constant delimiters for RESP protocol
CRLF = b"\r\n"
CRLF_LEN = len(CRLF)

# Protocol safety limits
MAX_BULK_STRING_SIZE = 512 * 1024 * 1024  # 512 MB
MAX_ARRAY_LEN = 1_000_000
MAX_NESTING_DEPTH = 32
MAX_INLINE_LENGTH = 64 * 1024  # 64 KB


class RespError(Exception):
    """Represents a RESP error response (-ERR ...\r\n)."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __eq__(self, other: object) -> bool:
        if isinstance(other, RespError):
            return self.message == other.message
        return False

    def __repr__(self) -> str:
        return f"RespError({self.message!r})"


@dataclass(frozen=True)
class SimpleString:
    """Represents a RESP Simple String (+OK\r\n)."""

    data: str

    def __repr__(self) -> str:
        return f"SimpleString({self.data!r})"


class IncompleteDataError(Exception):
    """Raised when the buffer does not yet contain a complete RESP message."""
    pass


class RespParseError(Exception):
    """Raised when incoming bytes violate the RESP protocol or exceed safety limits."""
    pass


# ---------------------------------------------------------------------------
# Encoders
# ---------------------------------------------------------------------------

def encode_simple_string(s: str) -> bytes:
    """Encode a simple string: +<string>\r\n"""
    # Simple strings cannot contain CR or LF characters
    if "\r" in s or "\n" in s:
        raise ValueError("Simple strings must not contain CR or LF characters.")
    return f"+{s}\r\n".encode("utf-8")


def encode_error(err_message: str) -> bytes:
    """Encode an error: -<message>\r\n"""
    if "\r" in err_message or "\n" in err_message:
        raise ValueError("Error messages must not contain CR or LF characters.")
    return f"-{err_message}\r\n".encode("utf-8")


def encode_integer(val: int) -> bytes:
    """Encode an integer: :<int>\r\n"""
    if isinstance(val, bool):
        raise TypeError("Booleans cannot be encoded as RESP integers.")
    return f":{val}\r\n".encode("ascii")


def encode_bulk_string(val: Optional[Union[bytes, str]]) -> bytes:
    """Encode a bulk string: $<len>\r\n<bytes>\r\n, or $-1\r\n for None."""
    if val is None:
        return b"$-1\r\n"
    if isinstance(val, str):
        val = val.encode("utf-8")
    elif not isinstance(val, (bytes, bytearray)):
        raise TypeError(f"Expected bytes, str, or None, got {type(val).__name__}")
    return f"${len(val)}\r\n".encode("ascii") + bytes(val) + CRLF


def encode_array(val: Optional[Union[list, tuple]]) -> bytes:
    """Encode an array: *<count>\r\n<elem1>...<elemn>, or *-1\r\n for None."""
    if val is None:
        return b"*-1\r\n"
    if not isinstance(val, (list, tuple)):
        raise TypeError(f"Expected list, tuple, or None, got {type(val).__name__}")

    parts = [f"*{len(val)}\r\n".encode("ascii")]
    for item in val:
        parts.append(encode_resp(item))
    return b"".join(parts)


def encode_resp(val: Any) -> bytes:
    """
    Polymorphic encoder for Python types into RESP2 format:
    - None -> Null bulk string ($-1\r\n)
    - SimpleString -> Simple string (+<str>\r\n)
    - RespError -> Error (-<str>\r\n)
    - int (not bool) -> Integer (:<int>\r\n)
    - bytes / bytearray -> Bulk string ($<len>\r\n<data>\r\n)
    - str -> Bulk string (UTF-8 encoded)
    - list / tuple -> Array (*<len>\r\n...)
    """
    if val is None:
        return b"$-1\r\n"
    if isinstance(val, SimpleString):
        return encode_simple_string(val.data)
    if isinstance(val, RespError):
        return encode_error(val.message)
    if isinstance(val, bool):
        raise TypeError("Booleans are not supported in RESP2 directly.")
    if isinstance(val, int):
        return encode_integer(val)
    if isinstance(val, (bytes, bytearray)):
        return encode_bulk_string(val)
    if isinstance(val, str):
        return encode_bulk_string(val)
    if isinstance(val, (list, tuple)):
        return encode_array(val)

    raise TypeError(f"Cannot encode type {type(val).__name__} to RESP.")


# ---------------------------------------------------------------------------
# Decoders / Parser
# ---------------------------------------------------------------------------

def decode_resp(data: Union[bytes, bytearray], depth: int = 0) -> tuple[Any, int]:
    """
    Decode a single RESP2 message from bytes.

    Returns:
        (parsed_value, bytes_consumed)

    Raises:
        IncompleteDataError: If data ends before a complete RESP message.
        RespParseError: If syntax is invalid or safety limits are exceeded.
    """
    if not data:
        raise IncompleteDataError("Buffer is empty.")

    if depth > MAX_NESTING_DEPTH:
        raise RespParseError(f"Exceeded maximum nesting depth of {MAX_NESTING_DEPTH}.")

    prefix = data[0:1]

    # Simple String: +<str>\r\n
    if prefix == b"+":
        crlf_pos = data.find(CRLF)
        if crlf_pos == -1:
            raise IncompleteDataError("Incomplete simple string.")
        content = data[1:crlf_pos].decode("utf-8", errors="replace")
        return SimpleString(content), crlf_pos + CRLF_LEN

    # Error: -<error>\r\n
    if prefix == b"-":
        crlf_pos = data.find(CRLF)
        if crlf_pos == -1:
            raise IncompleteDataError("Incomplete error string.")
        content = data[1:crlf_pos].decode("utf-8", errors="replace")
        return RespError(content), crlf_pos + CRLF_LEN

    # Integer: :<int>\r\n
    if prefix == b":":
        crlf_pos = data.find(CRLF)
        if crlf_pos == -1:
            raise IncompleteDataError("Incomplete integer.")
        raw_int = data[1:crlf_pos]
        try:
            val = int(raw_int)
        except ValueError as err:
            raise RespParseError(f"Invalid integer format: {raw_int!r}") from err
        return val, crlf_pos + CRLF_LEN

    # Bulk String: $<len>\r\n<data>\r\n
    if prefix == b"$":
        crlf_pos = data.find(CRLF)
        if crlf_pos == -1:
            raise IncompleteDataError("Incomplete bulk string header.")
        raw_len = data[1:crlf_pos]
        try:
            str_len = int(raw_len)
        except ValueError as err:
            raise RespParseError(f"Invalid bulk string length: {raw_len!r}") from err

        # Null bulk string
        if str_len == -1:
            return None, crlf_pos + CRLF_LEN
        if str_len < -1:
            raise RespParseError(f"Invalid negative bulk string length: {str_len}")
        if str_len > MAX_BULK_STRING_SIZE:
            raise RespParseError(
                f"Bulk string size {str_len} exceeds maximum allowed of {MAX_BULK_STRING_SIZE}."
            )

        start_data = crlf_pos + CRLF_LEN
        end_data = start_data + str_len
        total_len = end_data + CRLF_LEN

        if len(data) < total_len:
            raise IncompleteDataError("Incomplete bulk string payload.")

        # Ensure the trailing delimiter is CRLF
        if data[end_data:total_len] != CRLF:
            raise RespParseError("Bulk string not followed by CRLF.")

        content = bytes(data[start_data:end_data])
        return content, total_len

    # Array: *<count>\r\n<elem1>...<elemn>
    if prefix == b"*":
        crlf_pos = data.find(CRLF)
        if crlf_pos == -1:
            raise IncompleteDataError("Incomplete array header.")
        raw_count = data[1:crlf_pos]
        try:
            count = int(raw_count)
        except ValueError as err:
            raise RespParseError(f"Invalid array count: {raw_count!r}") from err

        # Null array
        if count == -1:
            return None, crlf_pos + CRLF_LEN
        if count < -1:
            raise RespParseError(f"Invalid negative array count: {count}")
        if count > MAX_ARRAY_LEN:
            raise RespParseError(
                f"Array length {count} exceeds maximum allowed of {MAX_ARRAY_LEN}."
            )

        offset = crlf_pos + CRLF_LEN
        elements: list[Any] = []
        for _ in range(count):
            elem, consumed = decode_resp(data[offset:], depth=depth + 1)
            elements.append(elem)
            offset += consumed

        return elements, offset

    # Inline command support (e.g., "PING\r\n" or "SET a b\r\n")
    # Useful for telnet or simple netcat test sessions
    crlf_pos = data.find(CRLF)
    lf_pos = data.find(b"\n")

    if crlf_pos == -1 and lf_pos == -1:
        if len(data) > MAX_INLINE_LENGTH:
            raise RespParseError(
                f"Inline command length exceeds maximum allowed of {MAX_INLINE_LENGTH}."
            )
        raise IncompleteDataError("Incomplete inline command.")

    if crlf_pos != -1:
        line = data[:crlf_pos]
        consumed = crlf_pos + CRLF_LEN
    else:
        line = data[:lf_pos]
        consumed = lf_pos + 1

    if len(line) > MAX_INLINE_LENGTH:
        raise RespParseError(
            f"Inline command length exceeds maximum allowed of {MAX_INLINE_LENGTH}."
        )

    parts = line.strip().split()
    if not parts:
        # Empty line, consume and return empty array
        return [], consumed

    # Return as list of bulk string bytes matching standard parsed command format
    return [part for part in parts], consumed


class RESPParser:
    """
    Streaming RESP2 parser for network connections.

    Buffers bytes and yields complete parsed messages one by one.
    """

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, data: bytes) -> None:
        """Feed incoming chunks of bytes into the internal buffer."""
        self._buffer.extend(data)

    def get_message(self) -> Optional[Any]:
        """
        Attempt to parse the next complete message from the buffer.

        Returns:
            The parsed message object, or None if more data is required.

        Raises:
            RespParseError: If invalid protocol syntax or limit violation is detected.
        """
        if not self._buffer:
            return None

        try:
            msg, consumed = decode_resp(self._buffer)
        except IncompleteDataError:
            return None
        except Exception:
            # On parse error, clear buffer to prevent getting stuck
            self._buffer.clear()
            raise

        del self._buffer[:consumed]
        return msg

    def clear(self) -> None:
        """Clear the internal buffer."""
        self._buffer.clear()

    def __len__(self) -> int:
        return len(self._buffer)
