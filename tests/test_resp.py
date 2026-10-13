"""Unit tests for RESP2 parser and encoder (Phase 1)."""

import pytest
from mini_redis.resp import (
    IncompleteDataError,
    RESPParser,
    RespError,
    RespParseError,
    SimpleString,
    decode_resp,
    encode_array,
    encode_bulk_string,
    encode_error,
    encode_integer,
    encode_resp,
    encode_simple_string,
)


class TestRESPEncoder:
    """Tests for encoding Python objects to RESP2 wire format."""

    def test_encode_simple_string(self) -> None:
        assert encode_simple_string("OK") == b"+OK\r\n"
        assert encode_simple_string("PONG") == b"+PONG\r\n"
        assert encode_resp(SimpleString("OK")) == b"+OK\r\n"

    def test_encode_simple_string_invalid(self) -> None:
        with pytest.raises(ValueError):
            encode_simple_string("line1\rline2")
        with pytest.raises(ValueError):
            encode_simple_string("line1\nline2")

    def test_encode_error(self) -> None:
        assert encode_error("ERR unknown command") == b"-ERR unknown command\r\n"
        assert encode_resp(RespError("WRONGTYPE Operation against a key")) == (
            b"-WRONGTYPE Operation against a key\r\n"
        )

    def test_encode_error_invalid(self) -> None:
        with pytest.raises(ValueError):
            encode_error("bad\r\nerror")

    def test_encode_integer(self) -> None:
        assert encode_integer(0) == b":0\r\n"
        assert encode_integer(100) == b":100\r\n"
        assert encode_integer(-42) == b":-42\r\n"
        assert encode_resp(12345) == b":12345\r\n"

    def test_encode_integer_bool_rejected(self) -> None:
        with pytest.raises(TypeError):
            encode_integer(True)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            encode_resp(False)

    def test_encode_bulk_string(self) -> None:
        assert encode_bulk_string(b"foobar") == b"$6\r\nfoobar\r\n"
        assert encode_bulk_string("hello") == b"$5\r\nhello\r\n"
        assert encode_bulk_string(b"") == b"$0\r\n\r\n"
        assert encode_bulk_string(None) == b"$-1\r\n"
        assert encode_resp(b"binary\x00data\r\n") == b"$13\r\nbinary\x00data\r\n\r\n"
        assert encode_resp(None) == b"$-1\r\n"

    def test_encode_array(self) -> None:
        # Standard command: SET key value
        cmd = [b"SET", b"mykey", b"myvalue"]
        expected = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"
        assert encode_array(cmd) == expected
        assert encode_resp(cmd) == expected

        # Empty array
        assert encode_array([]) == b"*0\r\n"
        assert encode_resp([]) == b"*0\r\n"

        # Null array
        assert encode_array(None) == b"*-1\r\n"

        # Mixed array
        mixed = [SimpleString("OK"), 100, b"bulk", None]
        expected_mixed = b"*4\r\n+OK\r\n:100\r\n$4\r\nbulk\r\n$-1\r\n"
        assert encode_array(mixed) == expected_mixed


class TestRESPDecoder:
    """Tests for decoding RESP2 wire format to Python objects."""

    def test_decode_simple_string(self) -> None:
        val, consumed = decode_resp(b"+OK\r\n")
        assert val == SimpleString("OK")
        assert consumed == 5

    def test_decode_error(self) -> None:
        val, consumed = decode_resp(b"-ERR unknown command 'foo'\r\n")
        assert isinstance(val, RespError)
        assert val.message == "ERR unknown command 'foo'"
        assert consumed == 28

    def test_decode_integer(self) -> None:
        val, consumed = decode_resp(b":1000\r\n")
        assert val == 1000
        assert consumed == 7

        val_neg, consumed_neg = decode_resp(b":-1\r\n")
        assert val_neg == -1
        assert consumed_neg == 5

    def test_decode_bulk_string(self) -> None:
        val, consumed = decode_resp(b"$6\r\nfoobar\r\n")
        assert val == b"foobar"
        assert consumed == 12

        val_empty, consumed_empty = decode_resp(b"$0\r\n\r\n")
        assert val_empty == b""
        assert consumed_empty == 6

        val_null, consumed_null = decode_resp(b"$-1\r\n")
        assert val_null is None
        assert consumed_null == 5

    def test_decode_bulk_string_with_embedded_crlf(self) -> None:
        # A 6-byte string containing \r\n inside the payload
        payload = b"foo\r\nbar"
        raw = f"${len(payload)}\r\n".encode("ascii") + payload + b"\r\n"
        val, consumed = decode_resp(raw)
        assert val == payload
        assert consumed == len(raw)

    def test_decode_array(self) -> None:
        raw = b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"
        val, consumed = decode_resp(raw)
        assert val == [b"ECHO", b"hello"]
        assert consumed == len(raw)

        # Empty array
        val_empty, consumed_empty = decode_resp(b"*0\r\n")
        assert val_empty == []
        assert consumed_empty == 4

        # Null array
        val_null, consumed_null = decode_resp(b"*-1\r\n")
        assert val_null is None
        assert consumed_null == 5

    def test_decode_nested_array(self) -> None:
        raw = b"*2\r\n*2\r\n:1\r\n:2\r\n*1\r\n+OK\r\n"
        val, consumed = decode_resp(raw)
        assert val == [[1, 2], [SimpleString("OK")]]
        assert consumed == len(raw)

    def test_decode_inline_command(self) -> None:
        val, consumed = decode_resp(b"PING\r\n")
        assert val == [b"PING"]
        assert consumed == 6

        val2, consumed2 = decode_resp(b"SET key 12345\r\n")
        assert val2 == [b"SET", b"key", b"12345"]
        assert consumed2 == 15

    def test_decode_incomplete_raises_incomplete_data_error(self) -> None:
        with pytest.raises(IncompleteDataError):
            decode_resp(b"")
        with pytest.raises(IncompleteDataError):
            decode_resp(b"+OK")
        with pytest.raises(IncompleteDataError):
            decode_resp(b"$6\r\nfoo")
        with pytest.raises(IncompleteDataError):
            decode_resp(b"*2\r\n$4\r\nECHO\r\n")

    def test_decode_corrupted_data_raises_parse_error(self) -> None:
        # Invalid integer
        with pytest.raises(RespParseError):
            decode_resp(b":notanumber\r\n")

        # Invalid bulk string length
        with pytest.raises(RespParseError):
            decode_resp(b"$-2\r\n")

        # Missing terminating CRLF on bulk string
        with pytest.raises(RespParseError):
            decode_resp(b"$3\r\nfooXX")


class TestStreamingRESPParser:
    """Tests for RESPParser buffering and incremental stream handling."""

    def test_streaming_chunk_by_chunk(self) -> None:
        parser = RESPParser()
        full_msg = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n"

        # Feed 1 byte at a time
        for i, byte in enumerate(full_msg):
            parser.feed(bytes([byte]))
            msg = parser.get_message()
            if i < len(full_msg) - 1:
                assert msg is None
            else:
                assert msg == [b"SET", b"mykey", b"myvalue"]

        # Buffer should be completely consumed
        assert len(parser) == 0
        assert parser.get_message() is None

    def test_streaming_pipelined_messages(self) -> None:
        parser = RESPParser()
        # Feed two commands at once in a single TCP chunk
        packet = b"+OK\r\n+PONG\r\n:42\r\n"
        parser.feed(packet)

        assert parser.get_message() == SimpleString("OK")
        assert parser.get_message() == SimpleString("PONG")
        assert parser.get_message() == 42
        assert parser.get_message() is None
        assert len(parser) == 0

    def test_streaming_clear_on_error(self) -> None:
        parser = RESPParser()
        parser.feed(b":invalid\r\n")
        with pytest.raises(RespParseError):
            parser.get_message()
        assert len(parser) == 0


class TestSafetyLimits:
    """Tests for protocol safety limits in RESP decoding."""

    def test_bulk_string_exceeds_max_size(self) -> None:
        too_large = (512 * 1024 * 1024) + 1
        with pytest.raises(RespParseError, match="Bulk string size.*exceeds maximum allowed"):
            decode_resp(f"${too_large}\r\n".encode("ascii"))

    def test_array_exceeds_max_len(self) -> None:
        too_many = 1_000_001
        with pytest.raises(RespParseError, match="Array length.*exceeds maximum allowed"):
            decode_resp(f"*{too_many}\r\n".encode("ascii"))

    def test_nesting_depth_exceeded(self) -> None:
        nested = b"*1\r\n" * 33 + b":1\r\n"
        with pytest.raises(RespParseError, match="Exceeded maximum nesting depth"):
            decode_resp(nested)

    def test_inline_length_exceeded_with_crlf(self) -> None:
        long_line = b"P" * (64 * 1024 + 1) + b"\r\n"
        with pytest.raises(RespParseError, match="Inline command length exceeds"):
            decode_resp(long_line)

    def test_inline_length_exceeded_without_crlf(self) -> None:
        long_line = b"P" * (64 * 1024 + 1)
        with pytest.raises(RespParseError, match="Inline command length exceeds"):
            decode_resp(long_line)

