import unittest

import wire_composition as wc


class HttpParserTests(unittest.TestCase):
    def test_content_length_stream(self):
        first = (
            b"POST /api/v1/rpc HTTP/1.1\r\n"
            b"Host: x\r\n"
            b"Content-Length: 2\r\n\r\n{}"
        )
        second = (
            b"POST /api/v1/rpc HTTP/1.1\r\n"
            b"Host: x\r\n"
            b"Content-Length: 2\r\n\r\n[]"
        )
        parsed = wc.parse_http_stream(first + second)
        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[0].body, b"{}")
        self.assertEqual(parsed[1].body, b"[]")
        self.assertEqual(
            sum(message.total_wire_bytes for message in parsed),
            len(first + second),
        )

    def test_chunked_stream(self):
        raw = (
            b"HTTP/1.1 200 OK\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n"
            b"4\r\ntest\r\n"
            b"0\r\n\r\n"
        )
        parsed = wc.parse_http_stream(raw)
        self.assertEqual(parsed[0].body, b"test")
        self.assertGreater(parsed[0].transfer_framing_bytes, 0)
        self.assertEqual(parsed[0].total_wire_bytes, len(raw))

    def test_compact_json_is_stable(self):
        self.assertEqual(wc.compact_json(["a", "b"]), b'["a","b"]')

    def test_binary_fetch_body_parser(self):
        payloads = [b"abc", b"defgh"]
        body = bytearray(b"SKOF")
        body.append(1)
        body.extend((2).to_bytes(4, "little"))
        for payload in payloads:
            body.extend(len(payload).to_bytes(4, "little"))
            body.extend(payload)
        parsed, framing = wc.parse_binary_fetch_body(bytes(body))
        self.assertEqual(parsed, payloads)
        self.assertEqual(framing, 9 + 4 * len(payloads))

    def test_binary_fetch_body_rejects_trailing_bytes(self):
        body = b"SKOF" + bytes([1]) + (0).to_bytes(4, "little") + b"x"
        with self.assertRaises(ValueError):
            wc.parse_binary_fetch_body(body)


if __name__ == "__main__":
    unittest.main()
