#ifndef ARROW_TOOLS_STRING_BUFFER_H_
#define ARROW_TOOLS_STRING_BUFFER_H_

#include "rapidjson/rapidjson.h" // RAPIDJSON_UNLIKELY

/*
 * A buffer callers may append to infinitely -- but only the first `maxLength`
 * bytes will be stored.
 */
struct StringBuffer {
    using buffer_type = std::vector<uint8_t>;
    using size_type = buffer_type::size_type;

    buffer_type bytes;
    size_type pos; // may go past maxLength

    StringBuffer(size_type maxLength) : bytes(maxLength), pos(0) {}

    void append(const uint8_t* s, size_type len) {
        // append to this->bytes only where there's room
        if (this->pos < this->bytes.size()) {
            auto n = std::min(len, this->bytes.size() - pos);
            std::copy(s, &s[n], &this->bytes[pos]);
        }
        // always "append" to this->pos
        this->pos += len;
    }

    void append(const char* s, size_type len) {
        this->append(reinterpret_cast<const uint8_t*>(s), len);
    }

    void append(uint8_t c) {
        this->append(&c, 1);
    }

    void append(char c) {
        this->append(&c, 1);
    }

    void appendAsJsonQuotedString(const uint8_t* s, uint32_t len) {
        // Logic copied from rapidjson writer.h
        static const char hexDigits[16] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
        static const char escape[256] = {
#define Z16 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
            //0    1    2    3    4    5    6    7    8    9    A    B    C    D    E    F
            'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'b', 't', 'n', 'u', 'f', 'r', 'u', 'u', // 00
            'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', // 10
              0,   0, '"',   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0, // 20
            Z16, Z16,                                                                       // 30~4F
              0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,'\\',   0,   0,   0, // 50
            Z16, Z16, Z16, Z16, Z16, Z16, Z16, Z16, Z16, Z16                                // 60~FF
#undef Z16
        };

        // https://tools.ietf.org/html/rfc7159#page-8
        this->append('"');
        for (auto i = 0; i < len; i++) {
            uint8_t c = s[i];
            if (RAPIDJSON_UNLIKELY(escape[c])) {
                // \n, \f, \r, etc. Might even be \u....
                this->append('\\');
                this->append(escape[c]);

                if (escape[c] == 'u') {
                    // we wrote \u; now write 00XX to make a \u00XX code
                    this->append("00", 2);
                    this->append(hexDigits[c >> 4]);
                    this->append(hexDigits[c & 0xf]);
                }
            } else {
                this->append(c);
            }
        }
        this->append('"');
    }

    void reset() {
        this->pos = 0;
    }

    bool hasOverflow() const {
        return this->pos > this->bytes.size();
    }

    uint32_t validUtf8Length() const {
        if (this->pos > this->bytes.size()) {
            return this->calculateGreatestValidUtf8Length(&this->bytes[0], this->bytes.size());
        } else {
            return this->pos;
        }
    }

    /// Build valid UTF-8 string, shaving the last few characters if needed
    ///
    /// Assumes the appended data was all valid UTF-8.
    std::string copyUtf8String() const {
        return std::string(&this->bytes[0], &this->bytes[this->validUtf8Length()]);
    }

    std::string_view toUtf8StringView() const {
        return std::string_view(reinterpret_cast<const char*>(&this->bytes[0]), this->validUtf8Length());
    }

    std::string_view toRawStringView() const {
        return std::string_view(reinterpret_cast<const char*>(&this->bytes[0]), std::min(this->pos, this->bytes.size()));
    }

private:
    uint32_t calculateGreatestValidUtf8Length(const uint8_t* buf, uint32_t len) const {
        if (len == 0) return 0;
        uint8_t lastByte = buf[len - 1];

        if ((lastByte & 0xc0) == 0xc0) {
            // Byte fits format 0b11xxxxxx. That means it's the first byte of a
            // UTF-8 sequence. Drop it.
            return len - 1;
        } else if ((lastByte & 0xc0) == 0x80) {
            // Byte fits format 0b10xxxxxx. That means it's a continuation byte.
            // Assume valid UTF-8.

            // Since this is valid UTF-8, we know len >= 2
            char secondLastByte = buf[len - 2];

            if ((secondLastByte & 0xe0) == 0xc0) {
                // Previous byte fits format 0b110xxxxx (first byte of a 2-byte
                // sequence). This 2-byte sequence is valid UTF-8. Include it.
                return len;
            } else if ((secondLastByte & 0xe0) == 0xe0) {
                // Byte fits format 0b111xxxxx. That means it's the first byte of a
                // 3-byte or 4-byte UTF-8 sequence. We're truncating mid-sequence.
                // Drop this incomplete sequence of bytes.
                return len - 2;
            }

            // Since this is valid UTF-8, we know len >= 3
            char thirdLastByte = buf[len - 3];
            if ((thirdLastByte & 0xf8) == 0xf0) {
                // Third-last byte fits format 0b11110xxx (first byte of a 4-byte
                // sequence). Drop all three bytes.
                return len - 3;
            } else {
                // We're either the 3rd byte of a 3-byte sequence or the 4th byte of a
                // 4-byte sequence. Good.
                return len;
            }
        } else {
            // ASCII character
            return len;
        }
    }
};

#endif  // ARROW_TOOLS_STRING_BUFFER_H_
