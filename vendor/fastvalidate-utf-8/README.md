Copied from https://github.com/lemire/fastvalidate-utf-8
... and then deleted the non-AVX2 code, to compile with -Werror=unused-function

Explainer: https://lemire.me/blog/2018/10/19/validating-utf-8-bytes-using-only-0-45-cycles-per-byte-avx-edition/

AVX2 (256-bit) registers require Intel Haswell or above. This is guaranteed in
a node pool on us-central1-b, because Haswell is the zone's minimum.
