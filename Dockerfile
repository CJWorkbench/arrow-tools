FROM debian:bullseye AS cpp-builddeps

# DEBUG SYMBOLS: to build with debug symbols (which help gdb), run
# `docker build --build-arg CMAKE_BUILD_TYPE=Debug ...`
#
# We install libstdc++6-10-dbg, gdb and time regardless. They don't affect final
# image size in Release mode.
ARG CMAKE_BUILD_TYPE=Release

# We build Arrow ourselves instead of using precompiled binaries. Two reasons:
#
# 1. File size. These statically-linked executables get copied and run a lot.
#    Smaller files mean faster deploys (and marginally-faster start time).
# 2. Dev experience. With --build-arg CMAKE_BUILD_TYPE=Debug, we can help this
#    package's maintainer get a useful stack trace sooner.

RUN true \
      && apt-get update \
      && apt-get install -y --no-install-recommends \
          build-essential \
          ca-certificates \
          cmake \
          curl \
          gdb \
          libc-dbg \
          libdouble-conversion-dev \
          libgflags-dev \
          libstdc++6-10-dbg \
          tar \
          time

RUN true \
      && mkdir -p /src \
      && cd /src \
      && curl -L "http://www.apache.org/dyn/closer.lua?filename=arrow/arrow-3.0.0/apache-arrow-3.0.0.tar.gz&action=download" | tar xz

# arrow
RUN true \
      && cd /src/apache-arrow-3.0.0/cpp \
      && cmake \
          -DARROW_COMPUTE=ON \
          -DARROW_WITH_UTF8PROC=ON \
          -DARROW_WITH_RE2=OFF \
          -DARROW_OPTIONAL_INSTALL=ON \
          -DARROW_BUILD_STATIC=ON \
          -DARROW_BUILD_SHARED=OFF \
          -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE . \
      && make -j4 arrow arrow_bundled_dependencies \
      && make install

# xlnt
COPY patches/ /src/patches/
RUN true \
      && cd /src \
      && curl --location https://github.com/tfussell/xlnt/archive/v1.5.0.tar.gz | tar zx \
      && cd xlnt-* \
      && find /src/patches/xlnt/* -exec patch -p1 -i {} \; \
      && cmake \
        -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE \
        -DCMAKE_DEBUG_POSTFIX= \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DSTATIC=ON \
        -DTESTS=OFF \
      && make -j4 \
      && make install

# libxls
RUN true \
      && cd /src \
      && curl --location https://github.com/libxls/libxls/releases/download/v1.6.2/libxls-1.6.2.tar.gz | tar zx \
      && cd libxls-* \
      && ./configure --prefix=/usr --enable-static=yes --enable-shared=no \
      && make -j4 \
      && make install


# Old version of python lets us use an old version of pyarrow
FROM python:3.8.7-buster AS python-dev

# Old version of pyarrow lets us build an invalid buffer in test_check_safe_string_array
RUN pip install pyarrow==0.16.0 pytest pandas==1.2.3 openpyxl==3.0.6 xlwt==1.3.0

RUN mkdir /app
WORKDIR /app


FROM cpp-builddeps AS cpp-build

RUN mkdir -p /app/src
COPY vendor/ /app/vendor/
RUN touch \
  /app/src/arrow-validate.cc \
  /app/src/column-builder.cc \
  /app/src/common.cc \
  /app/src/csv-to-arrow.cc \
  /app/src/excel-table-builder.cc \
  /app/src/json-table-builder.cc \
  /app/src/json-to-arrow.cc \
  /app/src/json-warnings.cc \
  /app/src/xls-to-arrow.cc \
  /app/src/xlsx-to-arrow.cc

WORKDIR /app
COPY CMakeLists.txt /app
# Redeclare CMAKE_BUILD_TYPE: its scope is its build stage
ARG CMAKE_BUILD_TYPE=Release
RUN cmake -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE .

COPY src/ /app/src/
RUN VERBOSE=true make -j4 install/strip
# Display size. In v2.1, it's ~7MB per executable.
RUN ls -lh /usr/bin/*arrow*


FROM python-dev AS test

COPY --from=cpp-build /usr/bin/*arrow* /usr/bin/
COPY tests/ /app/tests/
WORKDIR /app
RUN pytest -vv -s


FROM scratch AS dist
COPY --from=cpp-build /usr/bin/*arrow* /usr/bin/
