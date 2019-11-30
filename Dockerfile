FROM debian:buster AS cpp-builddeps

# DEBUG SYMBOLS: to build with debug symbols (which help gdb), do these
# changes (but don't commit them):
#
# * in Dockerfile, ensure libstdc++6-8-dbg is installed (this, we commit)
# * in Dockerfile, set -DCMAKE_BUILD_TYPE=Debug when building Arrow
# * in Dockerfile, set -DCMAKE_BUILD_TYPE=Debug when building csv-to-arrow

RUN true \
      && apt-get update \
      && apt-get install -y \
          autoconf \
          bison \
          build-essential \
          cmake \
          curl \
          flex \
          g++ \
          gnupg \
          libboost-dev \
          libboost-filesystem-dev \
          libboost-regex-dev \
          libboost-system-dev \
          libstdc++6-8-dbg \
          pkg-config \
          python \
          tar \
      && true

RUN true \
      && mkdir -p /src \
      && cd /src \
      && curl -Oapache-arrow-0.15.1.tar.gz --location http://archive.apache.org/dist/arrow/arrow-0.15.1/apache-arrow-0.15.1.tar.gz \
      && tar zxf apache-arrow-0.15.1.tar.gz \
      && cd apache-arrow-0.15.1/cpp \
      && cmake -DARROW_COMPUTE=ON -DARROW_OPTIONAL_INSTALL=ON -DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=OFF -DCMAKE_BUILD_TYPE=Release . \
      && make -j4 arrow \
      && make install

ENV PKG_CONFIG_PATH "/src/apache-arrow-0.15.1/cpp/jemalloc_ep-prefix/src/jemalloc_ep/dist/lib/pkgconfig:/src/apache-arrow-0.15.1/cpp/gflags_ep-prefix/src/gflags_ep/lib/pkgconfig"
ENV CMAKE_PREFIX_PATH "/src/apache-arrow-0.15.1/cpp/double-conversion_ep/src/double-conversion_ep"


FROM python:3.7.4-buster AS python-dev

RUN pip install pyarrow==0.15.1 pytest pandas==0.25.3

RUN mkdir /app
WORKDIR /app


FROM cpp-builddeps AS cpp-build

RUN mkdir -p /app/src
COPY vendor/ /app/vendor/
RUN touch /app/src/csv-to-arrow.cc /app/src/json-to-arrow.cc /app/src/json-warnings.cc /app/src/json-table-builder.cc /app/src/common.cc
WORKDIR /app
COPY CMakeLists.txt /app
RUN cmake -DCMAKE_BUILD_TYPE=Release .

COPY src/ /app/src/
RUN make -j4


FROM python-dev AS test

COPY --from=cpp-build /app/csv-to-arrow /usr/bin/csv-to-arrow
COPY --from=cpp-build /app/json-to-arrow /usr/bin/json-to-arrow
COPY tests/ /app/tests/
WORKDIR /app
RUN pytest -vv


FROM scratch AS dist
COPY --from=cpp-build /app/csv-to-arrow /usr/bin/csv-to-arrow
COPY --from=cpp-build /app/json-to-arrow /usr/bin/json-to-arrow
