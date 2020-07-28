FROM debian:buster AS cpp-builddeps

# DEBUG SYMBOLS: to build with debug symbols (which help gdb), do these
# changes (but don't commit them):
#
# * in Dockerfile, ensure libstdc++6-8-dbg is installed (this, we commit)
# * in Dockerfile, set CMAKE_BUILD_TYPE=Debug here
ENV CMAKE_BUILD_TYPE=Release

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
          libdouble-conversion-dev \
          libgflags-dev \
          libstdc++6-8-dbg \
          pkg-config \
          python \
          tar \
      && true

# arrow
RUN true \
      && mkdir -p /src \
      && cd /src \
      && curl --location http://archive.apache.org/dist/arrow/arrow-0.16.0/apache-arrow-0.16.0.tar.gz | tar zx \
      && cd apache-arrow-0.16.0/cpp \
      && cmake -DARROW_COMPUTE=ON -DARROW_OPTIONAL_INSTALL=ON -DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=OFF -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE . \
      && make -j4 arrow \
      && make install

# xlnt
COPY patches/ /src/patches/
RUN true \
      && cd /src \
      && curl --location https://github.com/tfussell/xlnt/archive/v1.5.0.tar.gz | tar zx \
      && cd xlnt-* \
      && find /src/patches/xlnt/* -exec patch -p1 -i {} \; \
      && cmake -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE -DCMAKE_DEBUG_POSTFIX= -DCMAKE_INSTALL_PREFIX=/usr -DSTATIC=ON -DTESTS=OFF \
      && make -j4 \
      && make install

# libxls
RUN true \
      && cd /src \
      && curl --location https://github.com/libxls/libxls/releases/download/v1.5.2/libxls-1.5.2.tar.gz | tar zx \
      && cd libxls-* \
      && ./configure --prefix=/usr --enable-static=yes --enable-shared=no \
      && make -j4 \
      && make install

ENV PKG_CONFIG_PATH "/src/apache-arrow-0.16.0/cpp/jemalloc_ep-prefix/src/jemalloc_ep/dist/lib/pkgconfig"


FROM python:3.8.1-buster AS python-dev

RUN pip install pyarrow==0.16.0 pytest pandas==1.0.0 openpyxl==3.0.3 xlwt==1.3.0

RUN mkdir /app
WORKDIR /app


FROM cpp-builddeps AS cpp-build

RUN mkdir -p /app/src
COPY vendor/ /app/vendor/
RUN touch /app/src/csv-to-arrow.cc /app/src/json-to-arrow.cc /app/src/xls-to-arrow.cc /app/src/xlsx-to-arrow.cc /app/src/json-warnings.cc /app/src/column-builder.cc /app/src/excel-table-builder.cc /app/src/json-table-builder.cc /app/src/common.cc /app/src/arrow-validate.cc
WORKDIR /app
COPY CMakeLists.txt /app
RUN cmake -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE .

COPY src/ /app/src/
RUN make -j4


FROM python-dev AS test

COPY --from=cpp-build /app/csv-to-arrow /usr/bin/csv-to-arrow
COPY --from=cpp-build /app/json-to-arrow /usr/bin/json-to-arrow
COPY --from=cpp-build /app/xls-to-arrow /usr/bin/xls-to-arrow
COPY --from=cpp-build /app/xlsx-to-arrow /usr/bin/xlsx-to-arrow
COPY --from=cpp-build /app/arrow-validate /usr/bin/arrow-validate
COPY tests/ /app/tests/
WORKDIR /app
RUN pytest -vv


FROM scratch AS dist
COPY --from=cpp-build /app/csv-to-arrow /usr/bin/csv-to-arrow
COPY --from=cpp-build /app/json-to-arrow /usr/bin/json-to-arrow
COPY --from=cpp-build /app/xls-to-arrow /usr/bin/xls-to-arrow
COPY --from=cpp-build /app/xlsx-to-arrow /usr/bin/xlsx-to-arrow
COPY --from=cpp-build /app/arrow-validate /usr/bin/arrow-validate
