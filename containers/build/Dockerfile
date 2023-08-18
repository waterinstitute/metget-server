FROM ubuntu:22.04

ARG TARGETPLATFORM
ARG TARGETARCH
SHELL ["/bin/bash", "-c"]
WORKDIR /app

#...Basic software installation
RUN apt-get update && apt-get install -y ca-certificates python3 python3-pip \
    openssl build-essential swig cmake libnetcdf-dev git libproj-dev libgmp-dev \
    libboost-iostreams-dev libboost-system-dev libpq-dev libeccodes-dev && rm -rf /var/lib/apt/lists/*

#...Installation of the metbuild c++ library
#...Note that we do a little tuning for the amd64 chips. We'll assume
# that metget is always running on a modern-ish system with the avx2
# instruction set available. The aarm64 chips are a little
# trickier so we'll just let gcc do its best with the generic flag
RUN git clone https://github.com/waterinstitute/metget-server; \
    mkdir /app/metget-server/libraries/libmetbuild/build; \
    cd /app/metget-server/libraries/libmetbuild/build; \
    echo "Building for ${TARGETARCH}"; \
    if [[ "x${TARGETARCH}" == "xamd64" ]] ; then \
        COPTFLAGS="-march=core-avx2 -mtune=core-avx2"; \
    else \
        COPTFLAGS="-mtune=generic"; \
    fi; \
    cmake .. -DCMAKE_C_COMPILER=gcc \
             -DCMAKE_CXX_COMPILER=g++ \
             -DCMAKE_BUILD_TYPE=Release \
             -DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG ${COPTFLAGS}" \
             -DCMAKE_C_FLAGS_RELEASE="-O3 -DNDEBUG ${COPTFLAGS}"; \
    make -j2 ; \
    make install; \
    cd /app; \
    rm -rf /app/metget-server;

#...Install some of the python dependencies from the requirements.txt file
COPY containers/build/requirements.txt /tmp/
RUN pip install --requirement /tmp/requirements.txt && rm -rf /tmp/requirements.txt

#...Install the metbuild python library
COPY libraries/metbuild /tmp/metbuild
RUN pip install /tmp/metbuild/. && rm -rf /tmp/metbuild

#...Install the metget-build entrypoint script(s)
COPY containers/build/*.py /app/ 
