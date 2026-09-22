FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git gcc make curl wget \
        iproute2 \
        procps \
        net-tools \
        tree \
        file \
    && rm -rf /var/lib/apt/lists/*

# Python packages baked in — no internet needed at runtime.
# Customize this list for your use case.
RUN pip install --no-cache-dir \
    websockets \
    numpy \
    pandas \
    pyyaml \
    httpx \
    jinja2 \
    tabulate \
    matplotlib

RUN mkdir -p /sandbox /workspace

COPY worker.py /sandbox/worker.py
