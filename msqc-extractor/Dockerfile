# Image for the msqc-extractor, used by the `docker` job engine.
# Build (from the repo root): ./compose.sh --profile msqc build

FROM python:3.11-slim-bookworm

# alpharaw reads Thermo files through pythonnet. Taking the .NET runtime from the official
# Microsoft image avoids both a mono installation and adding their apt repository.
COPY --from=mcr.microsoft.com/dotnet/runtime:8.0-bookworm-slim /usr/share/dotnet /usr/share/dotnet
ENV DOTNET_ROOT=/usr/share/dotnet
ENV PATH="${DOTNET_ROOT}:${PATH}"

# pythonnet's runtime is process-global and chosen on `import alpharaw`, so it must be set here.
# Note that this rules out reading SCIEX .wiff files, which require `mono` or `netfx`.
ENV ALPHARAW_DOTNET_RUNTIME=coreclr

# libicu72 is the only runtime dependency of .NET 8 that the slim python image lacks (it comes
# preinstalled in the Microsoft image we take the runtime from). Without it, the runtime aborts with
# "Couldn't find a valid ICU package installed on the system".
# git is required to install alpharaw from a commit.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git libicu72 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/
RUN --mount=type=cache,target=/root/.cache/pip pip install -r requirements.txt

COPY main.py entrypoint.sh /app/

ENTRYPOINT ["/app/entrypoint.sh"]
