FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /build
# bump to force a fresh clone of the dev and SkyFilter repos (layer cache otherwise reuses an old clone)
ARG CACHEBUST=20261031
RUN git init dev && git -C dev remote add origin https://github.com/Coflnet/HypixelSkyblock.git && git -C dev fetch --depth=1 origin 2df0f7c7a34c8c04653e4cf7c02ab6805876eb7a && git -C dev checkout --detach FETCH_HEAD
RUN git clone --depth=1 https://github.com/Coflnet/SkyFilter.git
WORKDIR /build/sky
COPY SkyAuctions.csproj SkyAuctions.csproj
RUN dotnet restore
COPY . .
RUN dotnet publish -c release -o /app /p:UseAppHost=false /p:PublishReadyToRun=true

FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled-extra
WORKDIR /app

COPY --from=build --chown=$APP_UID:$APP_UID /app .

ENV ASPNETCORE_URLS=http://+:8000 \
    DOTNET_EnableDiagnostics=0 \
    COMPlus_EnableDiagnostics=0 \
    DOTNET_RUNNING_IN_CONTAINER=true \
    HOME=/tmp \
    TMPDIR=/tmp

USER $APP_UID

ENTRYPOINT ["dotnet", "SkyAuctions.dll", "--hostBuilder:reloadConfigOnChange=false"]
