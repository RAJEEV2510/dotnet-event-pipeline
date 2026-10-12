# Build from the repository root: docker build -f tools/EventProcessor.LoadGen/Dockerfile .
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY tools/EventProcessor.LoadGen/ tools/EventProcessor.LoadGen/
RUN dotnet publish tools/EventProcessor.LoadGen/EventProcessor.LoadGen.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app
COPY --from=build /app .
USER app
ENTRYPOINT ["dotnet", "EventProcessor.LoadGen.dll"]
