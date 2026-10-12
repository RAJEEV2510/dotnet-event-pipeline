# Build from the repository root: docker build -f src/EventProcessor.Api/Dockerfile .
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY EventProcessor.sln ./
COPY src/ src/
RUN dotnet publish src/EventProcessor.Api/EventProcessor.Api.csproj -c Release -o /app

# aspnet runtime for both images: the worker references Serilog.AspNetCore
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app .
USER app
ENTRYPOINT ["dotnet", "EventProcessor.Api.dll"]
