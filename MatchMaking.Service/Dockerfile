FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY ["MatchMaking.Service/MatchMaking.Service.csproj", "MatchMaking.Service/"]
COPY ["MatchMaking.Shared/MatchMaking.Shared.csproj", "MatchMaking.Shared/"]
RUN dotnet restore "MatchMaking.Service/MatchMaking.Service.csproj"
COPY . .
WORKDIR "/src/MatchMaking.Service"
RUN dotnet build "MatchMaking.Service.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "MatchMaking.Service.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "MatchMaking.Service.dll"]