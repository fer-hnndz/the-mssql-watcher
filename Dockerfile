FROM mcr.microsoft.com/mssql/server:2022-latest

ENV ACCEPT_EULA=Y
ENV MSSQL_SA_PASSWORD=freaky_gates123
EXPOSE 1433

ARG name=watcher
ARG hostname=localhost
