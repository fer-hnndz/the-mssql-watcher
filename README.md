# Comandos para levantar SQL Server
```bash
$ docker image build . -t mssql-watcher:1
$ docker container run -p 1433:1433 mssql-watcher:1