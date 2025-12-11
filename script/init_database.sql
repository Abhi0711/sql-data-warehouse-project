/*
create Database and schemas
script purpose:
this script creates a new database named "Datawarehouse" after checking if it already exits.*/


use master;
go

--
create database Datawarehouse;

use Datawarehouse;

create schema bronze;
go
create schema silver;
go
create schema gold;
go
