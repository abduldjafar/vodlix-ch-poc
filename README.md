# vodlix-ch-poc

1. This service currently runing on top docker compose
2. for runing it type `docker compose up` or `bash run.sh`
3. after the all containers run please run `http://localhost:18000/api/v1/table_ddl_history` in your web browser for generate the ddl history table

## Description
1. the documentation of the api can read in `/docs` path
2. this is for body request create table with custom columns `{
	"tb_name":"asepso",
	"db_name":"asek",
	"columns":{
		"name":"String",
		"address":"String",
		"date":"Int32"
	},
	"order_by":"name"
}
`

