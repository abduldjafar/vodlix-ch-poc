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
3. this is for body example request insert datas`
	{
		"tb_name":"uji",
		"db_name":"testing",
		"datas":{
			"name":"Abdul",
			"address":"H Taha",
			"age":32
		}
	}
`. field `datas` is dynamic.
4. this is for body example alter table `{
	"tb_name":"uji",
	"db_name":"testing",
	"column_name":"asoi",
	"operation_type":"DELETE",
	"data_type":"String"
}`. cuurrently the operation type just ADD and DELETE. if choose DELETE operation, the `data_type` field can be empty.

