# Npgsql.Bulk
A sample code for EF6 + Npgsql + Bulk INSERT\UPDATE

```c#
var uploader = new NpgsqlBulkUploader(context);
var data = GetALotOfData();
uploader.Insert(data);
// OR
uploader.Update(data);
```
    

More info at: http://tsherlock.tech/2017/10/11/solving-some-problems-with-bulk-operations-in-npgsql/
