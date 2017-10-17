# Npgsql.Bulk
Helper for performing COPY (bulk insert and update) operation easily, using Entity Framework + Npgsql. 

.Net 4.5, Standard 1.5 and Standard 2.0 are supported.

```c#
var uploader = new NpgsqlBulkUploader(context);
var data = GetALotOfData();
uploader.Insert(data);
// OR
uploader.Update(data);
```
    

More info at: http://tsherlock.tech/2017/10/11/solving-some-problems-with-bulk-operations-in-npgsql/
