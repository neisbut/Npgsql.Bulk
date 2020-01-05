using Npgsql.Bulk.Model;
using System;
using System.Collections.Generic;
using System.Data;
#if EFCore
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.EntityFrameworkCore.Storage;
#else
using System.Data.Entity;
#endif
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Npgsql.Bulk
{
    internal interface IRelationalHelper
    {
        NpgsqlConnection GetNpgsqlConnection(DbContext context);

#if EFCore
        IDbContextTransaction EnsureOrStartTransaction(DbContext context, IsolationLevel defaultIsolationLevel);
#else
        DbContextTransaction EnsureOrStartTransaction(DbContext context, IsolationLevel defaultIsolationLevel);
#endif

        List<ColumnInfo> GetColumnsInfo(DbContext context, string tableName);
    }
}
