using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Npgsql.Bulk.Model
{
    internal class UpdateQueryParts
    {
        public string TableName { get; set; }

        public string TableNameQualified { get; set; }

        public string SetClause { get; set; }

        public string WhereClause { get; set; }
    }
}
