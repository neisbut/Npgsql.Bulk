using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Npgsql.Bulk.Model
{
    internal class InsertQueryParts
    {

        public string TableName { get; set; }

        public string TableNameQualified { get; set; }

        public string TargetColumnNamesQueryPart { get; set; }

        public string SourceColumnNamesQueryPart { get; set; }

        public string Returning { get; set; }

        public string ReturningSetQueryPart { get; set; }

    }
}
