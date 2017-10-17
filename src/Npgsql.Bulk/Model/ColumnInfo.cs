namespace Npgsql.Bulk.Model
{
    internal class ColumnInfo
    {
        public string ColumnName { get; set; }

        public string ColumnType { get; set; }

        public string ColumnTypeExtra { get; set; }

        public bool HasDefault { get; set; }
    }
}
