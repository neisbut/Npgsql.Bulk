using Npgsql.Bulk.Model;
using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Npgsql.Bulk
{
    public class InsertConflictAction
    {
        /// <summary>
        /// DO NOTHING conflict action
        /// </summary>
        public static InsertConflictAction DoNothing() => new InsertConflictAction() { doUpdate = false };

        /// <summary>
        /// DO NOTHING ( sql )
        /// </summary>
        /// <param name="conflictTargetSql"></param>
        /// <returns></returns>
        public static InsertConflictAction DoNothing(string conflictTargetSql) => new InsertConflictAction()
        {
            doUpdate = false,
            conflictTarget = conflictTargetSql
        };

        public static InsertConflictAction DoNothingColumn(string columnName) => DoNothing($"({columnName})");

        public static InsertConflictAction DoNothingIndex(string indexName) => DoNothing($"({indexName})");

        public static InsertConflictAction DoNothingConstraint(string constraint) => DoNothing($"ON CONSTRAINT {constraint}");

        public static InsertConflictAction Update<T>(string conflictTargetSql, params Expression<Func<T, object>>[] updateProperties) => new InsertConflictAction()
        {
            doUpdate = true,
            conflictTarget = conflictTargetSql,
            updateProperties = updateProperties
        };

        public static InsertConflictAction UpdateProperty<T>(Expression<Func<T, object>> conflictProperty, params Expression<Func<T, object>>[] updateProperties) => new InsertConflictAction()
        {
            doUpdate = true,
            conflictProperty = conflictProperty,
            updateProperties = updateProperties
        };

        public static InsertConflictAction UpdateColumn<T>(string columnName, params Expression<Func<T, object>>[] updateProperties) => Update($"({columnName})", updateProperties);

        public static InsertConflictAction UpdateIndex<T>(string indexName, params Expression<Func<T, object>>[] updateProperties) => Update($"({indexName})", updateProperties);

        public static InsertConflictAction UpdateConstraint<T>(string constraint, params Expression<Func<T, object>>[] updateProperties) => Update($"ON CONSTRAINT {constraint}", updateProperties);

        bool doUpdate;
        string conflictTarget;
        LambdaExpression conflictProperty;
        LambdaExpression[] updateProperties;

        /// <summary>
        /// ctor for DO NOTHING case
        /// </summary>
        /// <param name="_"></param>
        private InsertConflictAction()
        {
        }

        PropertyInfo UnwrapProperty(LambdaExpression expression)
        {
            var propExpression = expression.Body;
            while (propExpression.GetType() == typeof(UnaryExpression))
                propExpression = ((UnaryExpression)propExpression).Operand;

            if (propExpression.NodeType != ExpressionType.MemberAccess)
                throw new InvalidOperationException($"MemberAccess was expected, but got: '{expression}'");
            var updateMember = ((MemberExpression)propExpression).Member;
            if (updateMember.MemberType != MemberTypes.Property)
                throw new InvalidOperationException($"Property was expected, but got: '{updateMember.MemberType}'");
            return (PropertyInfo)updateMember;
        }

        internal object GetSql(EntityInfo mapping)
        {
            string conflictAction;

            if (!doUpdate)
            {
                conflictAction = "DO NOTHING";
            }
            else
            {
                if (updateProperties == null)
                    throw new ArgumentNullException(nameof(updateProperties));
                if (updateProperties.Length == 0)
                    throw new InvalidOperationException("Update properties list update can't be empty");

                if (conflictProperty != null)
                {
                    if (!mapping.PropToMappingInfo.TryGetValue(UnwrapProperty(conflictProperty), out MappingInfo info))
                        throw new InvalidOperationException($"Can't find property {conflictProperty} in mapping");

                    conflictTarget = $"({NpgsqlHelper.GetQualifiedName(info.ColumnInfo.ColumnName)})";
                }

                var sb = new StringBuilder(100);
                sb.Append("DO UPDATE SET ");
                foreach (var expr in updateProperties)
                {
                    var updateProp = UnwrapProperty(expr);

                    if (!mapping.PropToMappingInfo.TryGetValue(updateProp, out MappingInfo info))
                        throw new InvalidOperationException($"Can't find property {updateProp} in mapping");

                    sb.Append(NpgsqlHelper.GetQualifiedName(info.ColumnInfo.ColumnName));
                    sb.Append(" = EXCLUDED.");
                    sb.Append(NpgsqlHelper.GetQualifiedName(info.ColumnInfo.ColumnName));
                    sb.Append(", ");
                }

                sb.Length -= 2;
                conflictAction = sb.ToString();
            }

            return $"ON CONFLICT {conflictTarget} {conflictAction}";
        }
    }
}
