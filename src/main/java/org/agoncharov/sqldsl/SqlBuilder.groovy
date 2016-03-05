package org.agoncharov.sqldsl


class SqlBuilder {

    private static class Builder {

        private Object[] selectFields
        private Object[] fromTables
        private List<Restriction> whereRestrictions = []
        private List<Join> joins = []
        private Object[] groupFields
        private Map<String, OrderingDirection> orderings
        private List<Tuple2<String, String>> conjunctions = []

        @Override
        String toString() {
            StringBuilder result = new StringBuilder("SELECT ${selectFields.join(', ')}")
            if (fromTables) {
                result << " FROM ${fromTables.join(', ')}"
            }
            joins.each {
                result << " $it.type JOIN $it.table ON ${it.restrictions.join(' ')}"
            }
            if (whereRestrictions) {
                result << " WHERE ${whereRestrictions.join(' ')}"
            }
            if (groupFields) {
                result << " GROUP BY ${groupFields.join(', ')}"
            }
            if (orderings) {
                result << " ORDER BY ${orderings.collect { "$it.key $it.value" }.join(', ')}"
            }
            conjunctions.each {
                result << " $it.first $it.second"
            }
            return result.toString()
        }
    }

    trait WithBuilder {

        Builder builder

        WithBuilder union(Closure<WithBuilder> subQueryClosure) {
            builder.conjunctions << new Tuple2<String, String>('UNION', query(subQueryClosure))
            return this
        }

        WithBuilder intersect(Closure<WithBuilder> subQueryClosure) {
            builder.conjunctions << new Tuple2<String, String>('INTERSECT', query(subQueryClosure))
            return this
        }
    }

    private class Restriction {

        private Object left, right
        private String operator
        private Boolean isAnd

        @Override
        String toString() {
            if (right == null && operator == '=') {
                return "${isAnd != null ? (isAnd ? 'AND ' : 'OR ') : ''}$left IS NULL"
            }
            return "${isAnd != null ? (isAnd ? 'AND ' : 'OR ') : ''}$left $operator $right"
        }
    }

    class Select implements WithBuilder {

        From from(Object... tables) {
            builder.fromTables = tables
            return new From(builder: builder)
        }

        From from(Map<String, Object> tables) {
            builder.fromTables = tables.collect {
                "$it.key $it.value"
            }
            return new From(builder: builder)
        }
    }

    class From implements Groupable {

        Where where(Object field) {
            return new Where(field: field, builder: builder)
        }

        Join join(Object table) {
            return new Join(table: table, type: 'INNER', builder: builder)
        }

        PreJoin left(JoinType joinType) {
            return new PreJoin(builder: builder, type: 'LEFT OUTER')
        }

        PreJoin right(JoinType joinType) {
            return new PreJoin(builder: builder, type: 'RIGHT OUTER')
        }

        PreJoin full(JoinType joinType) {
            return new PreJoin(builder: builder, type: 'FULL OUTER')
        }

        Join join(Map<String, Object> tables) {
            String table = tables.collect {
                "$it.key $it.value"
            }.join ', '
            return new Join(table: table, type: 'INNER', builder: builder)
        }
    }

    static class JoinType {
        final static JoinType OUTER = new JoinType()
    }

    JoinType getOuter() {
        return JoinType.OUTER
    }

    class PreJoin {

        String type
        Builder builder

        Join join(Object table) {
            return new Join(table: table, type: type, builder: builder)
        }

        Join join(Map<String, Object> tables) {
            String table = tables.collect {
                "$it.key $it.value"
            }.join ', '
            return new Join(table: table, type: type, builder: builder)
        }
    }

    class Join {

        private Builder builder
        private Object table
        private String type
        private List<Restriction> restrictions = []

        On on(Object field) {
            builder.joins << this
            return new On(field: field, join: this, builder: builder)
        }
    }

    class On {

        private Builder builder
        private Object field
        private Join join
        private Boolean isAnd

        OnCondOperator eq(Object value) {
            join.restrictions << new Restriction(left: field, right: value, operator: '=', isAnd: isAnd)
            return new OnCondOperator(builder: builder, join: join)
        }
    }

    class OnCondOperator extends From {

        Join join

        On and(Object field) {
            return new On(builder: builder, join: join, field: field, isAnd: true)
        }

        On or(Object field) {
            return new On(builder: builder, join: join, field: field, isAnd: false)
        }
    }

    class Where {

        private Builder builder
        private Object field
        private Boolean isAnd

        CondOperator eq(Object value) {
            return handle('=', value)
        }

        CondOperator ne(Object value) {
            return handle('<>', value)
        }

        CondOperator lt(Object value) {
            return handle('<', value)
        }

        CondOperator gt(Object value) {
            return handle('>', value)
        }

        CondOperator ge(Object value) {
            return handle('>=', value)
        }

        CondOperator le(Object value) {
            return handle('<=', value)
        }

        CondOperator in_(Closure<WithBuilder> subQueryClosure) {
            def subQuery = query(subQueryClosure)
            return handle('IN', new Term(name: "(${subQuery})"))
        }

        CondOperator in_(Object... values) {
            return handle('IN', new Term(name: "(${values.join(', ')})"))
        }

        CondOperator handle(String operator, Object right) {
            builder.whereRestrictions << new Restriction(
                    operator: operator, right: right,
                    left: field,
                    isAnd: isAnd)
            return new CondOperator(builder: builder)
        }
    }

    class CondOperator implements Orderable {

        Where and(Object field) {
            return new Where(builder: builder, field: field, isAnd: true)
        }

        Where or(Object field) {
            return new Where(builder: builder, field: field, isAnd: false)
        }
    }

    trait Groupable extends WithBuilder {

        GroupBy groupBy(Object... fields) {
            builder.groupFields = fields
            def groupBy = new GroupBy()
            groupBy.builder = builder
            return groupBy
        }
    }

    trait Orderable extends Groupable {

        OrderBy orderBy(Map<String, OrderingDirection> fields) {
            builder.orderings = fields

            def orderBy = new OrderBy()
            orderBy.builder = builder
            return orderBy
        }

        OrderBy orderBy(Object... fields) {
            Map<String, OrderingDirection> orderedFields = fields.collectEntries { [it, OrderingDirection.ASC] }
            return orderBy(orderedFields)
        }
    }

    class GroupBy implements Orderable {
    }

    class OrderBy implements WithBuilder {
    }

    OrderingDirection getAsc() {
        return OrderingDirection.ASC
    }

    OrderingDirection getDesc() {
        return OrderingDirection.DESC
    }

    enum OrderingDirection {
        ASC,
        DESC
    }

    class Term {

        private String name

        @Override
        String toString() {
            return name
        }

        String rightShift(Object alias) {
            return "$name $alias"
        }

        Term getProperty(String propertyName) {
            return new Term(name: "${name}.${propertyName}")
        }

    }

    Term get(String propertyName) {
        return new Term(name: propertyName)
    }

    Select select(Object... fields) {
        return new Select(builder: new Builder(selectFields: fields))
    }

    Select select(Map<String, Object> fields) {
        def aliasedFields = fields.collect {
            "$it.key $it.value"
        }
        return new Select(builder: new Builder(selectFields: aliasedFields))
    }

    String count(Object field) {
        return "COUNT($field)"
    }

    String avg(Object field) {
        return "AVG($field)"
    }

    static String query(
            @DelegatesTo(value = SqlBuilder, strategy = Closure.DELEGATE_FIRST) Closure<WithBuilder> closure) {
        closure = closure.rehydrate(new SqlBuilder(), this, this)
        closure.resolveStrategy = Closure.DELEGATE_FIRST
        return closure().builder.toString()
    }
}
