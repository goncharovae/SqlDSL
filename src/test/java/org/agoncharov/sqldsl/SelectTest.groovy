package org.agoncharov.sqldsl

import groovy.transform.CompileStatic

import static org.agoncharov.sqldsl.SqlBuilder.query
import org.junit.Test
import static org.junit.Assert.*


class SelectTest {

    @Test
    void basic() {
        String query = query {
            select id, name from users where email eq "'example@email.com'"
        }
        String expected = "SELECT id, name FROM users WHERE email = 'example@email.com'"
        assertEquals expected, query
    }

    @Test
    @CompileStatic
    void compileStatic() {
        String query = query {
            select 'id', 'name' from 'users' where 'email' eq "'example@email.com'"
        }
        String expected = "SELECT id, name FROM users WHERE email = 'example@email.com'"
        assertEquals expected, query
    }

    @Test
    void multipleConditions() {
        String query = query {
            select '*' from users where name ne "'alex'" and project eq "'TST'" or id eq null
        }
        String expected = "SELECT * FROM users WHERE name <> 'alex' AND project = 'TST' OR id IS NULL"
        assertEquals expected, query
    }

    @Test
    void joining() {
        String query = query {
            select u.'*' from users: u join projects: p on u.project_id eq p.id
        }
        String expected = "SELECT u.* FROM users u INNER JOIN projects p ON u.project_id = p.id"
        assertEquals expected, query
    }

    @Test
    void joiningOnMultiple() {
        String query = query {
            select u.id from users: u    \
                join projects: p on u.project_id eq p.id and u.group_id eq p.group_id
        }
        String expected = "SELECT u.id FROM users u " +
                "INNER JOIN projects p ON u.project_id = p.id AND u.group_id = p.group_id"
        assertEquals expected, query
    }

    @Test
    void joinMultiple() {
        String query = query {
            select u.'*' from users: u    \
                join projects: p on u.project_id eq p.id    \
                join groups: g on g.id eq u.group_id
        }
        String expected = 'SELECT u.* FROM users u' +
                ' INNER JOIN projects p ON u.project_id = p.id' +
                ' INNER JOIN groups g ON g.id = u.group_id'
        assertEquals expected, query
    }

    @Test
    void outerJoins() {
        String query = query {
            select u.id from users: u    \
                left outer join projects: p on u.project_id eq p.id    \
                right outer join groups: g on g.id eq u.group_id    \
                full outer join roles: r on r.id eq g.id
        }
        String expected = 'SELECT u.id FROM users u' +
                ' LEFT OUTER JOIN projects p ON u.project_id = p.id' +
                ' RIGHT OUTER JOIN groups g ON g.id = u.group_id' +
                ' FULL OUTER JOIN roles r ON r.id = g.id'
        assertEquals expected, query
    }

    @Test
    void aggregate() {
        String query = query {
            select count('*'), avg(score) from users
        }
        String expected = "SELECT COUNT(*), AVG(score) FROM users"
        assertEquals expected, query
    }

    @Test
    void aggregateWithAliasing() {
        String query = query {
            select group_id: group, "${count('*')}": count from users groupBy group_id
        }
        String expected = "SELECT group_id group, COUNT(*) count FROM users GROUP BY group_id"
        assertEquals expected, query
    }

    @Test
    void inSubQuery() {
        String query = query {
            select '*' from users where project_id in_ { select id from projects }
        }
        String expected = "SELECT * FROM users WHERE project_id IN (SELECT id FROM projects)"
        assertEquals expected, query
    }

    @Test
    void orderBy() {
        String query = query {
            select '*' from users where group_id eq 5 groupBy project_id orderBy name
        }
        String expected = 'SELECT * FROM users WHERE group_id = 5 GROUP BY project_id ORDER BY name ASC'
        assertEquals expected, query
    }

    @Test
    void unionAndIntersect() {
        String query = query {
            select id, name from users \
               union { select id, name from groups } \
               intersect { select '*' from roles }
        }
        String expected = 'SELECT id, name FROM users' +
                ' UNION SELECT id, name FROM groups' +
                ' INTERSECT SELECT * FROM roles'
        assertEquals expected, query
    }

    @Test
    void inOperator() {
        String query = query {
            select '*' from users where id in_ (1, 2, 3, 4, 5) and project_id eq 1 orderBy date_created:desc
        }
        String expected = 'SELECT * FROM users WHERE id IN (1, 2, 3, 4, 5)' +
                ' AND project_id = 1 ORDER BY date_created DESC'
        assertEquals expected, query
    }
}
