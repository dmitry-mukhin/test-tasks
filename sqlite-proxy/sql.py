"""sql.py"""

from functools import wraps
import sqlite3
import re
import datetime

def subst(x, p):
    """Substitute x value and add param to p if necessary.

    arguments:
    x - value to substitute
    p - SqlParamContainer

    returns:
    substituted value

    """
    if isinstance(x, DbField) or isinstance(x, SqlParam):
        return x.name
    if hasattr(x, '__iter__'):
        return u'(%s)' % ','.join([subst(i, p) for i in x])
    if isinstance(x, DbField) or isinstance(x, SqlClause):
        return x
    return p.add(x)

def check_cmd(change_last_cmd=True):
    """Check SQL command order. Raises SqlException.

    arguments:
    change_last_cmd - if true, update last command

    returns:
    decorator

    """
    def decorator(f):
        @wraps(f)
        def wrapper(self, *args, **kw):
            cmd = f.__name__
            e = SqlException(
                u'Wrong command order: %s().%s()' % (self.last_cmd, cmd))
            if cmd in self.cmd_order[self.last_cmd]:
                if change_last_cmd:
                    self.last_cmd = cmd
                return f(self, *args, **kw)
            raise e
        return wrapper
    return decorator

def check_args(*types):
    """Check argument types. Raises SqlException.

    arguments:
    types - types to check args against

    returns:
    decorator

    """
    def decorator(f):
        @wraps(f)
        def wrapper(self, *args):
            for arg, _type in zip(args, types):
                if isinstance(arg, _type):
                    return f(self, *args)
                msg = 'Expecting "%s" instance, got "%s"' % (
                    _type.__name__, arg.__class__.__name__)
                raise SqlException(msg)
        return wrapper
    return decorator


class SqlException(Exception):
    """Simple exception class."""
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return u'%s' % self.msg


class DbField:
    """Represent database table field."""
    def __init__(self, table_name, name, _type):
        self.type = _type
        self.name = name
        self.table_name = table_name

    def __repr__(self):
        return self.full_name()

    def full_name(self):
        return u'%s.%s' % (self.table_name, self.name)


class DbTable:
    """Represent database table."""
    def __init__(self, name, sql):
        """DbTable constructor.

        arguments:
        name - table name
        sql - create table statement

        """
        self._name = name
        for (name, _type) in re.findall(r'\[(\w+)\] (\w+)', sql):
            field = DbField(self._name, name, _type)
            setattr(self, name, field)


class Db:
    """Represent database."""
    def __init__(self, path=':memory:', con=None):
        """Db constructor.

        arguments:
        path - path to database
        con - existing connection, if set path is ignored

        """
        if con is not None and isinstance(con, sqlite3.Connection):
            self.con = con
        else:
            self.con = sqlite3.connect(path)
        self.con.row_factory = sqlite3.Row
        self.cur = self.con.cursor()
        self.tables = {}

        self.cur.execute(
            'select name, sql from sqlite_master where type="table"')
        tables = self.cur.fetchall()
        for (name, sql) in tables:
            table = DbTable(name, sql)
            setattr(self, name, table)
            self.tables[name] = table

    def iresults(self, sql, params=None):
        """Execute sql, return results iterator.

        arguments:
        sql - sql statement to execute
        params - sql statement parameters

        returns:
        results iterator
        """
        def gen():
            self.cur.execute(sql, params)
            while True:
                # can be rewritten with fetchmany() for performance
                row = self.cur.fetchone()
                if row is None:
                    raise StopIteration
                yield SqlRow(row)
        return gen()

    def results(self, sql, params=None):
        """Execute sql, return results.

        arguments:
        sql - sql statement to execute
        params - sql statement parameters

        returns:
        rows - list of SqlRows that represent query results
        """
        self.cur.execute(sql, params)
        rows = [SqlRow(row) for row in self.cur.fetchall()]
        return rows

    def commit(self, sql, params={}):
        """Execute sql, commit changes to database.

        arguments:
        sql - sql statement to execute
        params - sql statement parameters

        returns:
        count - number of changed rows
        """
        count = self.cur.execute(sql, params).rowcount
        self.con.commit()
        return count


class SqlRow:
    """SqlRow represents query result row"""
    @check_args(sqlite3.Row)
    def __init__(self, row):
        """SqlRow constructor.

        arguments:
        row - sqlite3.Row to get data from
        """
        self.__row = row
        self.__dict = {}
        for key in row.keys():
            setattr(self, key, row[key])
            self.__dict[key] = row[key]

    def __repr__(self):
        return u'%s' % self.__dict


class SqlClause:
    """SqlClause is used to construct clause expressions."""
    cmp_ops = ['<', '<=', '>', '=>', '=', '==', '!=', '<>',]
    oth_ops = ['AND', 'OR', 'IN',]
    all_ops = cmp_ops + oth_ops

    def __init__(self, left, op, right, p):
        """SqlClause constructor.

        Constructs sql clause statement, updates SqlParamContainer,
        raises SqlException on errors.

        arguments:
        left - left operand
        op - operator
        right - right operand
        p - SqlParamContainer

        """
        op = op.upper()
        if op not in self.all_ops:
            raise SqlException('Unsupported operator: "%s"' % op)
        if (op == 'IN' and
            not hasattr(right, '__iter__') and
            not isinstance(right, SqlParam)):
            raise SqlException('Expecting iterable, got "%s"' % type(right))
        elif op in self.cmp_ops:
            self.check(left, right)

        left = subst(left, p)
        right = subst(right, p)

        self.sql = u'(%s %s %s)' % (left, op, right)

    def __repr__(self):
        return u'%s' % self.sql

    @staticmethod
    def check(left, right):
        """Check operands of compare operations.

        Raises SqlException on check failure.

        arguments:
        left - left operand
        right - right operand

        """
        def check_type(t, v):
            if isinstance(v, DbField) or isinstance(v, SqlParam):
                return
            if (t == 'VARCHAR' and
               (isinstance(v, str) or isinstance(v, unicode))):
                return
            if t == 'INTEGER' and isinstance(v, int):
                return
            if t == 'TIMESTAMP' and ( isinstance(v, datetime.datetime) or
                isinstance(v, datetime.date) ):
                return
            raise SqlException(
                'Type check failed %s cmp %s' % (t, v.__class__.__name__))
        if isinstance(left, DbField):
            check_type(left.type, right)
        if isinstance(right, DbField):
            check_type(right.type, left)


class SqlBuilder:
    """Builds and executes sql statements."""

    # command order {'prev': ['next', ...]}
    cmd_order = {
        '': ['Select', 'Delete', 'Update', 'InsertInto'],
        'Select': ['From'],
        'From': ['Where', 'FetchFrom', 'IFetchFrom', 'Join'],
        'Delete': ['From'],
        'Where': ['FetchFrom', 'CommitTo', 'And', 'Or'],
        'And': ['And', 'Or', 'FetchFrom', 'CommitTo'],
        'Or': ['And', 'Or', 'FetchFrom', 'CommitTo'],
        'InsertInto': ['Columns'],
        'Columns': ['Values'],
        'Values': ['CommitTo'],
        'Update': ['Set'],
        'Set': ['Where'],
        'Join': ['FetchFrom', 'Where']
    }

    def __init__(self):
        self.sql = u''
        self.last_cmd = ''
        self.params = SqlParamContainer()

    @check_cmd()
    def Select(self, *args):
        """Select command.

        arguments:
        args - fields to select, can be * or list of DbField instances

        returns:
        SqlBuilder instance
        """
        if args[0] == '*':
            self.sql = u'SELECT *'
            return self
        fields = []
        for f in args:
            if not isinstance(f, DbField):
                raise SqlException('Expecting "DbField" instance, got "%s"' % (
                    f.__class__.__name__))
            fields.append(u'%s' % f)
        self.sql += u'SELECT %s' % ', '.join(fields)
        return self

    @check_cmd()
    def Delete(self):
        """Delete command.

        returns:
        SqlBuilder instance
        """
        self.sql = u'DELETE'
        return self

    @check_cmd()
    @check_args(DbTable)
    def From(self, table):
        """From command.

        arguments:
        table - DbTable to select or delete from

        returns:
        SqlBuilder instance
        """
        self.sql += u' FROM %s' % table._name
        return self

    @check_cmd()
    @check_args(DbTable)
    def InsertInto(self, table):
        """Insert command.

        arguments:
        table - DbTable to insert to

        returns:
        SqlBuilder instance
        """
        self.sql += u'INSERT INTO %s' % table._name
        return self

    @check_cmd()
    @check_args(DbTable)
    def Update(self, table):
        """Update command.

        arguments:
        table - DbTable to update

        returns:
        SqlBuilder instance
        """
        self.sql += u'UPDATE %s' % table._name
        return self

    @check_cmd()
    def Set(self, *args):
        """Set command for UPDATE statement.

        arguments:
        args - list of (DbField, value) tuples

        returns:
        SqlBuilder instance
        """
        self.sql += ' SET'
        sets = []
        for (f, v) in args:
            if not isinstance(f, DbField):
                raise SqlException('Expecting "DbField" instance, got "%s"' % (
                f.__class__.__name__))
            sets.append(u' %s = %s' % (f.name, subst(v, self.params)))
        self.sql += ', '.join(sets)
        return self

    @check_cmd()
    def Columns(self, *args):
        """Columns command for INSERT statement.

        arguments:
        args - list of DbFields

        returns:
        SqlBuilder instance
        """
        fields = []
        for f in args:
            if isinstance(f, DbField):
                fields.append(f.name)
            else:
                raise SqlException('Expecting "DbField" instance, got "%s"' % (
                f.__class__.__name__))
        self.sql += u' (%s)' % ', '.join(fields)
        return self

    @check_cmd()
    def Values(self, *args):
        """Values command for INSERT statement.

        arguments:
        args - list of values

        returns:
        SqlBuilder instance

        """
        values = [subst(v, self.params) for v in args]
        self.sql += u' VALUES (%s)' % ', '.join(values)
        return self

    def Clause(self, left, op, right):
        """Create SQL clause for complex statements.

        arguments:
        left - left operand
        op - operator
        right - right operand

        returns:
        SqlClause instance

        usage:
        q = SqlBuilder()
        c = q.Clause
        q = q.Select(db.Users.login).From(db.Users).Where(
            c(db.Users.id, '=', 1),
            'OR',
            c(db.Users.id, '=', 3),
        ).FetchFrom(db)

        """
        return SqlClause(left, op, right, self.params)

    @check_cmd()
    def Where(self, left, op, right):
        """Where command.

        arguments:
        left - left operand
        op - operator
        right - right operand

        returns:
        SqlBuilder instance

        """
        self.sql += u' WHERE %s' % self.Clause(left, op, right)
        return self

    @check_cmd()
    def And(self, left, op, right):
        """And command.

        arguments:
        left - left operand
        op - operator
        right - right operand

        returns:
        SqlBuilder instance

        """
        self.sql += u' AND %s' % self.Clause(left, op, right)
        return self

    @check_cmd()
    def Or(self, left, op, right):
        """Or command.

        arguments:
        left - left operand
        op - operator
        right - right operand

        returns:
        SqlBuilder instance

        """
        self.sql += u' OR %s' % self.Clause(left, op, right)
        return self

    @check_cmd(False)
    @check_args(Db)
    def FetchFrom(self, db):
        """FetchFrom command.

        arguments:
        db - Db to fetch resulst from

        returns:
        list of SqlRow instances

        """
        return db.results(self.sql, self.params.all())

    @check_cmd(False)
    @check_args(Db)
    def IFetchFrom(self, db):
        """IFetchFrom command.

        arguments:
        db - Db to fetch resulst from

        returns:
        iterator over results, yields SqlRow instances

        """
        return db.iresults(self.sql, self.params.all())

    def SetParams(self, params):
        """Set params for SQL statements.

        arguments:
        params - dictionary of param {name: value}

        returns:
        SqlBuilderInstance

        """
        self.params.explicit = params
        return self

    def UpdateParams(self, params):
        """Update params for SQL statements.

        arguments:
        params - dictionary of param {name: value}

        returns:
        SqlBuilderInstance

        """
        self.params.explicit.update(params)
        return self

    @check_cmd(False)
    @check_args(Db)
    def CommitTo(self, db):
        """Execute statement nad commit changed to DB.

        arguments:
        db - Db to commit to

        returns:
        number of affected rows

        """
        return db.commit(self.sql, self.params.all())

    @check_cmd()
    @check_args(DbTable, DbField)
    def Join(self, table, field):
        """Join command.
        Simple JOIN with USING clause

        arguments:
        table - DbTable to join
        field - DbField used in USING clause

        returns:
        SqlBuilderInstance

        """
        self.sql += u' JOIN %s USING (%s)' % (table._name, field.name)
        return self


class SqlParam:
    """SqlParam to use as operand in sql clauses

    usage:
    q = SqlBuilder().Select('*').From(db.Users).Where(
        db.Users.id, '=', SqlParam('id') ).SetParams({'id': 1})
    rows1 = q.FetchFrom(db)
    q.UpdateParams({'id': 2})
    rows2 = q.FetchFrom(db)

    """
    def __init__(self, name):
        self.name = u':%s' % name

    def __repr__(self):
        return self.name


class SqlParamContainer:
    """Contains SQL implicit and explicit (used with SqlParam) statement
    paramters.

    """
    def __init__(self):
        self.counter = 0
        self.implicit = {}
        self.explicit = {}

    def add(self, x):
        """Add x to implicit parameters.

        arguments:
        x - paramter to add

        returns:
        name of parameter to use in SQL statement

        """
        name = u'sqlparam%d' % self.counter
        self.counter += 1
        self.implicit[name] = x
        return u':%s' % name

    def all(self):
        """Combine implicit and explicit parameters to use in execute().

        returns:
        combined param dictionary

        """
        d = {}
        d.update(self.implicit)
        d.update(self.explicit)
        return d
