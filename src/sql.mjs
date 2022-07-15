
let NULL = {}
const stringFormat = (s, ...varargs) => {
  let status = 0;
  let res = [];
  let j = -1;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (c === "%") {
      if (status === 0) {
        status = 1;
      } else if (status === 1) {
        status = 0;
        res.push("%");
      }
    } else if (c === "s" && status === 1) {
      j = j + 1;
      res.push(varargs[j]);
      status = 0;
    } else {
      res.push(c);
    }
  }
  return res.join("");
};
function makeRawToken(s) {
  function rawToken() {
    return s;
  }
  return rawToken;
}
let DEFAULT = makeRawToken("DEFAULT");

function _prefixWith_V(column) {
  return "V." + column;
}
function _escapeFactory(isLiteral, isBracket) {
  function asSqlToken(value) {
    let valueType = typeof value;
    if ("string" === valueType) {
      if (isLiteral) {
        return "'" + value.replaceAll("'", "''") + "'";
      } else {
        return value;
      }
    } else if ("number" === valueType) {
      return value;
    } else if ("boolean" === valueType) {
      return (value && "TRUE") || "FALSE";
    } else if ("function" === valueType) {
      return value();
    } else if (NULL === value) {
      return "NULL";
    } else if ("object" === valueType) {
      if (value instanceof Sql) {
        return "(" + value.statement() + ")";
      } else if (value[0] !== undefined) {
        let token = value.map(asSqlToken).join(", ");
        if (isBracket) {
          return "(" + token + ")";
        } else {
          return token;
        }
      } else {
        throw new Error("empty table as a Sql value is not allowed");
      }
    } else {
      throw new Error(
        `don't know how to escape value: ${value} (${valueType})`
      );
    }
  }
  return asSqlToken;
}
let asLiteral = _escapeFactory(true, true);
let asToken = _escapeFactory(false, false);
function getCteReturningValues(opts) {
  let values = [];
  for (let col of opts.columns) {
    values.push(asToken(col));
  }
  if (opts.literals) {
    for (let e of opts.literals) {
      values.push(asLiteral(e));
    }
  }
  return values;
}
function getReturningToken(opts) {
  if (opts.cteReturning) {
    return " RETURNING " + asToken(getCteReturningValues(opts.cteReturning));
  } else if (opts.returning) {
    return " RETURNING " + opts.returning;
  } else {
    return "";
  }
}
function assembleSql(opts) {
  let statement;
  if (opts.update) {
    let from = (opts.from && " FROM " + opts.from) || "";
    let where = (opts.where && " WHERE " + opts.where) || "";
    let returning = getReturningToken(opts);
    statement = `UPDATE ${opts.tableName} SET ${opts.update}${from}${where}${returning}`;
  } else if (opts.insert) {
    let returning = getReturningToken(opts);
    statement = `INSERT INTO ${opts.tableName} ${opts.insert}${returning}`;
  } else if (opts.delete) {
    let using = (opts.using && " USING " + opts.using) || "";
    let where = (opts.where && " WHERE " + opts.where) || "";
    let returning = getReturningToken(opts);
    statement = `DELETE FROM ${opts.tableName} ${using}${where}${returning}`;
  } else {
    let from = opts.from || opts.tableName;
    let where = (opts.where && " WHERE " + opts.where) || "";
    let group = (opts.group && " GROUP BY " + opts.group) || "";
    let having = (opts.having && " HAVING " + opts.having) || "";
    let order = (opts.order && " ORDER BY " + opts.order) || "";
    let limit = (opts.limit && " LIMIT " + opts.limit) || "";
    let offset = (opts.offset && " OFFSET " + opts.offset) || "";
    statement = `SELECT ${(opts.distinct && "DISTINCT ") || ""}${opts.select || "*"
      } FROM ${from}${where}${group}${having}${order}${limit}${offset}`;
  }
  return (opts.with && `WITH ${opts.with} ${statement}`) || statement;
}

class Sql {
  static r = makeRawToken;
  static DEFAULT = DEFAULT;
  static NULL = NULL;
  static asToken = asToken;
  static asLiteral = asLiteral;
  static new(tableName) {
    return new this(tableName)
  }
  constructor(tableName) {
    this.tableName = tableName
  }
  toString() {
    return this.statement()
  };
  pcall() {
    this._pcall = true;
    return this;
  }
  error(errMsg) {
    if (this._pcall) {
      throw new Error(errMsg);
    } else {
      throw new Error(errMsg);
    }
  }
  _getKeys(rows) {
    let columns = [];
    if (rows instanceof Array) {
      let d = [];
      for (let row of rows) {
        for (let k of Object.keys(row)) {
          if (!d[k]) {
            d[k] = true;
            columns.push(k);
          }
        }
      }
    } else {
      for (let k of Object.keys(rows)) {
        columns.push(k);
      }
    }
    return columns;
  }
  _rowsToArray(rows, columns, fallback) {
    let c = columns.length;
    let n = rows.length;
    let res = new Array(n);
    for (let i = 0; i < n; i = i + 1) {
      res[i] = new Array(c);
    }
    for (let [i, col] of columns.entries()) {
      for (let j = 0; j < n; j = j + 1) {
        let v = rows[j][col];
        if (v !== undefined) {
          res[j][i] = v;
        } else {
          res[j][i] = fallback;
        }
      }
    }
    return res;
  }
  _getInsertValuesToken(row, columns) {
    let valueList = [];
    if (!columns) {
      columns = [];
      for (let [k, v] of Object.entries(row)) {
        columns.push(k);
        valueList.push(v);
      }
    } else {
      for (let col of columns) {
        let e = row[col];
        if (e !== undefined) {
          valueList.push(e);
        } else {
          valueList.push(DEFAULT);
        }
      }
    }
    return [asLiteral(valueList), columns];
  }
  _getBulkInsertValuesToken(rows, columns, fallback) {
    columns = columns || this._getKeys(rows);
    rows = this._rowsToArray(rows, columns, fallback);
    return [rows.map(asLiteral), columns];
  }
  _getUpdateSetToken(columns, key, tableName) {
    let tokens = [];
    if (typeof key === "string") {
      for (let col of columns) {
        if (col !== key) {
          tokens.push(`${col} = ${tableName}.${col}`);
        }
      }
    } else {
      let sets = [];
      for (let k of key) {
        sets[k] = true;
      }
      for (let col of columns) {
        if (!sets[col]) {
          tokens.push(`${col} = ${tableName}.${col}`);
        }
      }
    }
    return tokens.join(", ");
  }
  _getSelectToken(a, b, ...varargs) {
    if (a === undefined) {
      return this.error(b || "augument is required for _get_select_token");
    } else if (b === undefined) {
      return asToken(a);
    } else {
      let s = asToken(a) + ", " + asToken(b);
      for (let i = 0; i < varargs.length; i = i + 1) {
        s = s + ", " + asToken(varargs[i]);
      }
      return s;
    }
  }
  _getSelectTokenLiteral(first, second, ...varargs) {
    if (first === undefined) {
      return this.error(
        "arguments must be provided for _get_select_token_literal"
      );
    } else if (second === undefined) {
      if (typeof first === "string") {
        return asLiteral(first);
      } else if (first instanceof Array) {
        let tokens = [];
        for (let i = 0; i < first.length; i = i + 1) {
          tokens[i] = asLiteral(first[i]);
        }
        return asToken(tokens);
      } else {
        return asLiteral(first);
      }
    } else {
      let s = asLiteral(first) + ", " + asLiteral(second);
      for (let i = 0; i < varargs.length; i = i + 1) {
        let name = varargs[i];
        s = s + ", " + asLiteral(name);
      }
      return s;
    }
  }
  _getUpdateToken(row, columns) {
    let kv = [];
    if (!columns) {
      for (let [k, v] of Object.entries(row)) {
        kv.push(`${k} = ${asLiteral(v)}`);
      }
    } else {
      for (let k of columns) {
        let v = row[k];
        kv.push(`${k} = ${(v !== undefined && asLiteral(v)) || "DEFAULT"}`);
      }
    }
    return kv.join(", ");
  }
  _getWithToken(name, token) {
    if (this.isInstance(token)) {
      return `${name} AS (${token.statement()})`;
    } else if (token !== undefined) {
      return `${name} AS ${token}`;
    } else {
      return asToken(name);
    }
  }
  _getInsertToken(row, columns) {
    [row, columns] = this._getInsertValuesToken(row, columns);
    return `(${asToken(columns)}) VALUES ${row}`;
  }
  _getBulkInsertToken(rows, columns) {
    [rows, columns] = this._getBulkInsertValuesToken(rows, columns, DEFAULT);
    return `(${asToken(columns)}) VALUES ${asToken(rows)}`;
  }
  _setSelectSubqueryInsertToken(subQuery, columns) {
    let columnsToken = asToken(columns || subQuery._select || "");
    if (columnsToken !== "") {
      this._insert = `(${columnsToken}) ${subQuery.statement()}`;
    } else {
      this._insert = subQuery.statement();
    }
  }
  _setCudSubqueryInsertToken(subQuery) {
    if (subQuery._cteReturning) {
      let cr = subQuery._cteReturning;
      let cteColumns = cr.columns;
      let insertColumns = addTable(cteColumns, cr.literalColumns);
      let cudSelectQuery = Sql.new({ tableName: "d" })
        .select(cteColumns)
        .selectLiteral(cr.literals);
      this.with(`d(${asToken(cteColumns)})`, subQuery);
      this._insert = `(${asToken(insertColumns)}) ${cudSelectQuery}`;
    } else if (subQuery._returningArgs) {
      let insertColumns = subQuery._returningArgs.flat();
      let cudSelectQuery = Sql.new({ tableName: "d" }).select(insertColumns);
      this.with(`d(${asToken(insertColumns)})`, subQuery);
      this._insert = `(${asToken(insertColumns)}) ${cudSelectQuery}`;
    }
  }
  _getUpsertToken(row, key, columns) {
    [row, columns] = this._getInsertValuesToken(row, columns);
    let insertToken = `(${asToken(
      columns
    )}) VALUES ${row} ON CONFLICT (${this._getSelectToken(key)})`;
    if (
      (typeof key === "object" && key.length === columns.length) ||
      columns.length === 1
    ) {
      return `${insertToken} DO NOTHING`;
    } else {
      return `${insertToken} DO UPDATE SET ${this._getUpdateSetToken(
        columns,
        key,
        "EXCLUDED"
      )}`;
    }
  }
  _getBulkUpsertToken(rows, key, columns) {
    [rows, columns] = this._getBulkInsertValuesToken(rows, columns, DEFAULT);
    let insertToken = `(${asToken(columns)}) VALUES ${asToken(
      rows
    )} ON CONFLICT (${this._getSelectToken(key)})`;
    if (
      (typeof key === "object" && key.length === columns.length) ||
      columns.length === 1
    ) {
      return `${insertToken} DO NOTHING`;
    } else {
      return `${insertToken} DO UPDATE SET ${this._getUpdateSetToken(
        columns,
        key,
        "EXCLUDED"
      )}`;
    }
  }
  _getUpsertQueryToken(rows, key, columns) {
    assert(
      typeof columns === "object",
      "columns (table) must be provided for upserting from subquery"
    );
    let columnsToken = this._getSelectToken(columns);
    let insertToken = `(${columnsToken}) ${rows.statement()} ON CONFLICT (${this._getSelectToken(
      key
    )})`;
    if (
      (typeof key === "object" && key.length === columns.length) ||
      columns.length === 1
    ) {
      return `${insertToken} DO NOTHING`;
    } else {
      return `${insertToken} DO UPDATE SET ${this._getUpdateSetToken(
        columns,
        key,
        "EXCLUDED"
      )}`;
    }
  }
  _getJoinExpr(a, b, c) {
    if (a === undefined) {
      return this.error(b || "auguments is required for _get_join_expr");
    } else if (b === undefined) {
      return a;
    } else if (c === undefined) {
      return `${a} = ${b}`;
    } else {
      return `${a} ${b} ${c}`;
    }
  }
  _getJoinToken(joinType, rightTable, conditions, ...varargs) {
    if (conditions !== undefined) {
      return `${joinType} JOIN ${rightTable} ON (${this._getJoinExpr(
        conditions,
        ...varargs
      )})`;
    } else {
      return `${joinType} JOIN ${rightTable}`;
    }
  }
  _getInnerJoin(...varargs) {
    return this._getJoinToken("INNER", ...varargs);
  }
  _getLeftJoin(...varargs) {
    return this._getJoinToken("LEFT", ...varargs);
  }
  _getRightJoin(...varargs) {
    return this._getJoinToken("RIGHT", ...varargs);
  }
  _getFullJoin(...varargs) {
    return this._getJoinToken("FULL", ...varargs);
  }
  _getInToken(cols, range, operator) {
    cols = asToken(cols);
    operator = operator || "IN";
    if (typeof range === "object") {
      if (range instanceof Sql) {
        return `(${cols}) ${operator} (${range.statement()})`;
      } else {
        return `(${cols}) ${operator} ${asLiteral(range)}`;
      }
    } else {
      return `(${cols}) ${operator} ${range}`;
    }
  }
  _getUpdateQueryToken(subSelect, columns) {
    return `(${(columns && this._getSelectToken(columns)) || subSelect._select
      }) = (${subSelect.statement()})`;
  }
  _getJoinConditions(key, leftTable, rightTable) {
    if (typeof key === "string") {
      return `${leftTable}.${key} = ${rightTable}.${key}`;
    }
    let res = [];
    for (let [_, k] of key.entries()) {
      res.push(`${leftTable}.${k} = ${rightTable}.${k}`);
    }
    return res.join(" AND ");
  }
  _getCteValuesLiteral(rows, columns) {
    return this._getBulkInsertValuesToken(rows, columns, NULL);
  }
  _handleWhereToken(whereToken, tpl) {
    if (whereToken === undefined || whereToken === "") {
      return this;
    } else if (this._where === undefined) {
      this._where = whereToken;
    } else {
      this._where = stringFormat(tpl, this._where, whereToken);
    }
    return this;
  }
  _getConditionTokenFromTable(kwargs, logic) {
    let tokens = [];
    for (let [k, value] of Object.entries(kwargs)) {
      if (typeof k === "string") {
        tokens.push(`${k} = ${asLiteral(value)}`);
      } else {
        let token = this._getConditionToken(value);
        if (token !== undefined && token !== "") {
          tokens.push("(" + (token + ")"));
        }
      }
    }
    if (logic === undefined) {
      return tokens.join(" AND ");
    } else {
      return tokens.join(" " + (logic + " "));
    }
  }
  _getConditionToken(first, second, third) {
    if (first === undefined) {
      return this.error(
        second || "arguments is required for _get_condition_token"
      );
    } else if (second === undefined) {
      let argtype = typeof first;
      if (argtype === "object") {
        return this._getConditionTokenFromTable(first);
      } else if (argtype === "string") {
        return first;
      } else if (argtype === "function") {
        let _where = this._where;
        delete this._where;
        let res
        try {
          res = first.call(this);
          if (res === this) {
            let groupWhere = this._where;
            this._where = _where;
            return groupWhere;
          } else {
            this._where = _where
            return res;
          }
        } catch (error) {
          return this.error("condition function raise error:" + error.message);
        }
      } else {
        return this.error("invalid condition type: " + argtype);
      }
    } else if (third === undefined) {
      return `${first} = ${asLiteral(second)}`;
    } else {
      return `${first} ${second} ${asLiteral(third)}`;
    }
  }
  _getConditionTokenOr(first, ...varargs) {
    if (typeof first === "object") {
      return this._getConditionTokenFromTable(first, "OR");
    } else {
      return this._getConditionToken(first, ...varargs);
    }
  }
  _getConditionTokenNot(first, ...varargs) {
    let token;
    if (typeof first === "object") {
      token = this._getConditionTokenFromTable(first, "OR");
    } else {
      token = this._getConditionToken(first, ...varargs);
    }
    return (token !== "" && `NOT (${token})`) || "";
  }
  _handleSetOption(otherSql, innerAttr) {
    if (!this[innerAttr]) {
      this[innerAttr] = otherSql;
    } else {
      this[innerAttr] = `(${this[innerAttr]}) ${innerAttr
        .slice(1)
        .toUpperCase()} (${otherSql})`;
    }
    if (this !== Sql) {
      this.statement = this._statementForSet;
    }
    return this;
  }
  _statementForSet() {
    let statement = Sql.prototype.statement.call(this);
    if (this._intersect) {
      statement = `(${statement}) INTERSECT (${this._intersect})`;
    } else if (this._intersectAll) {
      statement = `(${statement}) INTERSECT ALL (${this._intersectAll})`;
    } else if (this._union) {
      statement = `(${statement}) UNION (${this._union})`;
    } else if (this._unionAll) {
      statement = `(${statement}) UNION ALL (${this._unionAll})`;
    } else if (this._except) {
      statement = `(${statement}) EXCEPT (${this._except})`;
    } else if (this._exceptAll) {
      statement = `(${statement}) EXCEPT ALL (${this._exceptAll})`;
    }
    return statement;
  }
  statement() {
    let tableName = this.getTable();
    let statement = assembleSql({
      tableName: tableName,
      with: this._with,
      join: this._join,
      distinct: this._distinct,
      returning: this._returning,
      cteReturning: this._cteReturning,
      insert: this._insert,
      update: this._update,
      delete: this._delete,
      using: this._using,
      select: this._select,
      from: this._from,
      where: this._where,
      group: this._group,
      having: this._having,
      order: this._order,
      limit: this._limit,
      offset: this._offset,
    });
    return statement;
  }
  with(...varargs) {
    let withToken = this._getWithToken(...varargs);
    if (this._with) {
      this._with = `${this._with}, ${withToken}`;
    } else {
      this._with = withToken;
    }
    return this;
  }
  union(otherSql) {
    return this._handleSetOption(otherSql, "_union");
  }
  unionAll(otherSql) {
    return this._handleSetOption(otherSql, "_unionAll");
  }
  except(otherSql) {
    return this._handleSetOption(otherSql, "_except");
  }
  exceptAll(otherSql) {
    return this._handleSetOption(otherSql, "_exceptAll");
  }
  intersect(otherSql) {
    return this._handleSetOption(otherSql, "_intersect");
  }
  intersectAll(otherSql) {
    return this._handleSetOption(otherSql, "_intersectAll");
  }
  as(tableAlias) {
    this._as = tableAlias;
    return this;
  }
  withValues(name, rows) {
    let columns = this._getKeys(rows[0]);
    [rows, columns] = this._getCteValuesLiteral(rows, columns);
    let cteName = `${name}(${columns.join(", ")})`;
    let cteValues = `(VALUES ${asToken(rows)})`;
    return this.with(cteName, cteValues);
  }
  insert(rows, columns) {
    if (typeof rows === "object") {
      if (rows instanceof Sql) {
        if (rows._select) {
          this._setSelectSubqueryInsertToken(rows, columns);
        } else {
          this._setCudSubqueryInsertToken(rows);
        }
      } else if (rows instanceof Array) {
        this._insert = this._getBulkInsertToken(rows, columns);
      } else if (Object.keys(rows).length) {
        this._insert = this._getInsertToken(rows, columns);
      } else {
        return this.error("can't pass empty table to sql.insert");
      }
    } else if (rows !== undefined) {
      this._insert = rows;
    } else {
      return this.error("can't pass nil to sql.insert");
    }
    return this;
  }
  update(row, columns) {
    if (typeof row === "object") {
      if (!(row instanceof Sql)) {
        this._update = this._getUpdateToken(row, columns);
      } else {
        this._update = this._getUpdateQueryToken(row, columns);
      }
    } else {
      this._update = row;
    }
    return this;
  }
  upsert(rows, key, columns) {
    if (!key) {
      throw new Error("you must provide key for upsert(string or table)")
    }
    if (rows instanceof Sql) {
      this._insert = this._getUpsertQueryToken(rows, key, columns);
    } else if (rows instanceof Array) {
      this._insert = this._getBulkUpsertToken(rows, key, columns);
    } else {
      this._insert = this._getUpsertToken(rows, key, columns);
    }
    return this;
  }
  isInstance(row) {
    return row instanceof Sql;
  }
  merge(rows, key, columns) {
    if (rows.length === 0) {
      return this.error("empty rows passed to merge");
    }
    [rows, columns] = this._getCteValuesLiteral(rows, columns);
    let cteName = `V(${columns.join(", ")})`;
    let cteValues = `(VALUES ${asToken(rows)})`;
    let joinCond = this._getJoinConditions(key, "V", "T");
    let valsColumns = columns.map(_prefixWith_V);
    let insertSubquery = Sql.new("V")
      .select(valsColumns)
      .leftJoin("U AS T", joinCond)
      .whereNull("T." + (key[0] || key));
    let updatedSubquery;
    if (
      (typeof key === "object" && key.length === columns.length) ||
      columns.length === 1
    ) {
      updatedSubquery = Sql.new({ tableName: "V" })
        .select(valsColumns)
        .join(this.tableName + " AS T", joinCond);
    } else {
      updatedSubquery = Sql.new({ tableName: this.tableName, _as: "T" })
        .update(this._getUpdateSetToken(columns, key, "V"))
        .from("V")
        .where(joinCond)
        .returning(valsColumns);
    }
    this.with(cteName, cteValues).with("U", updatedSubquery);
    return Sql.prototype.insert.call(this, insertSubquery, columns);
  }
  updates(rows, key, columns) {
    if (this.isInstance(rows)) {
      columns = columns || rows._returningArgs.flat();
      let cteName = `V(${columns.join(", ")})`;
      let joinCond = this._getJoinConditions(
        key,
        "V",
        this._as || this.tableName
      );
      this.with(cteName, rows);
      return Sql.prototype.update
        .call(this, this._getUpdateSetToken(columns, key, "V"))
        .from("V")
        .where(joinCond);
    } else if (rows.length === 0) {
      return this.error("empty rows passed to updates");
    } else {
      [rows, columns] = this._getCteValuesLiteral(rows, columns);
      let cteName = `V(${columns.join(", ")})`;
      let cteValues = `(VALUES ${asToken(rows)})`;
      let joinCond = this._getJoinConditions(
        key,
        "V",
        this._as || this.tableName
      );
      this.with(cteName, cteValues);
      return Sql.prototype.update
        .call(this, this._getUpdateSetToken(columns, key, "V"))
        .from("V")
        .where(joinCond);
    }
  }
  gets(keys) {
    if (keys.length === 0) {
      return this.error("empty keys passed to gets");
    }
    let columns = this._getKeys(keys[0]);
    [keys, columns] = this._getCteValuesLiteral(keys, columns);
    let joinCond = this._getJoinConditions(
      columns,
      "V",
      this._as || this.tableName
    );
    let cteName = `V(${columns.join(", ")})`;
    let cteValues = `(VALUES ${asToken(keys)})`;
    return this.with(cteName, cteValues).rightJoin("V", joinCond);
  }
  mergeGets(rows, keys) {
    let columns = this._getKeys(rows[0]);
    [rows, columns] = this._getCteValuesLiteral(rows, columns);
    let joinCond = this._getJoinConditions(
      keys,
      "V",
      this._as || this.tableName
    );
    let cteName = `V(${columns.join(", ")})`;
    let cteValues = `(VALUES ${asToken(rows)})`;
    return Sql.prototype.select
      .call(this, "V.*")
      .with(cteName, cteValues)
      .rightJoin("V", joinCond);
  }
  copy() {
    return Object.assign(Object.create(Object.getPrototypeOf(this)), this)
  }
  delete(...varargs) {
    this._delete = true;
    if (varargs.length) {
      this.where(...varargs);
    }
    return this;
  }
  distinct() {
    this._distinct = true;
    return this;
  }
  select(...varargs) {
    let s = this._getSelectToken(...varargs);
    if (!this._select) {
      this._select = s;
    } else if (s !== undefined && s !== "") {
      this._select = this._select + (", " + s);
    }
    return this;
  }
  selectLiteral(...varargs) {
    let s = this._getSelectTokenLiteral(...varargs);
    if (!this._select) {
      this._select = s;
    } else if (s !== undefined && s !== "") {
      this._select = this._select + (", " + s);
    }
    return this;
  }
  returning(...varargs) {
    let s = this._getSelectToken(...varargs);
    if (!this._returning) {
      this._returning = s;
    } else if (s !== undefined && s !== "") {
      this._returning = this._returning + (", " + s);
    } else {
      return this;
    }
    if (this._returningArgs) {
      this._returningArgs = [this._returningArgs, ...varargs];
    } else {
      this._returningArgs = [...varargs];
    }
    return this;
  }
  returningLiteral(...varargs) {
    let s = this._getSelectTokenLiteral(...varargs);
    if (!this._returning) {
      this._returning = s;
    } else if (s !== undefined && s !== "") {
      this._returning = this._returning + (", " + s);
    }
    if (this._returningArgs) {
      this._returningArgs = [this._returningArgs, ...varargs];
    } else {
      this._returningArgs = [...varargs];
    }
    return this;
  }
  cteReturning(opts) {
    this._cteReturning = opts;
    return this;
  }
  group(...varargs) {
    if (!this._group) {
      this._group = this._getSelectToken(...varargs);
    } else {
      this._group = this._group + (", " + this._getSelectToken(...varargs));
    }
    return this;
  }
  groupBy(...varargs) {
    return this.group(...varargs);
  }
  order(...varargs) {
    if (!this._order) {
      this._order = this._getSelectToken(...varargs);
    } else {
      this._order = this._order + (", " + this._getSelectToken(...varargs));
    }
    return this;
  }
  orderBy(...varargs) {
    return this.order(...varargs);
  }
  _getArgsToken(...varargs) {
    return this._getSelectToken(...varargs);
  }
  using(...varargs) {
    this._delete = true;
    this._using = this._getArgsToken(...varargs);
    return this;
  }
  from(...varargs) {
    if (!this._from) {
      this._from = this._getArgsToken(...varargs);
    } else {
      this._from = this._from + (", " + this._getArgsToken(...varargs));
    }
    return this;
  }
  getTable() {
    return (
      (this._as === undefined && this.tableName) ||
      this.tableName + (" AS " + this._as)
    );
  }
  join(...varargs) {
    let joinToken = this._getInnerJoin(...varargs);
    this._from = `${this._from || this.getTable()} ${joinToken}`;
    return this;
  }
  innerJoin(...varargs) {
    return this.join(...varargs);
  }
  leftJoin(...varargs) {
    let joinToken = this._getLeftJoin(...varargs);
    this._from = `${this._from || this.getTable()} ${joinToken}`;
    return this;
  }
  rightJoin(...varargs) {
    let joinToken = this._getRightJoin(...varargs);
    this._from = `${this._from || this.getTable()} ${joinToken}`;
    return this;
  }
  fullJoin(...varargs) {
    let joinToken = this._getFullJoin(...varargs);
    this._from = `${this._from || this.getTable()} ${joinToken}`;
    return this;
  }
  limit(n) {
    this._limit = n;
    return this;
  }
  offset(n) {
    this._offset = n;
    return this;
  }
  where(first, ...varargs) {
    let whereToken = this._getConditionToken(first, ...varargs);
    return this._handleWhereToken(whereToken, "(%s) AND (%s)");
  }
  whereOr(first, ...varargs) {
    let whereToken = this._getConditionTokenOr(first, ...varargs);
    return this._handleWhereToken(whereToken, "(%s) AND (%s)");
  }
  orWhereOr(first, ...varargs) {
    let whereToken = this._getConditionTokenOr(first, ...varargs);
    return this._handleWhereToken(whereToken, "%s OR %s");
  }
  whereNot(first, ...varargs) {
    let whereToken = this._getConditionTokenNot(first, ...varargs);
    return this._handleWhereToken(whereToken, "(%s) AND (%s)");
  }
  orWhere(first, ...varargs) {
    let whereToken = this._getConditionToken(first, ...varargs);
    return this._handleWhereToken(whereToken, "%s OR %s");
  }
  orWhereNot(first, ...varargs) {
    let whereToken = this._getConditionTokenNot(first, ...varargs);
    return this._handleWhereToken(whereToken, "%s OR %s");
  }
  whereExists(builder) {
    if (this._where) {
      this._where = `(${this._where}) AND EXISTS (${builder})`;
    } else {
      this._where = `EXISTS (${builder})`;
    }
    return this;
  }
  whereNotExists(builder) {
    if (this._where) {
      this._where = `(${this._where}) AND NOT EXISTS (${builder})`;
    } else {
      this._where = `NOT EXISTS (${builder})`;
    }
    return this;
  }
  whereIn(cols, range) {
    let inToken = this._getInToken(cols, range);
    if (this._where) {
      this._where = `(${this._where}) AND ${inToken}`;
    } else {
      this._where = inToken;
    }
    return this;
  }
  whereNotIn(cols, range) {
    let notInToken = this._getInToken(cols, range, "NOT IN");
    if (this._where) {
      this._where = `(${this._where}) AND ${notInToken}`;
    } else {
      this._where = notInToken;
    }
    return this;
  }
  whereNull(col) {
    if (this._where) {
      this._where = `(${this._where}) AND ${col} IS NULL`;
    } else {
      this._where = col + " IS NULL";
    }
    return this;
  }
  whereNotNull(col) {
    if (this._where) {
      this._where = `(${this._where}) AND ${col} IS NOT NULL`;
    } else {
      this._where = col + " IS NOT NULL";
    }
    return this;
  }
  whereBetween(col, low, high) {
    if (this._where) {
      this._where = `(${this._where}) AND (${col} BETWEEN ${low} AND ${high})`;
    } else {
      this._where = `${col} BETWEEN ${low} AND ${high}`;
    }
    return this;
  }
  whereNotBetween(col, low, high) {
    if (this._where) {
      this._where = `(${this._where}) AND (${col} NOT BETWEEN ${low} AND ${high})`;
    } else {
      this._where = `${col} NOT BETWEEN ${low} AND ${high}`;
    }
    return this;
  }
  whereRaw(whereToken) {
    if (whereToken === "") {
      return this;
    } else if (this._where) {
      this._where = `(${this._where}) AND (${whereToken})`;
    } else {
      this._where = whereToken;
    }
    return this;
  }
  orWhereExists(builder) {
    if (this._where) {
      this._where = `${this._where} OR EXISTS (${builder})`;
    } else {
      this._where = `EXISTS (${builder})`;
    }
    return this;
  }
  orWhereNotExists(builder) {
    if (this._where) {
      this._where = `${this._where} OR NOT EXISTS (${builder})`;
    } else {
      this._where = `NOT EXISTS (${builder})`;
    }
    return this;
  }
  orWhereIn(cols, range) {
    let inToken = this._getInToken(cols, range);
    if (this._where) {
      this._where = `${this._where} OR ${inToken}`;
    } else {
      this._where = inToken;
    }
    return this;
  }
  orWhereNotIn(cols, range) {
    let notInToken = this._getInToken(cols, range, "NOT IN");
    if (this._where) {
      this._where = `${this._where} OR ${notInToken}`;
    } else {
      this._where = notInToken;
    }
    return this;
  }
  orWhereNull(col) {
    if (this._where) {
      this._where = `${this._where} OR ${col} IS NULL`;
    } else {
      this._where = col + " IS NULL";
    }
    return this;
  }
  orWhereNotNull(col) {
    if (this._where) {
      this._where = `${this._where} OR ${col} IS NOT NULL`;
    } else {
      this._where = col + " IS NOT NULL";
    }
    return this;
  }
  orWhereBetween(col, low, high) {
    if (this._where) {
      this._where = `${this._where} OR (${col} BETWEEN ${low} AND ${high})`;
    } else {
      this._where = `${col} BETWEEN ${low} AND ${high}`;
    }
    return this;
  }
  orWhereNotBetween(col, low, high) {
    if (this._where) {
      this._where = `${this._where} OR (${col} NOT BETWEEN ${low} AND ${high})`;
    } else {
      this._where = `${col} NOT BETWEEN ${low} AND ${high}`;
    }
    return this;
  }
  orWhereRaw(whereToken) {
    if (whereToken === "") {
      return this;
    } else if (this._where) {
      this._where = `${this._where} OR ${whereToken}`;
    } else {
      this._where = whereToken;
    }
    return this;
  }
  having(...varargs) {
    if (this._having) {
      this._having = `(${this._having}) AND (${this._getConditionToken(
        ...varargs
      )})`;
    } else {
      this._having = this._getConditionToken(...varargs);
    }
    return this;
  }
  havingNot(...varargs) {
    if (this._having) {
      this._having = `(${this._having}) AND (${this._getConditionTokenNot(
        ...varargs
      )})`;
    } else {
      this._having = this._getConditionTokenNot(...varargs);
    }
    return this;
  }
  havingExists(builder) {
    if (this._having) {
      this._having = `(${this._having}) AND EXISTS (${builder})`;
    } else {
      this._having = `EXISTS (${builder})`;
    }
    return this;
  }
  havingNotExists(builder) {
    if (this._having) {
      this._having = `(${this._having}) AND NOT EXISTS (${builder})`;
    } else {
      this._having = `NOT EXISTS (${builder})`;
    }
    return this;
  }
  havingIn(cols, range) {
    let inToken = this._getInToken(cols, range);
    if (this._having) {
      this._having = `(${this._having}) AND ${inToken}`;
    } else {
      this._having = inToken;
    }
    return this;
  }
  havingNotIn(cols, range) {
    let notInToken = this._getInToken(cols, range, "NOT IN");
    if (this._having) {
      this._having = `(${this._having}) AND ${notInToken}`;
    } else {
      this._having = notInToken;
    }
    return this;
  }
  havingNull(col) {
    if (this._having) {
      this._having = `(${this._having}) AND ${col} IS NULL`;
    } else {
      this._having = col + " IS NULL";
    }
    return this;
  }
  havingNotNull(col) {
    if (this._having) {
      this._having = `(${this._having}) AND ${col} IS NOT NULL`;
    } else {
      this._having = col + " IS NOT NULL";
    }
    return this;
  }
  havingBetween(col, low, high) {
    if (this._having) {
      this._having = `(${this._having}) AND (${col} BETWEEN ${low} AND ${high})`;
    } else {
      this._having = `${col} BETWEEN ${low} AND ${high}`;
    }
    return this;
  }
  havingNotBetween(col, low, high) {
    if (this._having) {
      this._having = `(${this._having}) AND (${col} NOT BETWEEN ${low} AND ${high})`;
    } else {
      this._having = `${col} NOT BETWEEN ${low} AND ${high}`;
    }
    return this;
  }
  havingRaw(token) {
    if (this._having) {
      this._having = `(${this._having}) AND (${token})`;
    } else {
      this._having = token;
    }
    return this;
  }
  orHaving(...varargs) {
    if (this._having) {
      this._having = `${this._having} OR ${this._getConditionToken(
        ...varargs
      )}`;
    } else {
      this._having = this._getConditionToken(...varargs);
    }
    return this;
  }
  orHavingNot(...varargs) {
    if (this._having) {
      this._having = `${this._having} OR ${this._getConditionTokenNot(
        ...varargs
      )}`;
    } else {
      this._having = this._getConditionTokenNot(...varargs);
    }
    return this;
  }
  orHavingExists(builder) {
    if (this._having) {
      this._having = `${this._having} OR EXISTS (${builder})`;
    } else {
      this._having = `EXISTS (${builder})`;
    }
    return this;
  }
  orHavingNotExists(builder) {
    if (this._having) {
      this._having = `${this._having} OR NOT EXISTS (${builder})`;
    } else {
      this._having = `NOT EXISTS (${builder})`;
    }
    return this;
  }
  orHavingIn(cols, range) {
    let inToken = this._getInToken(cols, range);
    if (this._having) {
      this._having = `${this._having} OR ${inToken}`;
    } else {
      this._having = inToken;
    }
    return this;
  }
  orHavingNotIn(cols, range) {
    let notInToken = this._getInToken(cols, range, "NOT IN");
    if (this._having) {
      this._having = `${this._having} OR ${notInToken}`;
    } else {
      this._having = notInToken;
    }
    return this;
  }
  orHavingNull(col) {
    if (this._having) {
      this._having = `${this._having} OR ${col} IS NULL`;
    } else {
      this._having = col + " IS NULL";
    }
    return this;
  }
  orHavingNotNull(col) {
    if (this._having) {
      this._having = `${this._having} OR ${col} IS NOT NULL`;
    } else {
      this._having = col + " IS NOT NULL";
    }
    return this;
  }
  orHavingBetween(col, low, high) {
    if (this._having) {
      this._having = `${this._having} OR (${col} BETWEEN ${low} AND ${high})`;
    } else {
      this._having = `${col} BETWEEN ${low} AND ${high}`;
    }
    return this;
  }
  orHavingNotBetween(col, low, high) {
    if (this._having) {
      this._having = `${this._having} OR (${col} NOT BETWEEN ${low} AND ${high})`;
    } else {
      this._having = `${col} NOT BETWEEN ${low} AND ${high}`;
    }
    return this;
  }
  orHavingRaw(token) {
    if (this._having) {
      this._having = `${this._having} OR ${token}`;
    } else {
      this._having = token;
    }
    return this;
  }
}

export default Sql;