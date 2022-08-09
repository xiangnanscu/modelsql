import Sql from "@xiangnanscu/sql"
let NULL = Sql.NULL;
let asLiteral = Sql.asLiteral;
let asToken = Sql.asToken;


let FOREIGNKEY = 2;
let NONFOREIGNKEY = 3;
let END = 4;
let COMPAREOPERATORS = {
  lt: "<",
  lte: "<=",
  gt: ">",
  gte: ">=",
  ne: "<>",
  eq: "=",
};
function assert(bool, errMsg) {
  if (!bool) {
    throw new Error(errMsg)
  } else {
    return bool
  }
}
function getForeignObject(attrs, prefix) {
  let fk = {};
  let n = prefix.length;
  for (let [k, v] of Object.entries(attrs)) {
    if (k.slice(0, n) === prefix) {
      fk[k.slice(n)] = v;
      delete attrs[k];
    }
  }
  return fk;
}
let bulkMethods = {
  upsert: { validateMethod: "validateCreateRows", sqlMethod: Sql.prototype.upsert },
  merge: { validateMethod: "validateCreateRows", sqlMethod: Sql.prototype.merge },
  updates: { validateMethod: "validateUpdateRows", sqlMethod: Sql.prototype.updates },
};
async function bulkDispatcher(name, self, rows, key, columns) {
  if (!self.isInstance(rows)) {
    let skipValidate = self._skipValidate === undefined ? false : !!self._skipValidate;
    if (!skipValidate) {
      [rows, key, columns] = self.model[bulkMethods[name].validateMethod](
        rows,
        key,
        columns
      );
      if (rows === undefined) {
        return self.error(key);
      }
    }
    [rows, columns] = self.model.prepareDbRows(
      rows,
      columns,
      name === "updates"
    );
  }
  if (rows === undefined) {
    return self.error(columns);
  }
  let bulkSql = bulkMethods[name].sqlMethod.call(self, rows, key, columns);
  if (self._commit === undefined || self._commit) {
    if (!self._returning) {
      return await bulkSql.returning(key).compact().execr();
    } else {
      return await bulkSql.compact().execr();
    }
  } else {
    return bulkSql;
  }
}

class ModelSql extends Sql {
  static new() {
    return new this()
  }
  static makeClass({ model, tableName }) {
    class ConcreteModelSql extends this {
      model = model
      tableName = tableName
    }
    return ConcreteModelSql
  }
  toString() {
    return this.statement()
  }
  _getConditionTokenFromTable(kwargs, logic) {
    let tokens = [];
    for (let [k, value] of Object.entries(kwargs)) {
      if (typeof k === "string") {
        tokens.push(this._getExprToken(value, ...this._getWhereKey(k)));
      } else {
        let token = this._getConditionToken(value);
        if (token !== undefined && token !== "") {
          tokens.push("(" + token + ")");
        }
      }
    }
    if (logic === undefined) {
      return tokens.join(" AND ");
    } else {
      return tokens.join(" " + logic + " ");
    }
  }
  _getConditionToken(first, second, third) {
    if (second === undefined) {
      return Sql.prototype._getConditionToken.call(this, first);
    } else if (third === undefined) {
      return `${this._getColumn(first)} = ${asLiteral(second)}`;
    } else {
      return `${this._getColumn(first)} ${second} ${asLiteral(third)}`;
    }
  }
  _getSelectToken(first, second, ...varargs) {
    if (first === undefined) {
      return this.error(
        second || "augument is required for _getSelectToken"
      );
    } else if (second === undefined) {
      if (first instanceof Array) {
        let tokens = [];
        for (let i = 0; i < first.length; i = i + 1) {
          tokens[i] = this._getColumn(first[i]);
        }
        return asToken(tokens);
      } else if (typeof first === "string") {
        return this._getColumn(first);
      } else {
        return asToken(first);
      }
    } else {
      first = this._getColumn(first);
      second = this._getColumn(second);
      let s = asToken(first) + ", " + asToken(second);
      for (let i = 0; i < varargs.length; i = i + 1) {
        let name = varargs[i];
        s = s + ", " + asToken(this._getColumn(name));
      }
      return s;
    }
  }
  _rowsToArray(rows, columns) {
    let c = columns.length;
    let n = rows.length;
    let res = new Array(n);
    let fields = this.model.fields;
    for (let i = 0; i < n; i = i + 1) {
      res[i] = new Array(c);
    }
    for (let [i, col] of columns.entries()) {
      for (let j = 0; j < n; j = j + 1) {
        let v = rows[j][col];
        if (v !== undefined) {
          res[j][i] = v;
        } else {
          let dft = fields[col].default;
          if (dft !== undefined) {
            res[j][i] = fields[col].getDefault(rows[j]);
          } else {
            res[j][i] = NULL;
          }
        }
      }
    }
    return res;
  }
  _getCteValuesLiteral(rows, columns, noCheck) {
    columns = columns || this._getKeys(rows);
    rows = this._rowsToArray(rows, columns);
    let firstRow = rows[0];
    for (let [i, col] of columns.entries()) {
      let [field] = this._findFieldModel(col);
      if (field) {
        firstRow[i] = `${asLiteral(firstRow[i])}::${field.dbType}`;
      } else if (noCheck) {
        firstRow[i] = asLiteral(firstRow[i]);
      } else {
        return this.error(
          "invalid field name for _getCteValuesLiteral: " + col
        );
      }
    }
    rows[0] = "(" + asToken(firstRow) + ")";
    let rl = rows.length
    for (let i = 1; i < rl; i = i + 1) {
      rows[i] = asLiteral(rows[i]);
    }
    return [rows, columns];
  }
  _handleJoin(joinType, joinTable, joinCond) {
    if (this._update) {
      this.from(joinTable);
      this.where(joinCond);
    } else if (this._delete) {
      this.using(joinTable);
      this.where(joinCond);
    } else {
      Sql.prototype[joinType + "Join"].call(this, joinTable, joinCond);
    }
  }
  _registerJoinModel(joinArgs, joinType) {
    joinType = joinType || joinArgs.joinType || "INNER";
    let find = true;
    let model = joinArgs.model || this.model;
    let fkModel = joinArgs.fkModel;
    let column = joinArgs.column;
    let fkColumn = joinArgs.fkColumn;
    let joinKey;
    if (joinArgs.joinKey === undefined) {
      if (this.model === model) {
        joinKey = column + "__" + fkModel.tableName;
      } else {
        joinKey = `${joinType}__${model.tableName}__${column}__${fkModel.tableName}__${fkColumn}`;
      }
    } else {
      joinKey = joinArgs.joinKey;
    }
    if (!this._joinKeys) {
      this._joinKeys = [];
    }
    let joinObj = this._joinKeys[joinKey];
    if (!joinObj) {
      find = false;
      joinObj = {
        joinType: joinType,
        model: model,
        column: column,
        alias: joinArgs.alias || model.tableName,
        fkModel: fkModel,
        fkColumn: fkColumn,
        fkAlias: "T" + this._getJoinNumber(),
      };
      let joinTable = `${fkModel.tableName} ${joinObj.fkAlias}`;
      let joinCond = `${joinObj.alias}.${joinObj.column} = ${joinObj.fkAlias}.${joinObj.fkColumn}`;
      this._handleJoin(joinType.toLowerCase(), joinTable, joinCond);
      this._joinKeys[joinKey] = joinObj;
    }
    return joinObj  //[joinObj, find];
  }
  _findFieldModel(col) {
    let field = this.model.fields[col];
    if (field) {
      return [field, this.model, this._as || this.model.tableName];
    }
    if (!this._joinKeys) {
      return;
    }
    for (let joinObj of Object.values(this._joinKeys)) {
      let fkField = joinObj.fkModel.fields[col];
      if (joinObj.model === this.model && fkField) {
        return [
          fkField,
          joinObj.fkModel,
          joinObj.fkAlias || joinObj.fkModel.tableName,
        ];
      }
    }
  }
  _getWhereKey(key) {
    let a = key.indexOf("__");
    if (a === -1) {
      return [this._getColumn(key), "eq"];
    }
    let e = key.slice(0, a);
    let [field, model, prefix] = this._findFieldModel(e);
    if (!field) {
      return this.error(
        `${e} is not a valid field name for ${this.model.tableName}`
      );
    }
    let i, state, fkModel, rc, joinKey;
    let operator = "eq";
    let fieldName = e;
    if (field.reference) {
      fkModel = field.reference;
      rc = field.referenceColumn;
      state = FOREIGNKEY;
    } else {
      state = NONFOREIGNKEY;
    }
    while (true) {
      i = a + 2;
      a = key.indexOf("__", i);
      if (a === -1) {
        e = key.slice(i);
      } else {
        e = key.slice(i, a);
      }
      if (state === NONFOREIGNKEY) {
        operator = e;
        state = END;
      } else if (state === FOREIGNKEY) {
        let fieldOfFk = fkModel.fields[e];
        if (fieldOfFk) {
          if (!joinKey) {
            joinKey = fieldName + "__" + fkModel.tableName;
          } else {
            joinKey = joinKey + "__" + fieldName;
          }
          let joinObj = this._registerJoinModel({
            joinKey: joinKey,
            model: model,
            column: fieldName,
            alias: prefix || model.tableName,
            fkModel: fkModel,
            fkColumn: rc,
          });
          prefix = joinObj.fkAlias;
          if (fieldOfFk.reference) {
            model = fkModel;
            fkModel = fieldOfFk.reference;
            rc = fieldOfFk.referenceColumn;
          } else {
            state = NONFOREIGNKEY;
          }
          fieldName = e;
        } else {
          operator = e;
          state = END;
        }
      } else {
        return this.error(
          `invalid condition table key parsing state ${state} with token ${e}`
        );
      }
      if (a === -1) {
        break;
      }
    }
    return [prefix + "." + fieldName, operator];
  }
  _getColumn(key) {
    if (this.model.fields[key]) {
      return (
        (this._as && this._as + "." + key) || this.model.nameCache[key]
      );
    }
    if (!this._joinKeys) {
      return key;
    }
    for (let joinObj of Object.values(this._joinKeys)) {
      if (joinObj.model === this.model && joinObj.fkModel.fields[key]) {
        return joinObj.fkAlias + "." + key;
      }
    }
    return key;
  }
  _getExprToken(value, key, op) {
    if (op === "eq") {
      return `${key} = ${asLiteral(value)}`;
    } else if (op === "in") {
      return `${key} IN ${asLiteral(value)}`;
    } else if (op === "notin") {
      return `${key} NOT IN ${asLiteral(value)}`;
    } else if (COMPAREOPERATORS[op]) {
      return `${key} ${COMPAREOPERATORS[op]} ${asLiteral(value)}`;
    } else if (op === "contains") {
      return `${key} LIKE '%${value.replaceAll("'", "''")}%'`;
    } else if (op === "startswith") {
      return `${key} LIKE '${value.replaceAll("'", "''")}%'`;
    } else if (op === "endswith") {
      return `${key} LIKE '%${value.replaceAll("'", "''")}'`;
    } else if (op === "null") {
      if (value) {
        return `${key} IS NULL`;
      } else {
        return `${key} IS NOT NULL`;
      }
    } else {
      return this.error("invalid sql operator: " + op);
    }
  }
  _getJoinNumber() {
    if (this._joinKeys) {
      return Object.keys(this._joinKeys).length + 1;
    } else {
      return 1;
    }
  }
  withValues(name, rows) {
    let columns = this._getKeys(rows[0]);
    [rows, columns] = this._getCteValuesLiteral(rows, columns, true);
    let cteName = `${name}(${columns.join(", ")})`;
    let cteValues = `(VALUES ${asToken(rows)})`;
    return this.with(cteName, cteValues);
  }
  insert(rows, columns) {
    if (!this.isInstance(rows)) {
      if (!this._skipValidate) {
        [rows, columns] = this.model.validateCreateData(rows, columns);
      }
      [rows, columns] = this.model.prepareDbRows(rows, columns);
    }
    return Sql.prototype.insert.call(this, rows, columns);
  }
  update(row, columns) {
    if (!this.isInstance(row)) {
      if (!this._skipValidate) {
        row = this.model.validateUpdate(row, columns);
      }
      [row, columns] = this.model.prepareDbRows(row, columns, true);
    }
    return Sql.prototype.update.call(this, row, columns);
  }
  async gets(keys) {
    if (this._commit === undefined || this._commit) {
      return await Sql.prototype.gets.call(this, keys).execr();
    } else {
      return Sql.prototype.gets.call(this, keys);
    }
  }
  async mergeGets(rows, keys) {
    let columns = this._getKeys(rows[0]);
    [rows, columns] = this._getCteValuesLiteral(rows, columns, true);
    let joinCond = this._getJoinConditions(
      keys,
      "V",
      this._as || this.tableName
    );
    let cteName = `V(${columns.join(", ")})`;
    let cteValues = `(VALUES ${asToken(rows)})`;
    let res = Sql.prototype.select
      .call(this, "V.*")
      .with(cteName, cteValues)
      .rightJoin("V", joinCond);
    if (this._commit === undefined || this._commit) {
      return await res.execr();
    } else {
      return res;
    }
  }
  join(joinArgs, ...varargs) {
    if (typeof joinArgs === "object") {
      this._registerJoinModel(joinArgs, "INNER");
    } else {
      Sql.prototype.join.call(this, joinArgs, ...varargs);
    }
    return this;
  }
  innerJoin(joinArgs, ...varargs) {
    if (typeof joinArgs === "object") {
      this._registerJoinModel(joinArgs, "INNER");
    } else {
      Sql.prototype.join.call(this, joinArgs, ...varargs);
    }
    return this;
  }
  leftJoin(joinArgs, ...varargs) {
    if (typeof joinArgs === "object") {
      this._registerJoinModel(joinArgs, "LEFT");
    } else {
      Sql.prototype.leftJoin.call(this, joinArgs, ...varargs);
    }
    return this;
  }
  rightJoin(joinArgs, ...varargs) {
    if (typeof joinArgs === "object") {
      this._registerJoinModel(joinArgs, "RIGHT");
    } else {
      Sql.prototype.rightJoin.call(this, joinArgs, ...varargs);
    }
    return this;
  }
  fullJoin(joinArgs, ...varargs) {
    if (typeof joinArgs === "object") {
      this._registerJoinModel(joinArgs, "FULL");
    } else {
      Sql.prototype.fullJoin.call(this, joinArgs, ...varargs);
    }
    return this;
  }
  whereIn(cols, range) {
    if (typeof cols === "string") {
      return Sql.prototype.whereIn.call(this, this._getColumn(cols), range);
    } else {
      let res = [];
      for (let i = 0; i < cols.length; i = i + 1) {
        res[i] = this._getColumn(cols[i]);
      }
      return Sql.prototype.whereIn.call(this, res, range);
    }
  }
  whereNotIn(cols, range) {
    if (typeof cols === "string") {
      cols = this._getColumn(cols);
    } else {
      for (let i = 0; i < cols.length; i = i + 1) {
        cols[i] = this._getColumn(cols[i]);
      }
    }
    return Sql.prototype.whereNotIn.call(this, cols, range);
  }
  whereNull(col) {
    return Sql.prototype.whereNull.call(this, this._getColumn(col));
  }
  whereNotNull(col) {
    return Sql.prototype.whereNotNull.call(this, this._getColumn(col));
  }
  whereBetween(col, low, high) {
    return Sql.prototype.whereBetween.call(this, this._getColumn(col), low, high);
  }
  whereNotBetween(col, low, high) {
    return Sql.prototype.whereNotBetween.call(
      this,
      this._getColumn(col),
      low,
      high
    );
  }
  orWhereIn(cols, range) {
    if (typeof cols === "string") {
      cols = this._getColumn(cols);
    } else {
      for (let i = 0; i < cols.length; i = i + 1) {
        cols[i] = this._getColumn(cols[i]);
      }
    }
    return Sql.prototype.orWhereIn.call(this, cols, range);
  }
  orWhereNotIn(cols, range) {
    if (typeof cols === "string") {
      cols = this._getColumn(cols);
    } else {
      for (let i = 0; i < cols.length; i = i + 1) {
        cols[i] = this._getColumn(cols[i]);
      }
    }
    return Sql.prototype.orWhereNotIn.call(this, cols, range);
  }
  orWhereNull(col) {
    return Sql.prototype.orWhereNull.call(this, this._getColumn(col));
  }
  orWhereNotNull(col) {
    return Sql.prototype.orWhereNotNull.call(this, this._getColumn(col));
  }
  orWhereBetween(col, low, high) {
    return Sql.prototype.orWhereBetween.call(
      this,
      this._getColumn(col),
      low,
      high
    );
  }
  orWhereNotBetween(col, low, high) {
    return Sql.prototype.orWhereNotBetween.call(
      this,
      this._getColumn(col),
      low,
      high
    );
  }
  async upsert(rows, key, columns) {
    return await bulkDispatcher("upsert", this, rows, key, columns);
  }
  async merge(rows, key, columns) {
    return await bulkDispatcher("merge", this, rows, key, columns);
  }
  async updates(rows, key, columns) {
    return await bulkDispatcher("updates", this, rows, key, columns);
  }
  async filter(kwargs) {
    let whereToken = this._getConditionTokenFromTable(kwargs);
    return await this._handleWhereToken(whereToken, "(%s) AND (%s)").exec();
  }
  async exists() {
    let statement = `SELECT EXISTS (${this.select("").limit(1).statement()})`;
    return await this.model.query(statement);
  }
  commit(bool) {
    this._commit = bool;
    return this;
  }
  skipValidate(bool) {
    this._skipValidate = bool === undefined ? true : !!bool;
    return this;
  }
  async flat(depth) {
    return await this.compact().execr().flat(depth);
  }
  async get(...varargs) {
    let records;
    if (varargs.length > 0) {
      records = await this.where(...varargs).limit(2).exec();
    } else {
      records = await this.limit(2).exec();
    }
    if (records.length === 1) {
      return records[0];
    } else {
      return this.error("not 1 record returned:" + records.length);
    }
  }
  async getOrCreate(params, ...varargs) {
    let records = await this.select(...varargs)
      .where(params)
      .limit(2)
      .exec();
    if (records.length === 1) {
      return records[0];
    } else if (records.length === 0) {
      let pk = this.model.primaryKey;
      let res = await this.model.Sql.new().insert(params).returning(pk).execr();
      params[pk] = res[0][pk];
      return this.model.new(params);
    } else {
      return this.error("getOrCreate: not 1 record returned");
    }
  }
  async asSet() {
    return await this.compact().execr().flat().asSet();
  }
  async count(...varargs) {
    let res = await this.select("count(*)")
      .where(...varargs)
      .compact()
      .exec();
    return res[0][0];
  }
  async execr() {
    return await this.raw().exec();
  }
  async exec() {
    let statement = this.statement();
    let records = await this.model.query(statement, this._compact);
    if (this._raw || this._compact) {
      return records;
    } else if ((this._select || (!this._update && !this._insert && !this._delete)) && Array.isArray(records)) {
      if (!this._loadFk) {
        for (let [i, record] of records.entries()) {
          records[i] = this.model.load(record);
        }
      } else {
        let fields = this.model.fields;
        let fieldNames = this.model.fieldNames;
        for (let [i, record] of records.entries()) {
          for (let name of fieldNames) {
            let field = fields[name];
            let value = record[name];
            if (value !== undefined) {
              let fkModel = this._loadFk[name];
              if (!fkModel) {
                if (!field.load) {
                  record[name] = value;
                } else {
                  record[name] = field.load(value);
                }
              } else {
                record[name] = fkModel.load(
                  getForeignObject(record, name + "__")
                );
              }
            }
          }
          records[i] = this.model.new(record);
        }
      }
      return records;
    } else {
      return records;
    }
  }
  compact() {
    this._compact = true;
    return this;
  }
  raw() {
    this._raw = true;
    return this;
  }
  loadFk(fkName, first, ...varargs) {
    let fk = this.model.foreignKeys[fkName];
    if (fk === undefined) {
      return this.error(
        fkName +
        " is not a valid forein key name for " + this.model.tableName
      );
    }
    let fkModel = fk.reference;
    let joinKey = fkName + "__" + fkModel.tableName;
    let joinObj = this._registerJoinModel({
      joinKey: joinKey,
      column: fkName,
      fkModel: fkModel,
      fkColumn: fk.referenceColumn,
    });
    if (!this._loadFk) {
      this._loadFk = [];
    }
    this._loadFk[fkName] = fkModel;
    if (!first) {
      return this;
    }
    let rightAlias = joinObj.fkAlias;
    let fks;
    if (typeof first === "object") {
      let res = [];
      for (let fkn of first) {
        assert(
          fkModel.fields[fkn],
          "invalid field name for fk model: " + fkn
        );
        res.push(`${rightAlias}.${fkn} AS ${fkName}__${fkn}`);
      }
      fks = res.join(", ");
    } else if (first === "*") {
      let res = [];
      for (let fkn of fkModel.fieldNames) {
        res.push(`${rightAlias}.${fkn} AS ${fkName}__${fkn}`);
      }
      fks = res.join(", ");
    } else if (typeof first === "string") {
      assert(
        fkModel.fields[first],
        "invalid field name for fk model: " + first
      );
      fks = `${rightAlias}.${first} AS ${fkName}__${first}`;
      for (let i = 0; i < varargs.length; i = i + 1) {
        let fkn = varargs[i];
        assert(
          fkModel.fields[fkn],
          "invalid field name for fk model: " + fkn
        );
        fks = `${fks}, ${rightAlias}.${fkn} AS ${fkName}__${fkn}`;
      }
    } else {
      return this.error(`invalid argument type ${typeof first} for loadFk`);
    }
    return Sql.prototype.select.call(this, fks);
  }
  OR(kwargs) {
    return this._getConditionTokenFromTable(kwargs, "OR");
  }
  AND(kwargs) {
    return this._getConditionTokenFromTable(kwargs);
  }
}

export default ModelSql;