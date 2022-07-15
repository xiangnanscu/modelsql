import Sql from './modelsql.mjs'


const User = {
  tableName: 'Usr',
  fieldNames: ['id', 'name'],
  nameCache: {
    id: "id",
    name:"name"
  },
  fields: {
    id: { name: 'id' },
    name: {name: 'name'}
  }
}
class UserSql extends Sql {
  model = User
  tableName = User.tableName
}

console.log(UserSql.new().select("id", "name").statement())
console.log(UserSql.new().validate(false).insert({ "id": 1, "name": "foo" }).statement())