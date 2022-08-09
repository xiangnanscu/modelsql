import Sql from './src/modelsql.mjs'


const User = {
  tableName: 'usr',
  fieldNames: ['id', 'name'],
  nameCache: {
    id: "id",
    name: "name"
  },
  fields: {
    id: { name: 'id' },
    name: { name: 'name' }
  },
  prepareDbRows(row, cols) {
    return [row, cols]
  },
  async query(s) {
    throw new Error("aa")
  }
}
class UserSql2 extends Sql {
  model = User
  tableName = User.tableName
}
const UserSql = Sql.makeClass({
  model: User,
  tableName: User.tableName
})



test('select', () => {
  expect(UserSql.new().select("id", "name").statement()).toBe("SELECT id, name FROM usr")
});

test('insert', async () => {
  await expect(UserSql.new().skipValidate().insert({ "id": 1, "name": "foo" }).exec()).rejects.toThrow(/aa/)
});