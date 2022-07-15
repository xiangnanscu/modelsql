import Sql from './sql.mjs'

let sqlLines = `Sql.new("usr").where({ i: 1, name: "foo", time: 36 });
Sql.new("usr").select("id", "name", "age");
Sql.new("usr").select(["id", "name", "x"]);
Sql.new("usr").select("id, name");
Sql.new("usr").select("id as i, name").where({ i: 1, name: "foo", time: 36 });
Sql.new("usr").select("id").whereOr({ a: 1, c: 2, d: "ff" });
Sql.new("usr").select("id").where("id", 1);
Sql.new("usr").select("id").where("id", ">", 1);
Sql.new("usr").select("id").where("name", "abc");
Sql.new("usr").select("id").where("name", Sql.NULL);
Sql.new("usr").select("id").where("name", ()=>'NULL');
Sql.new("usr").as("x").select("id", "name as n").where("name", Sql.NULL);
Sql.new("usr").join("profile", "usr.id=profile.userId").select("usr.id");
Sql.new("usr").join("profile", "usr.id", "profile.userId").select("usr.id");
Sql.new("profile").join("usr u1", "u1.id", "profile.userId").select("u1.id");
Sql.new("profile").join("usr u1", "u1.id", ">", "profile.userId").select("u1.id");
Sql.new("profile").join("usr u1", "u1.id <= profile.userId").select("u1.id");
Sql.new("profile")
  .as("p")
  .join("usr uf", "uf.id", "p.fatherId")
  .join("usr um", "um.id", "p.motherId")
  .select(
    "uf.email as fatherEmail",
    "um.email as motherEmail",
    "p.name as yourname"
  )
  .where("p.name", "kate");
Sql.new("usr").select("id").where("id", 1).whereOr({ name: "foo", age: 3 });
Sql.new("usr").select("id").where("id", 1).orWhere("name", "foo");
Sql.new("usr").select("id").orWhere("name", "foo");
Sql.new("usr").select("id").where("(c>1 or d>1)").where("(a>1 or b>1)");
Sql.new("usr")
  .select("id")
  .where("a", "like", "thus%")
  .where("b", "in", [1, 2, "3"]);
Sql.new("profile").where("usrId", Sql.new("usr").select("id").where("id", 123));
Sql.new("usr");
Sql.new("usr").whereNot("id", 1);
Sql.new("usr").whereNot("id", ">", 1);
Sql.new("usr").whereNot({ id: 1, name: "foo" });
Sql.new("usr").whereNot({ id: 1, name: "foo" }).orWhere("id", 2);
Sql.new("usr").where("id", 1).orWhereNot("id", 2);
Sql.new("usr").where("id", 1).orWhereNot("id", ">", 2);
Sql.new("usr").where("id", 1).orWhereNot("id", "in", [1, 2, true]);
Sql.new("usr").whereExists(Sql.new("usr").select("id"));
Sql.new("usr").whereExists(Sql.new("usr").select("id")).orWhere("name", "foo");
Sql.new("usr").whereNotExists(Sql.new("usr").select("")).orWhere("name", "foo");
Sql.new("usr").whereIn("id", [1, 2, 3]);
Sql.new("usr").whereIn("id", [1, 2, 3]).whereIn("name", ["a", "b"]);
Sql.new("usr").whereIn(
  ["id", "name"],
  [
    [1, "a"],
    [2, "b"],
  ]
);
Sql.new("usr").whereIn(["id", "name"], Sql.new("usr2").select("id", "name"));
Sql.new("usr").whereIn("id", [1, 2, 3]).orWhereIn("name", ["a", "b"]);
Sql.new("usr").whereNotIn("id", [1, 2, 3]);
Sql.new("usr").whereNotIn(
  ["id", "name"],
  [
    [1, "a"],
    [2, "b"],
  ]
);
Sql.new("usr")
  .whereNotIn(
    ["id", "name"],
    [
      [1, "a"],
      [2, "b"],
    ]
  )
  .orWhereNotIn("id", [1, 2, 3]);
Sql.new("usr").whereNull("id");
Sql.new("usr").whereIn("id", [1, 2, 3]).whereNull("id");
Sql.new("usr").whereNotNull("id");
Sql.new("usr").whereNotNull("id").orWhereNull("name");
Sql.new("usr").whereNotNull("id").orWhereNotNull("name");
Sql.new("usr").whereBetween("id", 1, 100);
Sql.new("usr").where("id", 1).orWhereBetween("id", 1, 100);
Sql.new("usr").whereNotBetween("id", 1, 100);
Sql.new("usr").where("id", 1).orWhereNotBetween("id", 1, 100);
Sql.new("usr").whereRaw("id>1 or id<10");
Sql.new("usr").where("id", 1).orWhereRaw("(id>1 AND id<10)");
Sql.new("usr").whereRaw("id>1 or id<10").select(["id", "name"]);
Sql.new("usr").whereRaw("id>1 or id<10").select("id", "name");
Sql.new("usr")
  .where("id>1 or id<10")
  .select("id", "name")
  .group("name")
  .having({ cnt: 3 })
  .order("id", "name")
  .limit(3)
  .offset(2);
Sql.new("usr").select("id", "name").havingIn("id", [1, 2, 4]);
Sql.new("usr").insert(Sql.new("usr").select("id", "name").where("id", ">", 1));
Sql.new("usr").insert(Sql.new("usr").select("id", "name"), "idx, namex");
Sql.new("usr")
  .insert(Sql.new("usr").select("id", "name"), "idx, namex")
  .with("tmp", Sql.new("usr").select("id", "name"));
Sql.new("t").update({ a: 1, b: "foo", c: false });
Sql.new("t").update({ a: 1, b: "foo", c: false }).where({ d: 5 });
Sql.new("t")
  .update({ a: 1, b: "foo", c: false })
  .from("u", "v")
  .where("u.id=t.id and v.id=t.id")
  .where("t.id", 30);
Sql.new("usr").insert("(a,b,c) values (1,2,3)");
Sql.new("usr").merge([{ phone: "13508198871", xm: "x1" }], "phone");
Sql.new("usr").merge([{ phone: "13508198871" }], "phone");
Sql.new("usr").upsert({ phone: "13508198871", xm: "x1" }, "phone");
Sql.new("usr").upsert(
  [
    { phone: "13508198871", xm: "x1" },
    { phone: "12", xm: "x2" },
  ],
  "phone"
);
Sql.new("usr").upsert(
  [
    { phone: "13508198871", xm: "x1" },
    { phone: "12", xm: "x2" },
  ],
  ["phone", "xm"]
);
Sql.new("usr").upsert(
  [
    { phone: "13508198871", xm: "x1", a: 1 },
    { phone: "12", xm: "x2", b: 2 },
  ],
  ["phone", "xm"]
);
Sql.new("usr").upsert(
  [
    { phone: "13508198871", xm: "x1", a: 1 },
    { phone: "12", xm: "x2", b: 2 },
  ],
  ["phone", "xm"],
  ["phone", "xm", "c"]
);
Sql.new("usr").upsert({ phone: "13508198871", xm: "x1" }, ["phone", "xm"]);
Sql.new("usr").upsert({ phone: "13508198871" }, "phone");
Sql.new("usr").delete();
Sql.new("usr").delete({ xm: "foo" });
Sql.new("usr").delete().where({ xm: "foo" });
Sql.new("usr").delete("xm", "foo");
Sql.new("usr").delete("p.userId=usr.id and p.name='foo'").using("profile p");
Sql.new("usr").where("p.userId=usr.id and p.name='foo'").using("profile p");
Sql.new("t").update({ a: 1, b: Sql.r("b+1") });
Sql.new("t").update({ a: "a+1", b: Sql.r("b+1"), c: false });
Sql.new("t").update(Sql.new("s").select("x,y,z").where("t.sId=s.id"));
Sql.new("t").update(Sql.new("s").select("x,y,z").where("t.sId=s.id"), [
  "sX",
  "sY",
  "sZ",
]);
Sql.new("usr").whereNot({ a: 1, b: 2 });
Sql.new("usr").whereOr({ a: 1, b: 2 }).orWhere({ a: 1, b: 2 });
Sql.new("profile")
  .as("p")
  .join("usr uf", "uf.id = p.fatherId")
  .join("usr um", "um.id = p.motherId")
  .select(
    "uf.email as fatherEmail",
    "um.email as motherEmail",
    "p.name as yourname"
  )
  .where("p.name", "kate");
Sql.new("usr")
  .delete({ ["p.userId"]: Sql.r("usr.id"), ["info.name"]: "foo" })
  .using("profile p")
  .join("info", "p.infoId=info.id");
Sql.new("profile").updates(
  [{ cjgzsj: "2010-01-01", sfzh: "51152319870713001X" }],
  "sfzh"
);
Sql.new("profile").where("usrId", Sql.new("usr").select("id").where("id", 123));
Sql.new("t").where("isOk", ()=>true).where("id", ()=>'TRUE').whereIn("id", ()=>[1,'2',true]);
Sql.new("usr").gets([{ phone: "13508198871" }, { phone: "13508198872" }]);
Sql.new("usr").upsert(
  [
    { phone: "13508198871", xm: "x1", a: 1 },
    { phone: "12", xm: "x2", b: 2 },
  ],
  ["phone", "xm"],
  ["phone", "xm", "c"]
);
Sql.new("usr")
  .merge(
    [
      { phone: "13508198871", xm: "x1" },
      { phone: "12", xm: "x2" },
    ],
    ["phone"]
  )
  .returning("*");
Sql.new("usr")
  .merge(
    [
      { phone: "13508198871", xm: "x1", id: 1 },
      { phone: "12", xm: "x2", id: 2 },
    ],
    ["phone", "xm"]
  )
  .returning("*");
Sql.new("t").insert(Sql.new("t2").select("c1", "c2"), ["tc1", "tc2"]);
Sql.new("usr")
  .where(function () {
    return this.whereOr({ name: "foo", id__lt: 3 })
  })
  .where({ name__like: "a" });
Sql.new("usr")
  .merge(
    [
      { phone: "13508198871", xm: "x1" },
      { phone: "12", xm: "x2" },
    ],
    ["phone"]
  )
  .returning("*");
Sql.new("usr")
  .select("a", "b")
  .union(Sql.new("usr").select("c", "d"))
  .union(Sql.new("usr").select("e", "f"));
Sql.new("t").update({ a: 1, b: "foo" }, ["a", "b", "c"]);
Sql.new("t").insert(Sql.new("t2").select("c1", "c2"));
Sql.new("t").insert(Sql.new("t2").delete({ a: 1 }).returning("c1", "c2"));
Sql.new("usr").insert({ a: 1, b: "foo", c: false });
Sql.new("usr").insert([
  { a: "1", b: 2 },
  { b: "2", c: false },
]);
Sql.new("usr").insert(
  [
    { a: "1", b: 2 },
    { b: "2", c: 3 },
    { c: "4", d: 5 },
  ],
  ["b", "c"]
);`
let t = `Sql.new("usr")
  .merge(
    [
      { phone: "13508198871", xm: "x1" },
      { phone: "12", xm: "x2" },
    ],
    ["phone"]
  )
  .returning("*");
Sql.new("usr")
  .select("a", "b")
  .union(Sql.new("usr").select("c", "d"))
  .union(Sql.new("usr").select("e", "f"));`
for (const line of sqlLines.split(';')) {
  if (!line) {
    continue
  }
  console.log(`${line} =>`);
  console.log(`  ${eval(line).statement()}`);

}

test('select', () => {
  expect(Sql.new("usr").select("id", "name", "age").statement()).toBe("SELECT id, name, age FROM usr")
});