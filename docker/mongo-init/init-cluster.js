// Ініціалізація кластера MongoDB. Запускається контейнером mongo-init:
//   mongosh --nodb --file /init/init-cluster.js
// 1) створює replica set на сервері конфігурації та на кожному шарді;
// 2) реєструє три шарди в mongos.
// Скрипт ідемпотентний: повторний запуск нічого не ламає.
// Шардування колекції medicines виконує застосунок (MongoDocumentAdapter.ensure_sharding).
//
// Увага: mongosh виконує команди до БД асинхронно, тому тут свідомо немає
// функцій-зворотних викликів (forEach, retry(fn)): лише прості цикли з try/catch.

const CONFIG = { id: "cfgrs", host: "configsvr:27019", configsvr: true };
const SHARDS = [
  { id: "shard1rs", host: "shard1:27018" },
  { id: "shard2rs", host: "shard2:27018" },
  { id: "shard3rs", host: "shard3:27018" },
];
const MONGOS = "mongos:27017";
const ATTEMPTS = 60;

// Підключення до окремого вузла (до ініціалізації replica set потрібен directConnection).
function connectAdmin(host) {
  for (let i = 1; i <= ATTEMPTS; i++) {
    try {
      const admin = new Mongo(`mongodb://${host}/?directConnection=true`).getDB("admin");
      admin.runCommand({ ping: 1 });
      return admin;
    } catch (e) {
      print(`  ${host}: очікую підключення (${i}/${ATTEMPTS}) - ${e.message}`);
      sleep(2000);
    }
  }
  throw new Error(`${host}: не вдалося підключитися`);
}

// Чекає, поки вузол стане PRIMARY (після replSetInitiate це займає кілька секунд).
function waitPrimary(admin, name) {
  for (let i = 1; i <= ATTEMPTS; i++) {
    let isPrimary = false;
    try {
      isPrimary = admin.runCommand({ hello: 1 }).isWritablePrimary === true;
    } catch (e) {
      print(`  ${name}: помилка перевірки - ${e.message}`);
    }
    if (isPrimary) {
      print(`${name}: PRIMARY готовий`);
      return;
    }
    print(`  ${name}: очікую вибору PRIMARY (${i}/${ATTEMPTS})`);
    sleep(1000);
  }
  throw new Error(`${name}: PRIMARY так і не обрано`);
}

function initReplicaSet(rs) {
  const admin = connectAdmin(rs.host);
  const config = { _id: rs.id, members: [{ _id: 0, host: rs.host }] };
  if (rs.configsvr) config.configsvr = true;

  // У mongosh runCommand при помилці кидає виняток (MongoServerError), а не повертає ok: 0.
  try {
    admin.runCommand({ replSetInitiate: config });
    print(`${rs.id}: replica set створено`);
  } catch (e) {
    if (e.code === 23 || e.codeName === "AlreadyInitialized") {   // повторний запуск
      print(`${rs.id}: вже ініціалізовано`);
    } else {
      throw e;
    }
  }
  waitPrimary(admin, rs.id);
}

// --- 1. Replica set-и ---
initReplicaSet(CONFIG);
for (const shard of SHARDS) {
  initReplicaSet(shard);
}

// --- 2. mongos: чекаємо, поки він побачить сервер конфігурації ---
let mongosAdmin = null;
for (let i = 1; i <= ATTEMPTS && mongosAdmin === null; i++) {
  try {
    const admin = new Mongo(`mongodb://${MONGOS}`).getDB("admin");
    const res = admin.runCommand({ listShards: 1 });
    if (res.ok) {
      mongosAdmin = admin;
    } else {
      print(`  mongos: ще не готовий (${i}/${ATTEMPTS}) - ${res.errmsg}`);
      sleep(2000);
    }
  } catch (e) {
    print(`  mongos: очікую підключення (${i}/${ATTEMPTS}) - ${e.message}`);
    sleep(2000);
  }
}
if (mongosAdmin === null) {
  throw new Error("mongos: не вдалося підключитися");
}

// --- 3. Реєстрація шардів ---
for (const shard of SHARDS) {
  let added = false;
  for (let i = 1; i <= ATTEMPTS && !added; i++) {
    let res;
    try {
      res = mongosAdmin.runCommand({ addShard: `${shard.id}/${shard.host}`, name: shard.id });
    } catch (e) {
      res = { ok: 0, errmsg: e.message };
    }
    if (res.ok) {
      added = true;
      print(`${shard.id}: зареєстровано в mongos`);
    } else {
      print(`  ${shard.id}: addShard не вдався (${i}/${ATTEMPTS}) - ${res.errmsg}`);
      sleep(2000);
    }
  }
  if (!added) {
    throw new Error(`${shard.id}: не вдалося додати шард`);
  }
}

const shardList = mongosAdmin.runCommand({ listShards: 1 }).shards;
const names = [];
for (const s of shardList) {
  names.push(`${s._id} (${s.host})`);
}
print(`Готово. Шарди кластера: ${names.join(", ")}`);
