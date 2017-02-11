const co = require('co')
const fs = require('fs')
const path = require('path')

function migrate({
  client,
  pool,
  force = false,
  table = 'migrations',
  migrationsPath = './migrations'
} = {}) {
  let conn
  let cleanup
  return co(function*() {
    if (client && pool || !client && !pool) {
      throw new Error('You must specify a client *OR* pool.')
    }

    if (client) {
      client.connect()
      conn = client
      cleanup = client.end.bind(client)
    } else if (pool) {
      yield new Promise((resolve, reject) => {
        pool.connect(function(err, client, done) {
          if (err) {
            reject(err)
            return
          }

          conn = client
          cleanup = () => {
            client.end()
            done(new Error('Trash this connection, please.'))
          }
          resolve()
        })
      })
    }

    const location = path.resolve(migrationsPath)
    const migrations = yield new Promise((resolve, reject) => {
      fs.readdir(location, (err, files) => {
        if (err) {
          reject(err)
        } else {
          resolve(files
            .map(x => x.match(/^(\d+).(.*?)\.sql$/))
            .filter(x => x !== null)
            .map(x => ({ id: Number(x[1]), name: x[2], filename: x[0] }))
            .sort((a, b) => Math.sign(a.id - b.id)))
        }
      })
    })

    if (!migrations.length) {
      throw new Error(`No migration files found in '${location}'.`)
    }

    yield Promise.all(migrations.map(migration => new Promise((resolve, reject) => {
      const filename = path.join(location, migration.filename)
      fs.readFile(filename, 'utf-8', (err, data) => {
        if (err) {
          reject(err)
        } else {
          const [up, down] = data.split(/^--\s+?down/mi)
          if (!down) {
            const message = `The ${migration.filename} file does not contain '-- Down' separator.`
            reject(new Error(message))
          } else {
            migration.up = up.replace(/^--.*?$/gm, '').trim()
            migration.down = down.replace(/^--.*?$/gm, '').trim()
            resolve()
          }
        }
      })
    })))

    yield conn.query(
      `CREATE TABLE IF NOT EXISTS "${table}" (
        id   INTEGER PRIMARY KEY,
        name TEXT    NOT NULL,
        up   TEXT    NOT NULL,
        down TEXT    NOT NULL
      )`
    )
    let dbMigrations = yield conn.query(
      `SELECT id, name, up, down FROM "${table}" ORDER BY id ASC`
    )
    dbMigrations = dbMigrations.rows

    const lastMigration = migrations[migrations.length - 1]
    for (const migration of dbMigrations.slice().sort((a, b) => Math.sign(b.id - a.id))) {
      if (!migrations.some(x => x.id === migration.id) ||
        (force === 'last' && migration.id === lastMigration.id)) {
        yield conn.query('BEGIN')
        try {
          yield conn.query(migration.down)
          yield conn.query(`DELETE FROM "${table}" WHERE id = $1`, [migration.id])
          yield conn.query('COMMIT')
          dbMigrations = dbMigrations.filter(x => x.id !== migration.id)
        } catch (err) {
          yield conn.query('ROLLBACK')
          throw err
        }
      } else {
        break
      }
    }

    const lastMigrationId = dbMigrations.length ? dbMigrations[dbMigrations.length - 1].id : 0
    for (const migration of migrations) {
      if (migration.id > lastMigrationId) {
        yield conn.query('BEGIN')
        try {
          yield conn.query(migration.up)
          yield conn.query(
            `INSERT INTO "${table}" (id, name, up, down) VALUES ($1, $2, $3, $4)`,
            [migration.id, migration.name, migration.up, migration.down]
          )
          yield conn.query('COMMIT')
        } catch (err) {
          yield conn.query('ROLLBACK')
          throw err
        }
      }
    }
  }).then(() => {
    if (typeof cleanup === 'function') cleanup()
  }).catch(e => {
    if (typeof cleanup === 'function') cleanup()
    throw e
  })
}

module.exports = migrate
