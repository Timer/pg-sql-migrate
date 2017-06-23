const co = require('co')
const fs = require('fs')
const path = require('path')
const debug = require('debug')('pg-sql-migrate')

function migrate(
  {
    client,
    pool,
    force = false,
    table = 'migrations',
    migrationsPath = './migrations',
  } = {}
) {
  let conn
  let cleanup
  return co(function*() {
    if ((client && pool) || (!client && !pool)) {
      throw new Error('You must specify a client *OR* pool.')
    }

    debug('connecting to database ...')
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

    debug('connected.')
    const location = path.resolve(migrationsPath)
    const migrations = yield new Promise((resolve, reject) => {
      fs.readdir(location, (err, files) => {
        if (err) {
          reject(err)
        } else {
          resolve(
            files
              .map(x => x.match(/^(\d+).(.*?)\.sql$/))
              .filter(x => x !== null)
              .map(x => ({ id: Number(x[1]), name: x[2], filename: x[0] }))
              .sort((a, b) => Math.sign(a.id - b.id))
          )
        }
      })
    })

    if (!migrations.length) {
      throw new Error(`No migration files found in '${location}'.`)
    }

    debug('loaded %d migrations', migrations.length)
    yield Promise.all(
      migrations.map(
        migration =>
          new Promise((resolve, reject) => {
            const filename = path.join(location, migration.filename)
            fs.readFile(filename, 'utf-8', (err, data) => {
              if (err) {
                reject(err)
              } else {
                const [up, down] = data.split(/^--\s+?down/im)
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
          })
      )
    )

    debug('ensuring migration table (%s) exists', table)
    yield conn.query(
      `create table if not exists "${table}" (
        id integer primary key,
        name text not null,
        up text not null,
        down text not null
      )`
    )

    debug('listing existing migrations ...')
    let dbMigrations = yield conn.query(
      `select id, name, up, down from "${table}" order by id asc`
    )
    dbMigrations = dbMigrations.rows
    debug('... has %d migrations', dbMigrations.length)

    const lastMigration = migrations[migrations.length - 1]
    for (const migration of dbMigrations.slice().sort((a, b) => Math.sign(b.id - a.id))) {
      if (
        !migrations.some(x => x.id === migration.id) ||
        (force === 'last' && migration.id === lastMigration.id)
      ) {
        debug('rolling back migration %d', migration.id)
        yield conn.query('begin')
        try {
          yield conn.query(migration.down)
          yield conn.query(`delete from "${table}" where id = $1`, [
            migration.id,
          ])
          yield conn.query('commit')
          dbMigrations = dbMigrations.filter(x => x.id !== migration.id)
        } catch (err) {
          yield conn.query('rollback')
          throw err
        }
      } else {
        break
      }
    }

    const lastMigrationId = dbMigrations.length
      ? dbMigrations[dbMigrations.length - 1].id
      : 0
    for (const migration of migrations) {
      if (migration.id > lastMigrationId) {
        debug('applying migration %d', migration.id)
        yield conn.query('begin')
        try {
          yield conn.query(migration.up)
          yield conn.query(
            `insert into "${table}" (id, name, up, down) values ($1, $2, $3, $4)`,
            [migration.id, migration.name, migration.up, migration.down]
          )
          yield conn.query('commit')
        } catch (err) {
          yield conn.query('rollback')
          throw err
        }
      }
    }

    debug('done')
  })
    .then(() => {
      if (typeof cleanup === 'function') cleanup()
    })
    .catch(e => {
      if (typeof cleanup === 'function') cleanup()
      throw e
    })
}

module.exports = migrate
