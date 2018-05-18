const co = require('co')
const fs = require('fs')
const path = require('path')
const debug = require('debug')('pg-sql-migrate')
const crypto = require('crypto')

function migrate(
  {
    client,
    pool,
    force = false,
    table = 'migrations',
    migrationsPath = './migrations',
    checkHash = false,
    validateDown = true,
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
                  migration.hash = crypto
                    .createHash('sha512')
                    .update(data.replace(/\r\n/g, '\n'))
                    .digest('hex')
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
        down text not null,
        hash text not null
      )`
    )

    const hashColumnCount = yield conn
      .query(
        `select column_name from information_schema.columns where table_name = '${table}' and column_name = 'hash';`
      )
      .then(r => r.rows.length)

    if (hashColumnCount < 1) {
      yield conn.query(
        `alter table "${table}" add column hash text not null default 'nil';`
      )
      yield conn.query(`alter table "${table}" alter column hash drop default;`)
    }

    debug('listing existing migrations ...')
    let dbMigrations = yield conn.query(
      `select id, name, up, down, hash from "${table}" order by id asc`
    )
    dbMigrations = dbMigrations.rows
    debug('... has %d migrations', dbMigrations.length)

    let firstMismatch = -1
    for (const { id, hash } of dbMigrations) {
      const file = migrations.find(x => x.id === id)
      if (file == null) {
        continue
      }
      if (hash === 'nil') {
        yield conn.query(`update "${table}" set hash = $1 where id = $2`, [
          file.hash,
          id,
        ])
      } else if (
        file.hash !== hash &&
        (firstMismatch === -1 || id < firstMismatch)
      ) {
        firstMismatch = id
      }
    }
    if (checkHash && firstMismatch !== -1) {
      debug('... %d has a hash mismatch, rolling back ...', firstMismatch)
    }

    const lastMigration = migrations[migrations.length - 1]
    for (const {
      id,
      down,
    } of dbMigrations.slice().sort((a, b) => Math.sign(b.id - a.id))) {
      if (
        !migrations.some(x => x.id === id) ||
        (checkHash && firstMismatch !== -1 && id >= firstMismatch) ||
        (force === 'last' && id === lastMigration.id)
      ) {
        debug('rolling back migration %d', id)
        yield conn.query('begin')
        try {
          yield conn.query(down)
          yield conn.query(`delete from "${table}" where id = $1`, [id])
          yield conn.query('commit')
          dbMigrations = dbMigrations.filter(x => x.id !== id)
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
    for (const { id, name, up, down, hash } of migrations) {
      if (id > lastMigrationId) {
        debug('applying migration %d', id)
        yield conn.query('begin')
        try {
          yield conn.query(up)
          if (validateDown) {
            yield conn.query(down)
            yield conn.query(up)
          }
          yield conn.query(
            `insert into "${table}" (id, name, up, down, hash) values ($1, $2, $3, $4, $5)`,
            [id, name, up, down, hash]
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
      if (typeof cleanup === 'function') {
        cleanup()
      }
    })
    .catch(e => {
      if (typeof cleanup === 'function') {
        cleanup()
      }
      throw e
    })
}

module.exports = migrate
