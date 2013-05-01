var should = require('should'),
    async = require('async'),
    dynamo = require('dynamo-client'),
    dynamoTable = require('..'),
    table = null

function deleteAndWait(cb) {
  table.describeTable(function(err, data) {
    if (err && err.name === 'ResourceNotFoundException') return cb()
    if (err) return cb(err)

    if (data.TableStatus !== 'ACTIVE') return setTimeout(deleteAndWait, 5000, cb)

    table.deleteTable(function(err, data) {
      if (err) return cb(err)

      setTimeout(deleteAndWait, 5000, cb)
    })
  })
}

function createAndWait(cb) {
  table.describeTable(function(err, data) {
    if (err && err.name === 'ResourceNotFoundException') {
      return table.createTable(function(err, data) {
        if (err) return cb(err)

        setTimeout(createAndWait, 5000, cb)
      })
    }
    if (err) return cb(err)

    if (data.TableStatus === 'ACTIVE') return cb()
    if (data.TableStatus !== 'CREATING') return cb(new Error(data.TableStatus))

    setTimeout(createAndWait, 5000, cb)
  })
}

before(function(done) {
  if (!process.env.AWS_SECRET_ACCESS_KEY || !process.env.AWS_ACCESS_KEY_ID)
    throw new Error('Must set AWS_SECRET_ACCESS_KEY and AWS_ACCESS_KEY_ID env vars')

  table = dynamoTable('dynamo-client-integration-test', {
    region: process.env.AWS_REGION,
    key: ['forumName', 'subject'],
    mappings: {forumName: 'S', subject: 'S', lastPostTime: 'isodate'},
    indexes: {postIx: 'lastPostTime'},
  })

  async.series([deleteAndWait, createAndWait], done)
})

after(deleteAndWait)

beforeEach(function(done) {
  table.scan(function(err, items) {
    if (err || !items.length) return done(err)

    var ids = items.map(function(item) {
      return {forumName: item.forumName, subject: item.subject}
    })

    table.batchWrite({deletes: ids}, done)
  })
})

describe('put', function() {
  it('should put basic item', function(done) {
    table.put({forumName: 'a', subject: 'b', lastPostTime: new Date}, done)
  })
})

describe('get', function() {
  it('should return nothing if nothing in DB', function(done) {
    table.get(['a', 'b'], {ConsistentRead: true}, function(err, jsObj) {
      if (err) return done(err)
      should.not.exist(jsObj)
      done()
    })
  })

  it('should return added item', function(done) {
    var now = new Date
    async.series([
      table.put.bind(table, {forumName: 'a', subject: 'b', lastPostTime: now}),
      table.get.bind(table, ['a', 'b'], {ConsistentRead: true})
    ], function(err, results) {
      if (err) return done(err)
      results[1].should.eql({forumName: 'a', subject: 'b', lastPostTime: now})
      done()
    })
  })
})

describe('query', function() {

  it('should return matching items on range key', function(done) {
    var now = new Date
    async.series([
      table.put.bind(table, {forumName: 'a', subject: 'a', lastPostTime: now}),
      table.put.bind(table, {forumName: 'a', subject: 'b', lastPostTime: now}),
      table.put.bind(table, {forumName: 'a', subject: 'c', lastPostTime: now}),
      table.query.bind(table, {forumName: 'a', subject: {'>': 'a'}}, {ConsistentRead: true})
    ], function(err, results) {
      if (err) return done(err)
      results[3].should.eql([
        {forumName: 'a', subject: 'b', lastPostTime: now},
        {forumName: 'a', subject: 'c', lastPostTime: now},
      ])
      done()
    })
  })

  it('should return matching items on index', function(done) {
    var now = new Date, now1 = new Date(+now + 1), now2 = new Date(+now + 2)
    async.series([
      table.put.bind(table, {forumName: 'a', subject: 'a', lastPostTime: now}),
      table.put.bind(table, {forumName: 'a', subject: 'b', lastPostTime: now1}),
      table.put.bind(table, {forumName: 'a', subject: 'c', lastPostTime: now2}),
      table.query.bind(table, {forumName: 'a', lastPostTime: {'>': now}}, {IndexName: 'postIx', ConsistentRead: true})
    ], function(err, results) {
      if (err) return done(err)
      results[3].should.eql([
        {forumName: 'a', subject: 'b', lastPostTime: now1},
        {forumName: 'a', subject: 'c', lastPostTime: now2},
      ])
      done()
    })
  })
})

describe('listTables', function() {
  it('should return single table', function(done) {
    table.listTables(function(err, tables) {
      if (err) return done(err)
      tables.should.include('dynamo-client-integration-test')
      done()
    })
  })
})

describe('updateTable', function() {
  it('should update capacity', function(done) {
    table.updateTable(2, 2, function(err, info) {
      if (err) {
        if (err.name === 'ResourceInUseException' || err.name === 'LimitExceededException') return done()
        if (/requested value equals the current value/.test(err)) {
          return table.updateTable(1, 1, function(err) {
            if (err && err.name === 'LimitExceededException') err = null
            done(err)
          })
        }
        return done(err)
      }
      info.TableStatus.should.equal('UPDATING')
      done()
    })
  })
})

describe('nextId', function() {

  beforeEach(function(done) {
    table.initId(done)
  })

  it('should increment from scratch', function(done) {
    table.nextId(function(err, newVal) {
      if (err) return done(err)
      newVal.should.equal(1)
      done()
    })
  })

  it('should increment multiple times in series', function(done) {
    var calls = [], i
    for (i = 0; i < 5; i++)
      calls.push(table.nextId.bind(table))
    async.series(calls, function(err, results) {
      if (err) return done(err)
      results[4].should.equal(5)
      done()
    })
  })

  it('should increment multiple times in parallel', function(done) {
    var calls = [], i
    for (i = 0; i < 20; i++)
      calls.push(table.nextId.bind(table))
    async.parallel(calls, function(err, results) {
      if (err) return done(err)
      for (i = 1; i <= 20; i++)
        results.should.include(i)
      done()
    })
  })
})


