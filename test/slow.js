var should = require('should'),
    async = require('async'),
    fakeDynamoGem = require('fake-dynamo-gem'),
    dynamoTable = require('..'),
    useLive = process.env.USE_LIVE_DYNAMO, // set this (and AWS credentials) if you want to test on a live instance
    region = process.env.AWS_REGION, // will just default to us-east-1 if not specified
    table

describe('integration', function() {

  before(function(done) {
    var setup = function(cb) { cb() }

    if (!useLive) {
      region = {host: 'localhost', port: 4567, credentials: {accessKeyId: 'a', secretAccessKey: 'a'}}
      setup = fakeDynamoGem.bind(null, ['--db', '/tmp/' + Date.now() + '.fdb'])
    }

    table = dynamoTable('dynamo-client-integration-test', {
      region: region,
      key: ['forumName', 'subject'],
      mappings: {forumName: 'S', subject: 'S', lastPostTime: 'isodate'},
      indexes: {postIx: 'lastPostTime'},
    })

    setup(function(err) {
      if (err) return done(err)
      async.series([table.deleteAndWait.bind(table), table.createAndWait.bind(table)], done)
    })
  })

  after(function (done) {
    table.deleteAndWait(done)
  })

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
          if (err.name === 'ResourceInUseException' || err.name === 'LimitExceededException')
            return done()
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

  describe('increment', function() {
    var key = {forumName: '0', subject: '0'}

    beforeEach(function(done) {
      table.update(key, {put: {lastId: 0}}, done)
    })

    it('should increment from scratch', function(done) {
      table.increment(key, 'lastId', function(err, newVal) {
        if (err) return done(err)
        newVal.should.equal(1)
        done()
      })
    })

    it('should increment multiple times in series', function(done) {
      var calls = [], i
      for (i = 0; i < 5; i++)
        calls.push(table.increment.bind(table, key, 'lastId'))
      async.series(calls, function(err, results) {
        if (err) return done(err)
        results[4].should.equal(5)
        done()
      })
    })

    it('should increment multiple times in parallel', function(done) {
      var calls = [], i
      for (i = 0; i < 20; i++)
        calls.push(table.increment.bind(table, key, 'lastId'))
      async.parallel(calls, function(err, results) {
        if (err) return done(err)
        for (i = 1; i <= 20; i++)
          results.should.include(i)
        done()
      })
    })
  })

})
