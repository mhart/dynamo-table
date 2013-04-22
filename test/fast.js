var should = require('should'),
    dynamoTable = require('..')

// ensure env variables have content to keep dynamo-client happy
before(function() {
  process.env.AWS_SECRET_ACCESS_KEY = 'a'
  process.env.AWS_ACCESS_KEY_ID = 'a'
})

describe('constructor', function() {
  it('should throw if no name', function() {
    dynamoTable.bind().should.throw()
    dynamoTable.bind(null, '').should.throw()
    dynamoTable.bind(null, 'a').should.not.throw()
  })

  it('should assign the table name', function() {
    var table = dynamoTable('MyTable')
    table.name.should.equal('MyTable')
  })

  it('should create default key of id', function() {
    var table = dynamoTable('name')
    table.key.should.eql(['id'])
  })

  it('should use mappings if no key', function() {
    var table = dynamoTable('name', {mappings: {id: 'N', orderId: 'S'}})
    table.key.should.eql(['id', 'orderId'])
  })

  it('should convert string key to array', function() {
    var table = dynamoTable('name', {key: 'orderId'})
    table.key.should.eql(['orderId'])
  })
})

describe('mapAttrToDb', function() {
  it('should guess default types', function() {
    var table = dynamoTable('name')
    table.mapAttrToDb('hello').should.eql({S: 'hello'})
    table.mapAttrToDb('23').should.eql({S: '23'})
    table.mapAttrToDb('"hello"').should.eql({S: '"hello"'})
    table.mapAttrToDb(true).should.eql({S: 'true'})
    table.mapAttrToDb(false).should.eql({S: 'false'})
    table.mapAttrToDb(100).should.eql({N: '100'})
    table.mapAttrToDb(100.25).should.eql({N: '100.25'})
    table.mapAttrToDb(new Buffer([1,2,3,4])).should.eql({B: 'AQIDBA=='})
    table.mapAttrToDb(['1', '2', '3']).should.eql({SS: ['1', '2', '3']})
    table.mapAttrToDb([1, 2, 3]).should.eql({NS: ['1', '2', '3']})
    table.mapAttrToDb([new Buffer([1]), new Buffer([2]), new Buffer([3])])
      .should.eql({BS: ['AQ==', 'Ag==', 'Aw==']})
    table.mapAttrToDb({id: 23}).should.eql({S: '{"id":23}'})
  })

  it('should use mapping function if it exists', function() {
    var table = dynamoTable('name', {mappings: {id: {to: function() { return {S: '23'} }}}})
    table.mapAttrToDb(100, 'id').should.eql({S: '23'})
  })

  it('should map explicit dynamodb types', function() {
    var table = dynamoTable('name', {mappings: {
      name: 'S',
      id: 'N',
      image: 'B',
      childNames: 'SS',
      childIds: 'NS',
      childImages: 'BS',
    }})
    table.mapAttrToDb('[1,2,3]', 'name').should.eql({S: '[1,2,3]'})
    table.mapAttrToDb(100, 'id').should.eql({N: '100'})
    table.mapAttrToDb(new Buffer([1,2,3,4]), 'image').should.eql({B: 'AQIDBA=='})
    table.mapAttrToDb(['1', '2', '3'], 'childNames').should.eql({SS: ['1', '2', '3']})
    table.mapAttrToDb([1, 2, 3], 'childIds').should.eql({NS: ['1', '2', '3']})
    table.mapAttrToDb([new Buffer([1]), new Buffer([2]), new Buffer([3])], 'childImages')
      .should.eql({BS: ['AQ==', 'Ag==', 'Aw==']})
  })

  it('should map explicit json type', function() {
    var table = dynamoTable('name', {mappings: {id: 'json'}})
    table.mapAttrToDb([1, 2, 3], 'id').should.eql({S: '[1,2,3]'})
    table.mapAttrToDb({id: 23}, 'id').should.eql({S: '{"id":23}'})
    table.mapAttrToDb(true, 'id').should.eql({S: 'true'})
    table.mapAttrToDb(false, 'id').should.eql({S: 'false'})
    table.mapAttrToDb(23, 'id').should.eql({S: '23'})
    table.mapAttrToDb('hello', 'id').should.eql({S: '"hello"'})
    table.mapAttrToDb('', 'id').should.eql({S: '""'})
    table.mapAttrToDb(null, 'id').should.eql({S: 'null'})
  })

  it('should map explicit bignum type', function() {
    var table = dynamoTable('name', {mappings: {id: 'bignum'}})
    table.mapAttrToDb('23', 'id').should.eql({N: '23'})
    table.mapAttrToDb('9999999999999999', 'id').should.eql({N: '9999999999999999'})
    table.mapAttrToDb('23.123512341234125678', 'id').should.eql({N: '23.123512341234125678'})
    table.mapAttrToDb(['1', '2', '3'], 'id').should.eql({NS: ['1', '2', '3']})
  })

  it('should map explicit isodate type', function() {
    var table = dynamoTable('name', {mappings: {id: 'isodate'}})
    table.mapAttrToDb(new Date(1345534683133), 'id').should.eql({S: '2012-08-21T07:38:03.133Z'})
  })

  it('should map explicit timestamp type', function() {
    var table = dynamoTable('name', {mappings: {id: 'timestamp'}})
    table.mapAttrToDb(new Date('2012-08-21T07:38:03.133Z'), 'id').should.eql({N: '1345534683133'})
  })

  it('should map explicit object map types', function() {
    var table = dynamoTable('name', {mappings: {a: 'mapS', b: 'mapN', c: 'mapB'}})
    table.mapAttrToDb({'1': 1, '2': 1, '3': 1}, 'a').should.eql({SS: ['1', '2', '3']})
    table.mapAttrToDb({'1': 1, '2': 1, '3': 1}, 'b').should.eql({NS: ['1', '2', '3']})
    table.mapAttrToDb({'AQ==': 1, 'Ag==': 1, 'Aw==': 1}, 'c').should.eql({BS: ['AQ==', 'Ag==', 'Aw==']})
  })

  it('should return undefined for empty values by default', function() {
    var table = dynamoTable('name', {mappings: {id: 'S'}})
    should.strictEqual(table.mapAttrToDb(), undefined)
    should.strictEqual(table.mapAttrToDb(undefined), undefined)
    should.strictEqual(table.mapAttrToDb(null), undefined)
    should.strictEqual(table.mapAttrToDb(''), undefined)
    should.strictEqual(table.mapAttrToDb([]), undefined)
    should.strictEqual(table.mapAttrToDb(new Buffer([])), undefined)
    should.strictEqual(table.mapAttrToDb(null, 'id'), undefined)
    should.strictEqual(table.mapAttrToDb('', 'id'), undefined)
  })
})

describe('mapAttrFromDb', function() {
  it('should use default mappings', function() {
    var table = dynamoTable('name')
    table.mapAttrFromDb({S: 'hello'}).should.equal('hello')
    table.mapAttrFromDb({S: '[1,2,3]'}).should.eql([1, 2, 3])
    table.mapAttrFromDb({S: '{"id":23}'}).should.eql({id: 23})
    table.mapAttrFromDb({S: 'true'}).should.equal(true)
    table.mapAttrFromDb({S: 'false'}).should.equal(false)
    table.mapAttrFromDb({S: '23'}).should.equal('23')
    table.mapAttrFromDb({S: '"hello"'}).should.equal('"hello"')
    table.mapAttrFromDb({N: '100'}).should.equal(100)
    table.mapAttrFromDb({N: '100.25'}).should.equal(100.25)
    table.mapAttrFromDb({B: 'AQIDBA=='}).should.eql(new Buffer([1,2,3,4]))
    table.mapAttrFromDb({SS: ['1', '2', '3']}).should.eql(['1', '2', '3'])
    table.mapAttrFromDb({NS: ['1', '2', '3']}).should.eql([1, 2, 3])
    table.mapAttrFromDb({BS: ['AQ==', 'Ag==', 'Aw==']})
      .should.eql([new Buffer([1]), new Buffer([2]), new Buffer([3])])
  })

  it('should use mapping function if it exists', function() {
    var table = dynamoTable('name', {mappings: {id: {from: function() { return 23 }}}})
    table.mapAttrFromDb({N: '100'}, 'id').should.equal(23)
  })

  it('should map explicit dynamodb types', function() {
    var table = dynamoTable('name', {mappings: {
      name: 'S',
      id: 'N',
      image: 'B',
      childNames: 'SS',
      childIds: 'NS',
      childImages: 'BS',
    }})
    table.mapAttrFromDb({S: '[1,2,3]'}, 'name').should.equal('[1,2,3]')
    table.mapAttrFromDb({N: '100'}, 'id').should.equal(100)
    table.mapAttrFromDb({B: 'AQIDBA=='}, 'image').should.eql(new Buffer([1,2,3,4]))
    table.mapAttrFromDb({SS: ['1', '2', '3']}, 'childNames').should.eql(['1', '2', '3'])
    table.mapAttrFromDb({NS: ['1', '2', '3']}, 'childIds').should.eql([1, 2, 3])
    table.mapAttrFromDb({BS: ['AQ==', 'Ag==', 'Aw==']}, 'childImages')
      .should.eql([new Buffer([1]), new Buffer([2]), new Buffer([3])])
  })

  it('should map explicit json type', function() {
    var table = dynamoTable('name', {mappings: {id: 'json'}})
    table.mapAttrFromDb({S: '[1,2,3]'}, 'id').should.eql([1, 2, 3])
    table.mapAttrFromDb({S: '{"id":23}'}, 'id').should.eql({id: 23})
    table.mapAttrFromDb({S: 'true'}, 'id').should.equal(true)
    table.mapAttrFromDb({S: 'false'}, 'id').should.equal(false)
    table.mapAttrFromDb({S: '23'}, 'id').should.equal(23)
    table.mapAttrFromDb({S: '"hello"'}, 'id').should.equal('hello')
    table.mapAttrFromDb({S: '""'}, 'id').should.equal('')
    should.strictEqual(table.mapAttrFromDb({S: 'null'}, 'id'), null)
  })

  it('should map explicit bignum type', function() {
    var table = dynamoTable('name', {mappings: {id: 'bignum'}})
    table.mapAttrFromDb({N: '23'}, 'id').should.equal('23')
    table.mapAttrFromDb({N: '9999999999999999'}, 'id').should.equal('9999999999999999')
    table.mapAttrFromDb({N: '23.123512341234125678'}, 'id').should.equal('23.123512341234125678')
    table.mapAttrFromDb({NS: ['1', '2', '3']}, 'id').should.eql(['1', '2', '3'])
  })

  it('should map explicit isodate type', function() {
    var table = dynamoTable('name', {mappings: {id: 'isodate'}})
    table.mapAttrFromDb({S: '2012-08-21T07:38:03.133Z'}, 'id').should.eql(new Date(1345534683133))
  })

  it('should map explicit timestamp type', function() {
    var table = dynamoTable('name', {mappings: {id: 'timestamp'}})
    table.mapAttrFromDb({N: 1345534683133}, 'id').should.eql(new Date('2012-08-21T07:38:03.133Z'))
  })

  it('should map explicit object map types', function() {
    var table = dynamoTable('name', {mappings: {a: 'mapS', b: 'mapN', c: 'mapB'}})
    table.mapAttrFromDb({SS: ['1', '2', '3']}, 'a').should.eql({'1': 1, '2': 1, '3': 1})
    table.mapAttrFromDb({NS: ['1', '2', '3']}, 'b').should.eql({'1': 1, '2': 1, '3': 1})
    table.mapAttrFromDb({BS: ['AQ==', 'Ag==', 'Aw==']}, 'c').should.eql({'AQ==': 1, 'Ag==': 1, 'Aw==': 1})
  })
})

describe('mapToDb', function() {
  it('should return null when passed null', function() {
    var table = dynamoTable('name')
    should.strictEqual(table.mapToDb(null), null)
  })

  it('should map to JS objects', function() {
    var table = dynamoTable('name')
    table.mapToDb({
      a: 1,
      b: 'a',
      c: new Buffer([1,2,3,4]),
      d: ['a', 'b'],
      e: [1, 2],
      f: [new Buffer([1]), new Buffer([2]), new Buffer([3])],
      g: {id: 23},
    }).should.eql({
      a: {N: '1'},
      b: {S: 'a'},
      c: {B: 'AQIDBA=='},
      d: {SS: ['a', 'b']},
      e: {NS: ['1', '2']},
      f: {BS: ['AQ==', 'Ag==', 'Aw==']},
      g: {S: '{"id":23}'},
    })
  })

  it('should exclude empty properties', function() {
    var table = dynamoTable('name')
    table.mapToDb({a: 1, b: '', c: null, d: [], e: new Buffer([])}).should.eql({a: {N: '1'}})
  })
})

describe('mapFromDb', function() {
  it('should return null when passed null', function() {
    var table = dynamoTable('name')
    should.strictEqual(table.mapFromDb(null), null)
  })

  it('should map from DB items', function() {
    var table = dynamoTable('name')
    table.mapFromDb({
      a: {N: '1'},
      b: {S: 'a'},
      c: {B: 'AQIDBA=='},
      d: {SS: ['a', 'b']},
      e: {NS: ['1', '2']},
      f: {BS: ['AQ==', 'Ag==', 'Aw==']},
      g: {S: '{"id":23}'},
    }).should.eql({
      a: 1,
      b: 'a',
      c: new Buffer([1,2,3,4]),
      d: ['a', 'b'],
      e: [1, 2],
      f: [new Buffer([1]), new Buffer([2]), new Buffer([3])],
      g: {id: 23},
    })
  })

  it('should throw if unrecognised type', function() {
    var table = dynamoTable('name')
    table.mapFromDb.bind(table, {a: {N: '1'}, b: {S: 'a'}, c: {G: 'a'}}).should.throw()
    table.mapFromDb.bind(table, {a: {N: '1'}, b: {S: 'a'}, c: {S: 'a'}}).should.not.throw()
  })

  it('should exclude mapped properties returning undefined', function() {
    var table = dynamoTable('name', {mappings: {a: {from: function() { return undefined }}}})
    table.mapFromDb({a: {N: '1'}, b: {S: 'a'}}).should.eql({b: 'a'})
  })
})

describe('resolveKey', function() {
  it('should resolve as "id" when no keys specified', function() {
    var table = dynamoTable('name')
    table.resolveKey(23).should.eql({id: {N: '23'}})
  })

  it('should resolve as single key when specified', function() {
    var table = dynamoTable('name', {key: 'name'})
    table.resolveKey('john').should.eql({name: {S: 'john'}})
  })

  it('should resolve as compound key when specified', function() {
    var table = dynamoTable('name', {key: ['id', 'name']})
    table.resolveKey([23, 'john']).should.eql({id: {N: '23'}, name: {S: 'john'}})
    table.resolveKey(23, 'john').should.eql({id: {N: '23'}, name: {S: 'john'}})
  })

  it('should resolve when object specified', function() {
    var table = dynamoTable('name')
    table.resolveKey({id: 23}).should.eql({id: {N: '23'}})
    table.resolveKey({id: 23, name: 'john'}).should.eql({id: {N: '23'}, name: {S: 'john'}})
  })

  it('should work with Buffer keys', function() {
    var table = dynamoTable('name')
    table.resolveKey(new Buffer([1, 2, 3, 4])).should.eql({id: {B: 'AQIDBA=='}})
  })
})

describe('get', function() {
  it('should call with default options', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('GetItem')
        options.TableName.should.equal('name')
        options.Key.should.eql({id: {N: '23'}})
        should.not.exist(options.AttributesToGet)
        should.not.exist(options.ConsistentRead)
        should.not.exist(options.ReturnConsumedCapacity)
        process.nextTick(function() {
          cb(null, {Item: {id: {N: '23'}, name: {S: 'john'}}})
        })
      }
    }
    table = dynamoTable('name', {client: client})
    table.get(23, function(err, jsObj) {
      if (err) return done(err)
      jsObj.should.eql({id: 23, name: 'john'})
      done()
    })
  })

  it('should use options if passed in', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('GetItem')
        options.TableName.should.equal('other')
        options.Key.should.eql({id: {N: '100'}})
        options.AttributesToGet.should.eql(['id'])
        should.not.exist(options.ConsistentRead)
        should.not.exist(options.ReturnConsumedCapacity)
        process.nextTick(function() {
          cb(null, {Item: {id: {N: '100'}}})
        })
      }
    }
    table = dynamoTable('name', {client: client})
    table.get(23, {
      TableName: 'other',
      Key: {id: {N: '100'}},
      AttributesToGet: ['id'],
    }, function(err, jsObj) {
      if (err) return done(err)
      jsObj.should.eql({id: 100})
      done()
    })
  })

  it('should callback with error if client error', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        process.nextTick(function() {
          cb(new Error('whoops'))
        })
      }
    }
    table = dynamoTable('name', {client: client})
    table.get(23, function(err) {
      err.should.be.an.instanceOf(Error)
      err.should.match(/whoops/)
      done()
    })
  })

  it('should accept different types of keys', function() {
    var table, client = {
      request: function(target, options) {
        options.Key.should.eql({id: {N: '23'}, name: {S: 'john'}})
      }
    }
    table = dynamoTable('name', {client: client, key: ['id', 'name']})
    table.get([23, 'john'])
    table.get({id: 23, name: 'john'})
  })

  it('should convert options to AttributesToGet if array', function() {
    var table, client = {
      request: function(target, options) {
        options.AttributesToGet.should.eql(['id', 'name'])
      }
    }
    table = dynamoTable('name', {client: client})
    table.get(23, ['id', 'name'])
  })

  it('should convert options to AttributesToGet if string', function() {
    var table, client = {
      request: function(target, options) {
        options.AttributesToGet.should.eql(['id'])
      }
    }
    table = dynamoTable('name', {client: client})
    table.get(23, 'id')
  })
})

describe('put', function() {
  it('should call with default options', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('PutItem')
        options.TableName.should.equal('name')
        options.Item.should.eql({id: {N: '23'}, name: {S: 'john'}})
        should.not.exist(options.Expected)
        should.not.exist(options.ReturnConsumedCapacity)
        should.not.exist(options.ReturnItemCollectionMetrics)
        should.not.exist(options.ReturnValues)
        process.nextTick(cb)
      }
    }
    table = dynamoTable('name', {client: client})
    table.put({id: 23, name: 'john'}, done)
  })
})

describe('delete', function() {
  it('should call with default options', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('DeleteItem')
        options.TableName.should.equal('name')
        options.Key.should.eql({id: {N: '23'}})
        should.not.exist(options.Expected)
        should.not.exist(options.ReturnConsumedCapacity)
        should.not.exist(options.ReturnItemCollectionMetrics)
        should.not.exist(options.ReturnValues)
        process.nextTick(cb)
      }
    }
    table = dynamoTable('name', {client: client})
    table.delete(23, done)
  })
})

describe('update', function() {
  it('should call with default options', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('UpdateItem')
        options.TableName.should.equal('name')
        options.Key.should.eql({id: {N: '23'}})
        options.AttributeUpdates.should.eql({
          name: {Value: {S: 'john'}},
          age: {Value: {N: '24'}},
          address: {Action: 'DELETE'},
        })
        should.not.exist(options.Expected)
        should.not.exist(options.ReturnConsumedCapacity)
        should.not.exist(options.ReturnItemCollectionMetrics)
        should.not.exist(options.ReturnValues)
        process.nextTick(cb)
      }
    }
    table = dynamoTable('name', {client: client})
    table.update({id: 23, name: 'john', age: 24, address: null}, done)
  })

  it('should throw if bad actions are used', function() {
    var table = dynamoTable('name')
    table.update.bind(table, 23, {delete: 'a', id: 34}).should.throw()
  })

  it('should assign mixed actions', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('UpdateItem')
        options.TableName.should.equal('name')
        options.Key.should.eql({id: {N: '23'}})
        options.AttributeUpdates.should.eql({
          name: {Value: {S: 'john'}},
          age: {Action: 'ADD', Value: {N: '24'}},
          address: {Action: 'DELETE'},
          parent: {Action: 'DELETE'},
        })
        should.not.exist(options.Expected)
        should.not.exist(options.ReturnConsumedCapacity)
        should.not.exist(options.ReturnItemCollectionMetrics)
        should.not.exist(options.ReturnValues)
        process.nextTick(cb)
      }
    }
    table = dynamoTable('name', {client: client})
    table.update(23, {put: {name: 'john', address: null}, add: {age: 24}, delete: 'parent'}, done)
  })

  it('should delete from sets', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('UpdateItem')
        options.TableName.should.equal('name')
        options.Key.should.eql({id: {N: '23'}})
        options.AttributeUpdates.should.eql({
          parent: {Action: 'DELETE'},
          clientIds: {Action: 'DELETE', Value: {NS: ['1', '2', '3']}},
        })
        should.not.exist(options.Expected)
        should.not.exist(options.ReturnConsumedCapacity)
        should.not.exist(options.ReturnItemCollectionMetrics)
        should.not.exist(options.ReturnValues)
        process.nextTick(cb)
      }
    }
    table = dynamoTable('name', {client: client})
    table.update(23, {delete: ['parent', {clientIds: [1, 2, 3]}]}, done)
  })
})

describe('query', function() {
  it('should call with default options', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('Query')
        options.TableName.should.equal('name')
        options.KeyConditions.should.eql({id: {ComparisonOperator: 'EQ', AttributeValueList: [{N: '23'}]}})
        should.not.exist(options.AttributesToGet)
        should.not.exist(options.ConsistentRead)
        should.not.exist(options.ReturnConsumedCapacity)
        should.not.exist(options.ExclusiveStartKey)
        should.not.exist(options.IndexName)
        should.not.exist(options.Limit)
        should.not.exist(options.ScanIndexForward)
        should.not.exist(options.Select)
        process.nextTick(function() {
          cb(null, {Items: [{id: {N: '23'}, name: {S: 'john'}}]})
        })
      }
    }
    table = dynamoTable('name', {client: client})
    table.query({id: 23}, function(err, items) {
      if (err) return done(err)
      items.should.eql([{id: 23, name: 'john'}])
      done()
    })
  })
})

describe('scan', function() {
  it('should call with default options', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('Scan')
        options.TableName.should.equal('name')
        should.not.exist(options.ScanFilter)
        should.not.exist(options.AttributesToGet)
        should.not.exist(options.ReturnConsumedCapacity)
        should.not.exist(options.ExclusiveStartKey)
        should.not.exist(options.Limit)
        should.not.exist(options.Select)
        process.nextTick(function() {
          cb(null, {Items: [{id: {N: '23'}, name: {S: 'john'}}, {id: {N: '24'}, name: {S: 'jane'}}]})
        })
      }
    }
    table = dynamoTable('name', {client: client})
    table.scan(function(err, items) {
      if (err) return done(err)
      items.should.eql([{id: 23, name: 'john'}, {id: 24, name: 'jane'}])
      done()
    })
  })
})

describe('describeTable', function() {
  it('should call with default options', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('DescribeTable')
        options.TableName.should.equal('name')
        process.nextTick(function() {
          cb(null, {Table: {AttributeDefinitions:[]}})
        })
      }
    }
    table = dynamoTable('name', {client: client})
    table.describeTable(function(err, table) {
      if (err) return done(err)
      table.should.eql({AttributeDefinitions:[]})
      done()
    })
  })
})

describe('updateTable', function() {
  it('should call with default options', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('UpdateTable')
        options.TableName.should.equal('name')
        options.ProvisionedThroughput.should.eql({ReadCapacityUnits: 10, WriteCapacityUnits: 20})
        process.nextTick(cb)
      }
    }
    table = dynamoTable('name', {client: client})
    table.updateTable(10, 20, done)
  })
})

describe('deleteTable', function() {
  it('should call with default options', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('DeleteTable')
        options.TableName.should.equal('name')
        process.nextTick(cb)
      }
    }
    table = dynamoTable('name', {client: client})
    table.deleteTable(done)
  })
})

describe('listTables', function() {
  it('should call with default options', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('ListTables')
        should.not.exist(options.TableName)
        should.not.exist(options.ExclusiveStartTableName)
        should.not.exist(options.Limit)
        process.nextTick(function() {
          cb(null, {TableNames: ['Orders', 'Items']})
        })
      }
    }
    table = dynamoTable('name', {client: client})
    table.listTables(function(err, tables) {
      if (err) return done(err)
      tables.should.eql(['Orders', 'Items'])
      done()
    })
  })
})

describe('increment', function() {
  it('should call with default options', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('UpdateItem')
        options.TableName.should.equal('name')
        options.Key.should.eql({id: {N: '23'}})
        options.AttributeUpdates.should.eql({count: {Action: 'ADD', Value: {N: '1'}}})
        options.ReturnValues.should.equal('UPDATED_NEW')
        should.not.exist(options.Expected)
        should.not.exist(options.ReturnConsumedCapacity)
        should.not.exist(options.ReturnItemCollectionMetrics)
        process.nextTick(function() {
          cb(null, {Attributes: {count: {N: '1'}}})
        })
      }
    }
    table = dynamoTable('name', {client: client})
    table.increment(23, 'count', function(err, newVal) {
      if (err) return done(err)
      newVal.should.equal(1)
      done()
    })
  })

  it('should allow specific amounts', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('UpdateItem')
        options.TableName.should.equal('name')
        options.Key.should.eql({id: {N: '23'}})
        options.AttributeUpdates.should.eql({count: {Action: 'ADD', Value: {N: '10'}}})
        options.ReturnValues.should.equal('UPDATED_NEW')
        should.not.exist(options.Expected)
        should.not.exist(options.ReturnConsumedCapacity)
        should.not.exist(options.ReturnItemCollectionMetrics)
        process.nextTick(function() {
          cb(null, {Attributes: {count: {N: '11'}}})
        })
      }
    }
    table = dynamoTable('name', {client: client})
    table.increment(23, 'count', 10, function(err, newVal) {
      if (err) return done(err)
      newVal.should.equal(11)
      done()
    })
  })
})

describe('nextId', function() {
  it('should call with default options', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('UpdateItem')
        options.TableName.should.equal('name')
        options.Key.should.eql({id: {S: '0'}})
        options.AttributeUpdates.should.eql({lastId: {Action: 'ADD', Value: {N: '1'}}})
        options.ReturnValues.should.equal('UPDATED_NEW')
        should.not.exist(options.Expected)
        should.not.exist(options.ReturnConsumedCapacity)
        should.not.exist(options.ReturnItemCollectionMetrics)
        process.nextTick(function() {
          cb(null, {Attributes: {lastId: {N: '1'}}})
        })
      }
    }
    table = dynamoTable('name', {client: client})
    table.nextId(function(err, newVal) {
      if (err) return done(err)
      newVal.should.equal(1)
      done()
    })
  })

  it('should use specified key types', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('UpdateItem')
        options.TableName.should.equal('name')
        options.Key.should.eql({id: {B: '0000'}, name: {S: '0'}})
        options.AttributeUpdates.should.eql({lastId: {Action: 'ADD', Value: {N: '1'}}})
        options.ReturnValues.should.equal('UPDATED_NEW')
        should.not.exist(options.Expected)
        should.not.exist(options.ReturnConsumedCapacity)
        should.not.exist(options.ReturnItemCollectionMetrics)
        process.nextTick(function() {
          cb(null, {Attributes: {lastId: {N: '1'}}})
        })
      }
    }
    table = dynamoTable('name', {client: client, key: ['id', 'name'], mappings: {id: 'B', name: 'S'}})
    table.nextId(function(err, newVal) {
      if (err) return done(err)
      newVal.should.equal(1)
      done()
    })
  })
})


describe('initId', function() {
  it('should call with default options', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('UpdateItem')
        options.TableName.should.equal('name')
        options.Key.should.eql({id: {S: '0'}})
        options.AttributeUpdates.should.eql({lastId: {Value: {N: '0'}}})
        should.not.exist(options.Expected)
        should.not.exist(options.ReturnConsumedCapacity)
        should.not.exist(options.ReturnItemCollectionMetrics)
        should.not.exist(options.ReturnValues)
        process.nextTick(cb)
      }
    }
    table = dynamoTable('name', {client: client})
    table.initId(done)
  })

  it('should use explicit value if passed in', function(done) {
    var table, client = {
      request: function(target, options, cb) {
        target.should.equal('UpdateItem')
        options.TableName.should.equal('name')
        options.Key.should.eql({id: {S: '0'}})
        options.AttributeUpdates.should.eql({lastId: {Value: {N: '100'}}})
        should.not.exist(options.Expected)
        should.not.exist(options.ReturnConsumedCapacity)
        should.not.exist(options.ReturnItemCollectionMetrics)
        should.not.exist(options.ReturnValues)
        process.nextTick(cb)
      }
    }
    table = dynamoTable('name', {client: client})
    table.initId(100, done)
  })
})
