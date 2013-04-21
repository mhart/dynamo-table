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
