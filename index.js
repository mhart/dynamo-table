var dynamo

try {
  dynamo = require('dynamo-client')
} catch (e) {
  // Assume consumer will pass in client
}

module.exports = function(name, options) {
  return new DynamoTable(name, options)
}
module.exports.DynamoTable = DynamoTable

function DynamoTable(name, options) {
  if (!name) throw new Error('Table must have a name')
  options = options || {}
  this.name = name
  this.client = options.client
  if (!this.client) {
    if (!dynamo) throw new Error('dynamo-client module is not installed')
    this.client = dynamo.createClient(options.region, options.credentials)
  }
  this.mappings = options.mappings || {}
  this.key = options.key || Object.keys(options.keyTypes || this.mappings).slice(0, 2)
  if (!Array.isArray(this.key)) this.key = [this.key]
  if (!this.key.length) this.key = ['id']
  this.keyTypes = options.keyTypes || {}
  this.readCapacity = options.readCapacity
  this.writeCapacity = options.writeCapacity
  this.indexes = options.indexes
  this.scanSegments = options.scanSegments
  this.preMapFromDb = options.preMapFromDb
  this.postMapFromDb = options.postMapFromDb
  this.preMapToDb = options.preMapToDb
  this.postMapToDb = options.postMapToDb
}

DynamoTable.prototype.mapAttrToDb = function(val, key, jsObj) {
  var mapping = this.mappings[key]
  if (mapping) {
    if (typeof mapping.to === 'function') return mapping.to(val, key, jsObj)
    if (typeof val === 'undefined') return
    if (mapping === 'json') return {S: JSON.stringify(val)}
    if (val == null || val === '') return
    switch (mapping) {
      case 'S': return {S: String(val)}
      case 'N': return {N: String(val)}
      case 'B': return {B: val.toString('base64')}
      case 'SS': return {SS: typeof val[0] === 'string' ? val : val.map(String)}
      case 'NS': return {NS: val.map(String)}
      case 'BS': return {BS: val.map(function(x) { return x.toString('base64') })}
      case 'bignum': return Array.isArray(val) ? {NS: val} : {N: val}
      case 'isodate': return {S: val.toISOString()}
      case 'timestamp': return {N: String(+val)}
      case 'mapS': return {SS: Object.keys(val)}
      case 'mapN': return {NS: Object.keys(val)}
      case 'mapB': return {BS: Object.keys(val)}
    }
  }
  if (val == null || val === '') return
  switch (typeof val) {
    case 'string': return {S: val}
    case 'boolean': return {S: String(val)}
    case 'number': return {N: String(val)}
  }
  if (Buffer.isBuffer(val)) {
    if (!val.length) return
    return {B: val.toString('base64')}
  }
  if (Array.isArray(val)) {
    if (!val.length) return
    if (typeof val[0] === 'string') return {SS: val}
    if (typeof val[0] === 'number') return {NS: val.map(String)}
    if (Buffer.isBuffer(val[0])) return {BS: val.map(function(x) { return x.toString('base64') })}
  }
  return {S: JSON.stringify(val)}
}

DynamoTable.prototype.mapAttrFromDb = function(val, key, dbItem) {
  var mapping = this.mappings[key]
  if (mapping) {
    if (typeof mapping.from === 'function') return mapping.from(val, key, dbItem)
    switch (mapping) {
      case 'S': return val.S
      case 'N': return +val.N
      case 'B': return new Buffer(val.B, 'base64')
      case 'SS': return val.SS
      case 'NS': return val.NS.map(Number)
      case 'BS': return val.BS.map(function(x) { return new Buffer(x, 'base64') })
      case 'json': return JSON.parse(val.S)
      case 'bignum': return val.N != null ? val.N : val.NS
      case 'isodate': return new Date(val.S)
      case 'timestamp': return new Date(val.N)
      case 'mapS':
      case 'mapN':
      case 'mapB':
        return (val.SS || val.NS || val.BS).reduce(function(mapObj, val) {
          mapObj[val] = 1
          return mapObj
        }, {})
    }
  }
  if (val.S != null) {
    if (val.S === 'true') return true
    if (val.S === 'false') return false
    if (val.S[0] === '{' || val.S[0] === '[')
      try { return JSON.parse(val.S) } catch (e) {}
    return val.S
  }
  if (val.N != null) return +val.N
  if (val.B != null) return new Buffer(val.B, 'base64')
  if (val.SS != null) return val.SS
  if (val.NS != null) return val.NS.map(Number)
  if (val.BS != null) return val.BS.map(function(x) { return new Buffer(x, 'base64') })
  throw new Error('Unknown DynamoDB type: ' + JSON.stringify(val))
}

DynamoTable.prototype.mapToDb = function(jsObj) {
  if (this.preMapToDb) jsObj = this.preMapToDb(jsObj)
  var self = this,
      dbItem = jsObj != null ? {} : null

  if (dbItem != null && jsObj != null) {
    Object.keys(jsObj).forEach(function(key) {
      var dbAttr = self.mapAttrToDb(jsObj[key], key, jsObj)
      if (!self._isEmpty(dbAttr))
        dbItem[key] = dbAttr
    })
  }
  if (this.postMapToDb) dbItem = this.postMapToDb(dbItem)
  return dbItem
}

DynamoTable.prototype.mapFromDb = function(dbItem) {
  if (this.preMapFromDb) dbItem = this.preMapFromDb(dbItem)
  var self = this,
      jsObj = dbItem != null ? {} : null

  if (dbItem != null && jsObj != null) {
    Object.keys(dbItem).forEach(function(key) {
      var jsAttr = self.mapAttrFromDb(dbItem[key], key, dbItem)
      if (typeof jsAttr !== 'undefined')
        jsObj[key] = jsAttr
    })
  }
  if (this.postMapFromDb) jsObj = this.postMapFromDb(jsObj)
  return jsObj
}

DynamoTable.prototype.resolveKey = function(key) {
  var self = this
  if (arguments.length > 1)
    key = [].slice.call(arguments)
  else if (typeof key !== 'object' || Buffer.isBuffer(key))
    key = [key]

  if (Array.isArray(key)) {
    return key.reduce(function(dbKey, val, ix) {
      dbKey[self.key[ix]] = self.mapAttrToDb(val, self.key[ix])
      return dbKey
    }, {})
  }
  return Object.keys(key).reduce(function(dbKey, attr) {
    dbKey[attr] = self.mapAttrToDb(key[attr], attr)
    return dbKey
  }, {})
}

DynamoTable.prototype._isEmpty = function(attr) {
  return attr == null || attr.S === '' || attr.N === '' || attr.B === '' ||
    attr.SS === '[]' || attr.NS === '[]' || attr.BS === '[]'
}

DynamoTable.prototype._getKeyType = function(attr) {
  var type = this.keyTypes[attr] || this.mappings[attr] || 'S'
  switch (type) {
    case 'N':
    case 'S':
    case 'B':
      return type
    case 'json':
    case 'isodate':
      return 'S'
    case 'bignum':
    case 'timestamp':
      return 'N'
  }
  throw new Error('Unsupported key type (' + type + ') for attr ' + attr)
}

DynamoTable.prototype.get = function(key, options, cb) {
  if (!cb) { cb = options; options = {} }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')
  options = this._getDefaultOptions(options)
  var self = this

  options.Key = options.Key || this.resolveKey(key)
  this.client.request('GetItem', options, function(err, data) {
    if (err) return cb(err)
    cb(null, self.mapFromDb(data.Item))
  })
}

DynamoTable.prototype.put = function(jsObj, options, cb) {
  if (!cb) { cb = options; options = {} }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')
  options.TableName = options.TableName || this.name

  options.Item = options.Item || this.mapToDb(jsObj)
  this.client.request('PutItem', options, cb)
}

DynamoTable.prototype.delete = function(key, options, cb) {
  if (!cb) { cb = options; options = {} }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')
  options.TableName = options.TableName || this.name

  options.Key = options.Key || this.resolveKey(key)
  this.client.request('DeleteItem', options, cb)
}

DynamoTable.prototype.update = function(key, actions, options, cb) {
  if (!cb) { cb = options; options = {} }
  if (!cb) { cb = actions; actions = key; key = null }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')
  options = this._getDefaultOptions(options)
  var self = this, pick, attrUpdates

  // If actions is a string or array, then it's a whitelist for attributes to update
  if (typeof actions === 'string') actions = [actions]
  if (Array.isArray(actions)) { pick = actions; actions = key; key = null }

  // If key is null, assume actions has a full object to put so clone it (without keys)
  if (key == null) {
    key = this.key.map(function(attr) { return actions[attr] })
    pick = pick || Object.keys(actions)
    actions = {put: pick.reduce(function(attrsObj, attr) {
      if (!~self.key.indexOf(attr)) attrsObj[attr] = actions[attr]
      return attrsObj
    }, {})}
  }

  // If we have some attributes that are not actions (put, add, delete), then throw
  if (Object.keys(actions).some(function(attr) { return !~['put', 'add', 'delete'].indexOf(attr) }))
    throw new Error('actions must only contain put/add/delete attributes')

  options.Key = options.Key || this.resolveKey(key)
  attrUpdates = options.AttributeUpdates = options.AttributeUpdates || {}

  if (actions.put != null) {
    Object.keys(actions.put).forEach(function(attr) {
      attrUpdates[attr] = attrUpdates[attr] || {Value: self.mapAttrToDb(actions.put[attr], attr)}
      if (self._isEmpty(attrUpdates[attr].Value)) {
        attrUpdates[attr].Action = 'DELETE' // "empty" attributes should actually be deleted
        delete attrUpdates[attr].Value
      }
    })
  }

  if (actions.add != null) {
    Object.keys(actions.add).forEach(function(attr) {
      attrUpdates[attr] = attrUpdates[attr] ||
        {Action: 'ADD', Value: self.mapAttrToDb(actions.add[attr], attr)}
    })
  }

  if (actions.delete != null) {
    if (!Array.isArray(actions.delete)) actions.delete = [actions.delete]
    actions.delete.forEach(function(attr) {
      if (typeof attr === 'string') {
        attrUpdates[attr] = attrUpdates[attr] || {Action: 'DELETE'}
      } else {
        Object.keys(attr).forEach(function(setKey) {
          attrUpdates[setKey] = attrUpdates[setKey] ||
            {Action: 'DELETE', Value: self.mapAttrToDb(attr[setKey], setKey)}
        })
      }
    })
  }

  if (!Object.keys(attrUpdates).length) return process.nextTick(cb)

  this.client.request('UpdateItem', options, cb)
}

DynamoTable.prototype.query = function(conditions, options, cb) {
  if (!cb) { cb = options; options = {} }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')
  options = this._getDefaultOptions(options)
  var self = this, nonKeys

  options.KeyConditions = options.KeyConditions || this.conditions(conditions)
  if (!options.IndexName) {
    nonKeys = Object.keys(options.KeyConditions).filter(function(attr) { return !~self.key.indexOf(attr) })
    if (nonKeys.length) {
      // we have a non-key attribute, must find an IndexName
      this.resolveIndexes(this.indexes).forEach(function(index) {
        if (index.key === nonKeys[0])
          options.IndexName = index.name
      })
      options.IndexName = options.IndexName || nonKeys[0]
    }
  }
  this._listRequest('Query', options, cb)
}

DynamoTable.prototype.scan = function(conditions, options, cb) {
  if (!cb) { cb = options; options = {} }
  if (!cb) { cb = conditions; conditions = null }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')
  options = this._getDefaultOptions(options)
  var totalSegments, segment, allItems

  if (conditions != null && !options.ScanFilter) options.ScanFilter = this.conditions(conditions)
  options.TotalSegments = options.TotalSegments || this.scanSegments

  if (options.Segment == null && options.TotalSegments) {
    totalSegments = options.TotalSegments
    allItems = new Array(totalSegments)
    for (segment = 0; segment < totalSegments; segment++)
      this.scan(null, cloneWithSegment(options, segment), checkDone(segment))
  } else {
    this._listRequest('Scan', options, cb)
  }
  function cloneWithSegment(options, segment) {
    return Object.keys(options).reduce(function(clone, key) {
      clone[key] = options[key]
      return clone
    }, {Segment: segment})
  }
  function checkDone(segment) {
    return function (err, items) {
      if (err) return cb(err)
      allItems[segment] = items
      if (!--totalSegments) {
        if (options.Select === 'COUNT')
          return cb(null, allItems.reduce(function(sum, count) { return sum + count }))
        cb(null, [].concat.apply([], allItems))
      }
    }
  }
}

// http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
DynamoTable.MAX_GET = 100
DynamoTable.prototype.batchGet = function(keys, options, tables, cb) {
  if (!cb) { cb = tables; tables = [] }
  if (!cb) { cb = options; options = {} }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')
  var self = this,
      onlyThis = false,
      tablesByName = {},
      allKeys, numRequests, allResults, i, j, key, requestItems, requestItem, opt

  if (keys && keys.length) {
    tables.unshift({table: this, keys: keys, options: options})
    onlyThis = tables.length === 1
  }
  allKeys = tables.map(function(tableObj) {
    var table = tableObj.table, keys = tableObj.keys, options = tableObj.options
    tablesByName[table.name] = table
    if (Array.isArray(options))
      options = {AttributesToGet: options}
    else if (typeof options === 'string')
      options = {AttributesToGet: [options]}
    return keys.map(function(key) {
      var dbKey = table.resolveKey(key)
      dbKey._table = table.name
      dbKey._options = options || {}
      return dbKey
    })
  })
  allKeys = [].concat.apply([], allKeys)
  numRequests = Math.ceil(allKeys.length / DynamoTable.MAX_GET)
  allResults = new Array(numRequests)

  for (i = 0; i < allKeys.length; i += DynamoTable.MAX_GET) {
    requestItems = {}
    for (j = i; j < i + DynamoTable.MAX_GET && j < allKeys.length; j++) {
      key = allKeys[j]
      requestItem = requestItems[key._table] = (requestItems[key._table] || {})
      for (opt in key._options)
        requestItem[opt] = key._options[opt]
      requestItem.Keys = requestItem.Keys || []
      requestItem.Keys.push(key)
      delete key._table
      delete key._options
    }
    batchRequest(requestItems, checkDone(i / DynamoTable.MAX_GET))
  }

  function batchRequest(requestItems, results, cb) {
    if (!cb) { cb = results; results = {} }
    self.client.request('BatchGetItem', {RequestItems: requestItems}, function(err, data) {
      if (err) return cb(err)
      for (var name in data.Responses) {
        results[name] = (results[name] || []).concat(
          data.Responses[name].map(tablesByName[name].mapFromDb.bind(tablesByName[name])))
      }
      if (Object.keys(data.UnprocessedKeys || {}).length)
        return batchRequest(data.UnprocessedKeys, results, cb)
      cb(null, results)
    })
  }

  function checkDone(ix) {
    return function (err, results) {
      if (err) return cb(err)
      allResults[ix] = results
      if (!--numRequests) {
        var merged = {}
        allResults.forEach(function(results) {
          for (var name in results)
            merged[name] = (merged[name] || []).concat(results[name])
        })
        cb(null, onlyThis ? merged[self.name] : merged)
      }
    }
  }
}

// http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
DynamoTable.MAX_WRITE = 25
DynamoTable.prototype.batchWrite = function(operations, tables, cb) {
  if (!cb) { cb = tables; tables = [] }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')
  var self = this,
      allOperations, numRequests, i, j, requestItems, operation

  if (operations && Object.keys(operations).length)
    tables.unshift({table: this, operations: operations})

  allOperations = tables.map(function(tableObj) {
    var table = tableObj.table, operations = tableObj.operations || [], ops
    if (Array.isArray(operations)) operations = {puts: operations, deletes: []}
    ops = (operations.puts || []).map(function(jsObj) {
      return {PutRequest: {Item: table.mapToDb(jsObj)}, _table: table.name}
    })
    return ops.concat((operations.deletes || []).map(function(key) {
      return {DeleteRequest: {Key: table.resolveKey(key)}, _table: table.name}
    }))
  })
  allOperations = [].concat.apply([], allOperations)
  numRequests = Math.ceil(allOperations.length / DynamoTable.MAX_WRITE)

  for (i = 0; i < allOperations.length; i += DynamoTable.MAX_WRITE) {
    requestItems = {}
    for (j = i; j < i + DynamoTable.MAX_WRITE && j < allOperations.length; j++) {
      operation = allOperations[j]
      requestItems[operation._table] = requestItems[operation._table] || []
      requestItems[operation._table].push(operation)
      delete operation._table
    }
    batchRequest(requestItems, checkDone)
  }

  function batchRequest(requestItems, cb) {
    self.client.request('BatchWriteItem', {RequestItems: requestItems}, function(err, data) {
      if (err) return cb(err)
      if (Object.keys(data.UnprocessedItems || {}).length)
        return batchRequest(data.UnprocessedItems, cb)
      cb()
    })
  }

  function checkDone(err) {
    if (err) return cb(err)
    if (!--numRequests)
      cb()
  }
}

DynamoTable.prototype.createTable = function(readCapacity, writeCapacity, indexes, options, cb) {
  if (!cb) { cb = options; options = {} }
  if (!cb) { cb = indexes; indexes = this.indexes }
  if (!cb) { cb = writeCapacity; writeCapacity = this.writeCapacity || 1 }
  if (!cb) { cb = readCapacity; readCapacity = this.readCapacity || 1 }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')
  options.TableName = options.TableName || this.name
  var self = this,
      attrMap = this.key.reduce(function(namesObj, attr) {
        namesObj[attr] = true
        return namesObj
      }, {})

  if (indexes && !options.LocalSecondaryIndexes) {
    options.LocalSecondaryIndexes = this.resolveIndexes(indexes).map(function(index) {
      var lsi = {
        IndexName: index.name,
        KeySchema: [self.key[0], index.key].map(function(attr, ix) {
          return { AttributeName: attr, KeyType: ix === 0 ? 'HASH' : 'RANGE' }
        }),
      }
      if (typeof index.projection === 'string')
        lsi.Projection = {ProjectionType: index.projection}
      else if (Array.isArray(index.projection))
        lsi.Projection = {ProjectionType: 'INCLUDE', NonKeyAttributes: index.projection}

      attrMap[index.key] = true

      return lsi
    })
  }
  options.KeySchema = options.KeySchema || this.key.map(function(attr, ix) {
    return {AttributeName: attr, KeyType: ix === 0 ? 'HASH' : 'RANGE'}
  })
  options.AttributeDefinitions = options.AttributeDefinitions || Object.keys(attrMap).map(function(attr) {
    return {AttributeName: attr, AttributeType: self._getKeyType(attr)}
  })
  options.ProvisionedThroughput = options.ProvisionedThroughput || {
    ReadCapacityUnits: readCapacity,
    WriteCapacityUnits: writeCapacity,
  }
  this.client.request('CreateTable', options, function(err, data) {
    if (err) return cb(err)
    cb(null, data.TableDescription)
  })
}

DynamoTable.prototype.describeTable = function(options, cb) {
  if (!cb) { cb = options; options = {} }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')
  options.TableName = options.TableName || this.name

  this.client.request('DescribeTable', options, function(err, data) {
    if (err) return cb(err)
    cb(null, data.Table)
  })
}

DynamoTable.prototype.updateTable = function(readCapacity, writeCapacity, options, cb) {
  if (!cb) { cb = options; options = {} }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')
  options.TableName = options.TableName || this.name

  options.ProvisionedThroughput = options.ProvisionedThroughput || {
    ReadCapacityUnits: readCapacity,
    WriteCapacityUnits: writeCapacity,
  }
  this.client.request('UpdateTable', options, function(err, data) {
    if (err) return cb(err)
    cb(null, data.TableDescription)
  })
}

DynamoTable.prototype.deleteTable = function(options, cb) {
  if (!cb) { cb = options; options = {} }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')
  options.TableName = options.TableName || this.name

  this.client.request('DeleteTable', options, function(err, data) {
    if (err) return cb(err)
    cb(null, data.TableDescription)
  })
}

// TODO: Support ExclusiveStartTableName/LastEvaluatedTableName
DynamoTable.prototype.listTables = function(options, cb) {
  if (!cb) { cb = options; options = {} }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')

  this.client.request('ListTables', options, function(err, data) {
    if (err) return cb(err)
    cb(null, data.TableNames)
  })
}

DynamoTable.prototype.deleteTableAndWait = function(options, cb) {
  if (!cb) { cb = options; options = {} }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')
  var self = this
  this.describeTable(function(err, data) {
    if (err && err.name === 'ResourceNotFoundException') return cb()
    if (err) return cb(err)

    if (data.TableStatus !== 'ACTIVE') return setTimeout(self.deleteTableAndWait.bind(self, options, cb), 1000)

    self.deleteTable(options, function(err) {
      if (err) return cb(err)
      self.deleteTableAndWait(options, cb)
    })
  })
}

DynamoTable.prototype.createTableAndWait = function(readCapacity, writeCapacity, indexes, options, cb) {
  if (!cb) { cb = options; options = {} }
  if (!cb) { cb = indexes; indexes = this.indexes }
  if (!cb) { cb = writeCapacity; writeCapacity = this.writeCapacity || 1 }
  if (!cb) { cb = readCapacity; readCapacity = this.readCapacity || 1 }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')
  var self = this
  this.describeTable(function(err, data) {
    if (err && err.name === 'ResourceNotFoundException') {
      return self.createTable(readCapacity, writeCapacity, indexes, options, function(err) {
        if (err) return cb(err)
        self.createTableAndWait(readCapacity, writeCapacity, indexes, options, cb)
      })
    }
    if (err) return cb(err)

    if (data.TableStatus === 'ACTIVE') return cb()
    if (data.TableStatus !== 'CREATING') return cb(new Error(data.TableStatus))

    setTimeout(self.createTableAndWait.bind(self, readCapacity, writeCapacity, indexes, options, cb), 1000)
  })
}

DynamoTable.prototype.increment = function(key, attr, incrAmt, options, cb) {
  var self = this, actions
  if (!cb) { cb = options; options = {} }
  if (!cb) { cb = incrAmt; incrAmt = 1 }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')

  options.ReturnValues = options.ReturnValues || 'UPDATED_NEW'
  actions = {add: {}}
  actions.add[attr] = incrAmt
  this.update(key, actions, options, function(err, data) {
    if (err) return cb(err)
    var newVal = (data.Attributes != null ? data.Attributes[attr] : null)
    if (newVal == null) return cb()
    cb(null, self.mapAttrFromDb(newVal, attr))
  })
}

DynamoTable.prototype._listRequest = function(operation, items, options, cb) {
  if (!cb) { cb = options; options = items; items = [] }
  if (typeof cb !== 'function') throw new Error('Last parameter must be a callback function')
  var self = this

  this.client.request(operation, options, function(err, data) {
    if (err) return cb(err)
    if (options.Select === 'COUNT') return cb(null, data.Count)

    items = items.concat(data.Items.map(function(item) { return self.mapFromDb(item) }))
    if (data.LastEvaluatedKey != null && (!options.Limit || options.Limit !== (data.ScannedCount || data.Count))) {
      options.ExclusiveStartKey = data.LastEvaluatedKey
      return self._listRequest(operation, items, options, cb)
    }
    cb(null, items)
  })
}

DynamoTable.prototype.conditions = function(conditionExprObj) {
  var self = this
  return Object.keys(conditionExprObj).reduce(function(condObj, attr) {
    condObj[attr] = self.condition(attr, conditionExprObj[attr])
    return condObj
  }, {})
}

DynamoTable.prototype.condition = function(key, conditionExpr) {
  var self = this,
      type = typeof conditionExpr,
      comparison, attrVals, cond

  if (conditionExpr === null) {
    comparison = 'NULL'
  } else if (conditionExpr === 'notNull' || conditionExpr === 'NOT_NULL') {
    comparison = 'NOT_NULL'
  } else if (type === 'string' || type === 'number' || Buffer.isBuffer(conditionExpr)) {
    comparison = 'EQ'
    attrVals = [conditionExpr]
  } else if (Array.isArray(conditionExpr)) {
    comparison = 'IN'
    attrVals = conditionExpr
  } else {
    comparison = Object.keys(conditionExpr)[0]
    attrVals = conditionExpr[comparison]
    if (!Array.isArray(attrVals)) attrVals = [attrVals]
    comparison = this.comparison(comparison)
  }
  cond = {ComparisonOperator: comparison}
  if (attrVals != null)
    cond.AttributeValueList = attrVals.map(function(val) { return self.mapAttrToDb(val, key) })
  return cond
}

DynamoTable.prototype.comparison = function(comparison) {
  switch (comparison) {
    case '=': return 'EQ'
    case '==': return 'EQ'
    case '!=': return 'NE'
    case '<=': return 'LE'
    case '<': return 'LT'
    case '>': return 'GT'
    case '>=': return 'GE'
    case '>=<=': return 'BETWEEN'
    case 'beginsWith':
    case 'startsWith':
      return 'BEGINS_WITH'
    case 'notContains':
    case 'doesNotContain':
      return 'NOT_CONTAINS'
  }
  return comparison.toUpperCase()
}

// indexes:
// [attr1/name1, attr2/name2]
// {name: attr1}
// {name: {key: attr1, projection: 'KEYS_ONLY'}}
// {name: {key: attr1, projection: [attr1, attr2]}}
// [{name: name1, key: attr1}, {name: name2, key: attr2}]
DynamoTable.prototype.resolveIndexes = function(indexes) {
  if (!indexes) return []
  if (!Array.isArray(indexes)) {
    indexes = Object.keys(indexes).map(function(name) {
      var index = indexes[name]
      if (typeof index === 'string') index = {key: index}
      return {name: name, key: index.key, projection: index.projection}
    })
  }
  return indexes.map(function(index) {
    if (typeof index === 'string') index = {name: index, key: index}
    index.projection = index.projection || 'ALL'
    return index
  })
}

DynamoTable.prototype._getDefaultOptions = function(options) {
  if (options == null)
    options = {}
  else if (Array.isArray(options))
    options = {AttributesToGet: options}
  else if (typeof options === 'string')
    options = {AttributesToGet: [options]}
  options.TableName = options.TableName || this.name
  return options
}

