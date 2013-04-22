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
  this.name = name
  options = options || {}
  this.client = options.client
  if (this.client == null) {
    if (dynamo == null) throw new Error('dynamo-client module is not installed')
    this.client = dynamo.createClient(options.region)
  }
  this.mappings = options.mappings || {}
  this.key = options.key || Object.keys(this.mappings).slice(0, 2)
  if (!Array.isArray(this.key)) this.key = [this.key]
  if (!this.key.length) this.key = ['id']
  this.preFrom = options.preFrom || function(dbItem) { return dbItem != null ? {} : null }
  this.postFrom = options.postFrom || function(jsObj) { return jsObj }
  this.preTo = options.preTo || function(jsObj) { return jsObj != null ? {} : null }
  this.postTo = options.postTo || function(dbItem) { return dbItem }
  this.useNextId = options.useNextId
}

DynamoTable.prototype.mapAttrToDb = function(val, key, jsObj) {
  var mapping = this.mappings[key]
  if (mapping != null) {
    if (typeof mapping.to === 'function') return mapping.to(val, key, jsObj)
    if (mapping === 'json') return {S: JSON.stringify(val)}
    if (val == null || val === '') return
    switch (mapping) {
      case 'S': return {S: String(val)}
      case 'N': return {N: String(val)}
      case 'B': return {B: val.toString('base64')}
      case 'SS': return {SS: typeof val[0] === 'string' ? val : val.map(function(x) { return String(x) })}
      case 'NS': return {NS: val.map(function(x) { return String(x) })}
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
    if (typeof val[0] === 'number') return {NS: val.map(function(x) { return String(x) })}
    if (Buffer.isBuffer(val[0])) return {BS: val.map(function(x) { return x.toString('base64') })}
  }
  return {S: JSON.stringify(val)}
}

DynamoTable.prototype.mapAttrFromDb = function(val, key, dbItem) {
  var mapping = this.mappings[key]
  if (mapping != null) {
    if (typeof mapping.from === 'function') return mapping.from(val, key, dbItem)
    switch (mapping) {
      case 'S': return val.S
      case 'N': return +val.N
      case 'B': return new Buffer(val.B, 'base64')
      case 'SS': return val.SS
      case 'NS': return val.NS.map(function(x) { return +x })
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
    if (val.S[0] === '{' || val.S[0] === '[') try { return JSON.parse(val.S) } catch (e) {}
    return val.S
  }
  if (val.N != null) return +val.N
  if (val.B != null) return new Buffer(val.B, 'base64')
  if (val.SS != null) return val.SS
  if (val.NS != null) return val.NS.map(function(x) { return +x })
  if (val.BS != null) return val.BS.map(function(x) { return new Buffer(x, 'base64') })
  throw new Error('Unknown DynamoDB type: ' + JSON.stringify(val))
}

DynamoTable.prototype.mapToDb = function(jsObj) {
  var self = this, dbItem = this.preTo(jsObj)
  if (dbItem != null && jsObj != null) {
    Object.keys(jsObj).forEach(function(key) {
      var dbAttr = self.mapAttrToDb(jsObj[key], key, jsObj)
      if (!self._isEmpty(dbAttr))
        dbItem[key] = dbAttr
    })
  }
  return this.postTo(dbItem)
}

DynamoTable.prototype.mapFromDb = function(dbItem) {
  var self = this, jsObj = this.preFrom(dbItem)
  if (dbItem != null && jsObj != null) {
    Object.keys(dbItem).forEach(function(key) {
      var jsAttr = self.mapAttrFromDb(dbItem[key], key, dbItem)
      if (typeof jsAttr !== 'undefined')
        jsObj[key] = jsAttr
    })
  }
  return this.postFrom(jsObj)
}

DynamoTable.prototype.resolveKey = function(key) {
  var self = this
  if (arguments.length > 1) key = [].slice.call(arguments)
  if (typeof key !== 'object' || Buffer.isBuffer(key)) key = [key]
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

DynamoTable.prototype._defaultValue = function(attr) {
  var type = this._getKeyType(attr)
  switch (type) {
    case 'S': return '0'
    case 'N': return 0
    case 'B': return new Buffer('0000', 'base64')
  }
}

DynamoTable.prototype._getKeyType = function(attr) {
  var type = this.mappings[attr] || 'S'
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


// get(23, cb)
// get({id: 23}, ['id', 'name'], cb)
// get(23, ['id', 'name'], cb)
// get([23, 'john'], {AttributesToGet: ['id', 'name']}, cb)

DynamoTable.prototype.get = function(key, options, cb) {
  var self = this
  if (typeof options === 'function') { cb = options; options = {} }
  options = this._getDefaultOptions(options)
  options.Key = options.Key || this.resolveKey(key)
  this.client.request('GetItem', options, function(err, data) {
    if (err) return cb(err)
    cb(null, self.mapFromDb(data.Item))
  })
}

DynamoTable.prototype.put = function(jsObj, options, cb) {
  if (typeof options === 'function') { cb = options; options = {} }
  options.TableName = options.TableName || this.name
  options.Item = options.Item || this.mapToDb(jsObj)
  this.client.request('PutItem', options, cb)
}

DynamoTable.prototype.delete = function(key, options, cb) {
  if (typeof options === 'function') { cb = options; options = {} }
  options.TableName = options.TableName || this.name
  options.Key = options.Key || this.resolveKey(key)
  this.client.request('DeleteItem', options, cb)
}

DynamoTable.prototype.update = function(key, actions, options, cb) {
  var self = this, attrUpdates
  if (typeof options === 'function') { cb = options; options = {} }
  else if (typeof actions === 'function') { cb = actions; actions = key; key = null }
  options = this._getDefaultOptions(options)

  // If key is null, assume actions has a full object to put so clone it (without keys)
  if (key == null) {
    key = this.key.map(function(attr) { return actions[attr] })
    actions = {put: Object.keys(actions).reduce(function(attrsObj, attr) {
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
        // "empty" attributes should actually be deleted
        attrUpdates[attr].Action = 'DELETE'
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

  this.client.request('UpdateItem', options, cb)
}

DynamoTable.prototype.query = function(conditions, options, cb) {
  if (typeof options === 'function') { cb = options; options = {} }
  options = this._getDefaultOptions(options)
  options.KeyConditions = options.KeyConditions || this.conditions(conditions)
  this._listRequest('Query', options, cb)
}

DynamoTable.prototype.scan = function(conditions, options, cb) {
  var self = this
  if (typeof options === 'function') { cb = options; options = {} }
  else if (typeof conditions === 'function') { cb = conditions; conditions = null }
  options = this._getDefaultOptions(options)

  // filter out the default key
  if (this.useNextId) {
    if (conditions == null) conditions = {}
    this.key.forEach(function(attr) {
      if (typeof conditions[attr] === 'undefined')
        conditions[attr] = {'!=': self._defaultValue(attr)}
    })
  }
  if (conditions != null) options.ScanFilter = options.ScanFilter || this.conditions(conditions)
  this._listRequest('Scan', options, cb)
}

DynamoTable.prototype.describeTable = function(options, cb) {
  if (typeof options === 'function') { cb = options; options = {} }
  options.TableName = options.TableName || this.name
  this.client.request('DescribeTable', options, function(err, data) {
    if (err) return cb(err)
    cb(null, data.Table)
  })
}

DynamoTable.prototype.updateTable = function(readCapacity, writeCapacity, options, cb) {
  if (typeof options === 'function') { cb = options; options = {} }
  options.TableName = options.TableName || this.name
  options.ProvisionedThroughput = options.ProvisionedThroughput || {
    ReadCapacityUnits: readCapacity,
    WriteCapacityUnits: writeCapacity,
  }
  this.client.request('UpdateTable', options, cb)
}
 
DynamoTable.prototype.deleteTable = function(options, cb) {
  if (typeof options === 'function') { cb = options; options = {} }
  options.TableName = options.TableName || this.name
  this.client.request('DeleteTable', options, cb)
}

// TODO: Support ExclusiveStartTableName/LastEvaluatedTableName
DynamoTable.prototype.listTables = function(options, cb) {
  if (typeof options === 'function') { cb = options; options = {} }
  this.client.request('ListTables', options, function(err, data) {
    if (err) return cb(err)
    cb(null, data.TableNames)
  })
}

DynamoTable.prototype.increment = function(key, attr, incrAmt, options, cb) {
  var self = this, actions
  if (typeof options === 'function') { cb = options; options = {} }
  else if (typeof incrAmt === 'function') { cb = incrAmt; incrAmt = 1 }
  if (incrAmt == null) incrAmt = 1
  options = options || {}
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

DynamoTable.prototype.nextId = function(incrAmt, options, cb) {
  if (typeof options === 'function') { cb = options; options = {} }
  else if (typeof incrAmt === 'function') { cb = incrAmt; incrAmt = 1 }
  if (incrAmt == null) incrAmt = 1
  var key = this.key.map(this._defaultValue.bind(this))
  this.increment(key, 'lastId', incrAmt, options, cb)
}

DynamoTable.prototype.initId = function(val, options, cb) {
  if (typeof options === 'function') { cb = options; options = {} }
  else if (typeof val === 'function') { cb = val; val = 0 }
  if (val == null) val = 0
  var key = this.key.map(this._defaultValue.bind(this))
  this.update(key, {put: {lastId: val}}, options, cb)
}

DynamoTable.prototype._listRequest = function(operation, items, options, cb) {
  var self = this
  if (typeof options === 'function') { cb = options; options = items; items = [] }
  this.client.request(operation, options, function(err, data) {
    if (err) return cb(err)
    if (options.Count) return cb(null, data.Count)

    items = items.concat(data.Items.map(function(item) { return self.mapFromDb(item) }))
    if (data.LastEvaluatedKey != null && (!options.Limit || options.Limit !== data.Count)) {
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
  var self = this, type = typeof conditionExpr, comparison, attrVals
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
    [comparison] = Object.keys(conditionExpr)
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

