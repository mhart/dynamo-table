dynamo-table
------------

[![Build Status](https://secure.travis-ci.org/mhart/dynamo-table.png?branch=master)](http://travis-ci.org/mhart/dynamo-table)

A lightweight module to map JS objects and queries to
[DynamoDB](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/API.html)
tables (supports the latest API version, 2012-08-10).

This builds upon the [dynamo-client](https://github.com/jed/dynamo-client)
module, although any client that supports a simple `request` method will also
work.

Example
-------

```javascript
var dynamoTable = require('dynamo-table')

// Will use us-east-1 and credentials from process.env unless otherwise specified
var table = dynamoTable('Orders', {key: ['customerId', 'orderId']})

table.put({customerId: 23, orderId: 101, lineItemIds: [1, 2, 3]}, function(err) {
  if (err) throw err

  table.put({customerId: 23, orderId: 102, lineItemIds: [4, 5, 6]}, function(err) {
    if (err) throw err

    table.query({customerId: 23, orderId: {'>': 101}}, function(err, items) {
      if (err) throw err

      console.log(items)
      // [{customerId: 23, orderId: 102, lineItemIds: [4, 5, 6]}]
    })
  })
})
```

API
---

### dynamoTable(name, [options])
### new DynamoTable(name, [options])

Constructor of the table, including DynamoDB details, keys, mappings, etc

### get(key, [options], callback)

Corresponds to [GetItem](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html)

### put(jsObj, [options], callback)

Corresponds to [PutItem](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html)

### delete(key, [options], callback)

Corresponds to [DeleteItem](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html)

### update(key, actions, [options], callback)
### update(jsObj, [options], callback)

Corresponds to [UpdateItem](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html)

### query(conditions, [options], callback)

Corresponds to [Query](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html)

### scan([conditions], [options], callback)

Corresponds to [Scan](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html)

### batchGet(keys, [options], [tables], callback)

Corresponds to [BatchGetItem](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html)

### batchWrite([operations], [tables], callback)

Corresponds to [BatchWriteItem](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html)

### createTable(readCapacity, writeCapacity, [indexes], [options], callback)

Corresponds to [CreateTable](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html)

### updateTable(readCapacity, writeCapacity, [options], callback)

Corresponds to [UpdateTable](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTable.html)

### describeTable([options], callback)

Corresponds to [DescribeTable](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html)

### deleteTable([options], callback)

Corresponds to [DeleteTable](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html)

### listTables([options], callback)

Corresponds to [ListTables](http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html)

### increment(key, attr, [incrAmt], [options], callback)

Helper to increment an attribute by a certain amount

### mapToDb(jsObj)

Maps a JavaScript object to a DynamoDB-friendly object

### mapFromDb(dbItem)

Maps a DynamoDB object to a JavaScript object

### mapAttrToDb(val, [key], [jsObj])

Maps an individual attribute/value to a DynamoDB-friendly attribute

### mapAttrFromDb(val, [key], [dbItem])

Maps an individual DynamoDB attribute to a JavaScript value


Installation
------------

With [npm](http://npmjs.org/) do:

```
npm install dynamo-table
```

Thanks
------

Thanks to [@jed](https://github.com/jed) for his lightweight
[dynamo-client](https://github.com/jed/dynamo-client) lib!

