library dart_orm_adapter_mongodb;

import 'dart:async';

import 'package:dart_orm/dart_orm.dart';
import 'package:mongo_dart/mongo_dart.dart' as mongo_connector;

import 'package:logging/logging.dart';

class MongoDBAdapter extends DBAdapter {
  final Logger log = new Logger('DartORM.MongoDBAdapter');

  String _connectionString;
  mongo_connector.Db _connection;

  MongoDBAdapter(String connectionString) {
    _connectionString = connectionString;
  }

  Map<String, mongo_connector.DbCollection> _collectionsCache = {};

  Future connect() async {
    _connection = new mongo_connector.Db(_connectionString);
    await _connection.open();
  }

  get connection => _connection;

  /// Closes all connections to the database.
  void close() {
    _connection.close();
    log.finest('Connection closed.');
  }

  dynamic convertCondition(Table table, Condition cond) {
    var w = null;

    Field pKey = table.getPrimaryKeyField();
    if (pKey != null) {
      if (cond.firstVar == pKey.fieldName) {
        cond.firstVar = '_id';
      }
      if (cond.secondVar == pKey.fieldName) {
        cond.secondVar = '_id';
      }
    }

    // TODO: type checking here
    // problem: ORM.Find f = new ORM.Find(User)
    // ..where(new ORM.LowerThan('id', 4)) -- ok
    // ..where(new ORM.LowerThan('id', '4')) -- silently returns empty list

    switch (cond.condition) {
      case '=':
        w = mongo_connector.where.eq(cond.firstVar, cond.secondVar);
        break;
      case '>':
        w = mongo_connector.where.gt(cond.firstVar, cond.secondVar);
        break;
      case '<':
        w = mongo_connector.where.lt(cond.firstVar, cond.secondVar);
        break;
    }

    if (cond.conditionQueue.length > 0) {
      for (Condition innerCond in cond.conditionQueue) {
        var innerWhere = convertCondition(table, innerCond);
        if (innerCond.logic == 'AND') {
          w.and(innerWhere);
        }
        if (innerCond.logic == 'OR') {
          w.or(innerWhere);
        }
      }
    }

    return w;
  }

  Future<List<Map<dynamic, dynamic>>> select(Select select) async {
    Completer completer = new Completer();

    log.finest('Select:' + select.toString());

    List found = new List();
    var mongoSelector = null;

    mongo_connector.DbCollection collection =
        _collectionsCache[select.table.tableName];

    if (collection == null) {
      List dbCollections = await _connection.listCollections();

      if (dbCollections.contains(select.table.tableName)) {
        collection = _connection.collection(select.table.tableName);
      }
    }

    if (collection == null) {
      throw new TableNotExistException();
    }

    if (select.condition != null) {
      mongoSelector = convertCondition(select.table, select.condition);
    }

    if (mongoSelector == null) {
      mongoSelector = mongo_connector.where.ne('_id', null);
    }

    if (select.sorts.length > 0) {
      for (String fieldName in select.sorts.keys) {
        Field pKey = select.table.getPrimaryKeyField();
        if (pKey != null) {
          if (fieldName == pKey.fieldName) {
            fieldName = '_id';
          }
        }

        if (select.sorts[fieldName] == 'ASC') {
          mongoSelector = mongoSelector.sortBy(fieldName, descending: false);
        } else {
          mongoSelector = mongoSelector.sortBy(fieldName, descending: true);
        }
      }
    }

    if (select.limit != null) {
      mongoSelector.limit(select.limit);
    }

    log.finest('Mongo selector:' + mongoSelector.toString());

    try {
      var findResult = await collection.find(mongoSelector).forEach((value) {
        // for each found value, if select.table contains primary key
        // we need to change '_id' to that primary key name
        Field f = select.table.getPrimaryKeyField();
        if (f != null) {
          value[f.fieldName] = value['_id'];
        }
        found.add(value);
      });
    } catch (e) {
      log.shout('Select failed for $mongoSelector', e);
    }

    log.finest('Results for $mongoSelector:' + found.toString());

    return found;
  }

  Future createTable(Table table) async {
    Field pKey = table.getPrimaryKeyField();

    log.finest('Create table:' + table.toString());

    if (pKey != null) {
      await createSequence(table, pKey);
    }

    mongo_connector.DbCollection createdCollection =
        await _connection.collection(table.tableName);

    _collectionsCache[table.tableName] = createdCollection;

    log.finest('Create table result:' + createdCollection.toString());
    return true;
  }

  Future<int> insert(Insert insert) async {
    log.finest('Insert:' + insert.toString());

    var collection = await _connection.collection(insert.table.tableName);

    Field pKey = insert.table.getPrimaryKeyField();
    var primaryKeyValue = 0;
    if (pKey != null) {
      primaryKeyValue = await getNextSequence(insert.table, pKey);
      insert.fieldsToInsert['_id'] = primaryKeyValue;
    }

    var insertResult = await collection.insert(insert.fieldsToInsert);

    log.finest('Insert result:', insertResult);
    return primaryKeyValue;
  }

  Future<int> update(Update update) async {
    log.finest('Update:' + update.toString());

    var collection = await _connection.collection(update.table.tableName);

    Field pKey = update.table.getPrimaryKeyField();
    if (pKey == null) {
      throw new Exception('Could not update a table row without primary key.');
    }

    var selector = convertCondition(update.table, update.condition);
    var modifiers = mongo_connector.modify;
    for (String fieldName in update.fieldsToUpdate.keys) {
      modifiers.set(fieldName, update.fieldsToUpdate[fieldName]);
    }

    var updateResult = await collection.update(selector, modifiers);

    log.finest('Update result:', updateResult);

    return updateResult.length;
  }

  Future<int> delete(Delete delete) async {
    log.finest('Delete: ' + delete.toString());

    var collection = await _connection.collection(delete.table.tableName);
    Field pKey = delete.table.getPrimaryKeyField();

    await collection.remove(convertCondition(delete.table, delete.condition));
    return null;
  }

  Future createSequence(Table table, Field field) async {
    log.finest('Create sequence:' + table.toString() + ' ' + field.toString());
    var countersCollection = await _connection.collection('counters');

    var existingCounter = await countersCollection.findOne(mongo_connector.where
        .eq('_id', "${table.tableName}_${field.fieldName}_seq"));
    if (existingCounter == null) {
      var insertResult = await countersCollection.insert(
          {'_id': "${table.tableName}_${field.fieldName}_seq", 'seq': 0});

      log.finest('Create sequence insert result:', insertResult);
    }
  }

  Future<int> getNextSequence(Table table, Field field) {
    Completer completer = new Completer();

    log.finest(
        'Get next sequence:' + table.toString() + ' ' + field.toString());

    String seqName = "${table.tableName}_${field.fieldName}_seq";

    Map command = {
      'findAndModify': 'counters',
      'query': {'_id': seqName},
      'update': {
        r'$inc': {'seq': 1}
      },
      'new': true
    };

    _connection
        .executeDbCommand(mongo_connector.DbCommand
            .createQueryDbCommand(_connection, command))
        .then((result) {
      log.finest('Get next sequence result:');
      log.finest(result);

      if (result['value'] == null) {
        completer.complete(null);
      } else {
        var value = result['value']['seq'];
        completer.complete(value);
      }
    }).catchError((e) {
      log.shout('Get next sequence error:', e);
      completer.completeError(e);
    });

    return completer.future;
  }
}
