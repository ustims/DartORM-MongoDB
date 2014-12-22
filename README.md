MongoDB adapter for DartORM.
============================

https://github.com/ustims/DartORM

Usage example
-------------

```dart
import 'package:dart_orm/dart_orm.dart';
import 'package:dart_orm_adapter_mongodb/dart_orm_adapter_mongodb.dart';

...

String mongoUser = 'dart_orm_test_user';
String mongoPass = 'dart_orm_test_user';
String mongoDBName = 'dart_orm_test';

MongoDBAdapter mongoAdapter = new MongoDBAdapter(
  'mongodb://$mongoUser:$mongoPass@127.0.0.1/$mongoDBName');
await mongoAdapter.connect();

ORM.Model.ormAdapter = mongoAdapter;
```