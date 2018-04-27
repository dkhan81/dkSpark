
val names = sc.textFile("/logs/zeppelin--697df1ac03e0.log")
names.take(10)
val lengths = names.map(str => str.length )
val totalLength = lengths.reduce( (acc, newValue) => acc + newValue )
val count = lengths.count()
val average = totalLength.toDouble / count
names.toDebugString
names.map( (lastName) => lastName.size ).toDebugString
names.filter( (name) => name.size > 3).toDebugString
names.groupBy( (name) => name.charAt(0) ).toDebugString
names.groupBy( (name) => name.charAt(0) ).mapValues( l => l.size).toDebugString
names.keyBy( (name) => name.charAt(0) ).mapValues( (name) => 1 ).reduceByKey( (x, y) => x + y).toDebugString

names.cache()
names.toDebugString
names.count()
names.toDebugString


