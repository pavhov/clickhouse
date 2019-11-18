# @arm.tech/clckh
NodeJS client for [ClickHouse](https://clickhouse.yandex/).
Send query over HTTP interface.

Install:

```bash
npm i @arm.tech/clckh
```

Example:

```javascript
const ClickHouse = require('@arm.tech/clckh');

const clckh = new ClickHouse();
```
or with all options:

```javascript
const clckh = new ClickHouse({
	url: 'http://localhost',
	port: 8123,
	debug: false,
	basicAuth: null,
	isUseGzip: false,
        format: "json", // "json" || "csv" || "tsv"
	config: {
		session_timeout                         : 60,
		output_format_json_quote_64bit_integers : 0,
		enable_http_compression                 : 0
	},
	
	// This object merge with request params (see request lib docs)
	reqParams: {
		...
	}
});
```

or change 

	basicAuth: null
to

	basicAuth: {
	username: 'default',
	password: '',
	},


***
 
Exec query:
```javascript
const queries = [
	'DROP TABLE IF EXISTS session_temp',

	`CREATE TABLE session_temp (
		date Date,
		time DateTime,
		mark String,
		ips Array(UInt32),
		queries Nested (
			act String,
			id UInt32
		)
	)
	ENGINE=MergeTree(date, (mark, time), 8192)`,

	'OPTIMIZE TABLE ukit.loadstat PARTITION 201807 FINAL'
];

for(const query of queries) {
	const r = await clckh.query(query).toPromise();

	console.log(query, r);
}
````

***

Exec by callback way:
```javascript
clckh.query(query).exec(function (err, rows) {
	...
});
````

***

Stream:
```javascript
clckh.query(`SELECT number FROM system.numbers LIMIT 10`).stream()
	.on('data', function() {
		const stream = this;

		stream.pause();

		setTimeout(() => {
			stream.resume();
		}, 1000);
	})
	.on('error', err => {
		...
	})
	.on('end', () => {
		...
	});
```

or **async** stream:
```javascript
// async iteration
for await (const row of clckh.query(sql).stream()) {
	console.log(row);
}
```

***

As promise:
```javascript
const rows = await clckh.query(query).toPromise();

// use query with external data
const rows = await clckh.query('SELECT * AS count FROM temp_table', {
	external: [
		{
			name: 'temp_table',
			data: e._.range(0, rowCount).map(i => `str${i}`)
		},
	]
}).toPromise();
```

***

Set session:
```javascript
clckh.sessionId = '...';
const r = await clckh.query(
	`CREATE TEMPORARY TABLE test_table
	(_id String, str String)
	ENGINE=Memory`
).toPromise();
````


Insert stream:
```javascript
const ws = clckh.insert('INSERT INTO session_temp').stream();
for(let i = 0; i <= 1000; i++) {
	await ws.writeRow(
		[
			e._.range(0, 50).map(
				j => `${i}:${i * 2}:${j}`
			).join('-')
		]
	);
}

//wait stream finish
const result = await ws.exec();
```

***

Pipe readable stream to writable stream (across transform):
```javascript
const rs = clckh.query(query).stream();

const tf = new stream.Transform({
	objectMode : true,
	transform  : function (chunk, enc, cb) {
		cb(null, JSON.stringify(chunk) + '\n');
	}
});

clckh.sessionId = Date.now();
const ws = clckh.insert('INSERT INTO session_temp2').stream();

const result = await rs.pipe(tf).pipe(ws).exec();
```

***
