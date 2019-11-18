const
	stream         = require('stream'),
	expect         = require('expect.js'),
	_              = require('lodash'),
	ClickHouse = require('../out/.').default;

const database = 'test_' + _.random(1000, 100000);

const
	clickhouse = new ClickHouse({
		database : database,
		debug    : false
	}),
	minRnd     = 50 * 1024,
	rowCount   = _.random(minRnd, 128 * 1024),
	sql        = `SELECT
				number,
				toString(number * 2) AS str,
				toDate(number + 1) AS date
			FROM system.numbers
			LIMIT ${rowCount}`;

before(async () => {
	const temp = new ClickHouse();
	
	await temp.query(`DROP DATABASE IF EXISTS ${database}`).toPromise();
	await temp.query(`CREATE DATABASE ${database}`).toPromise();
});

describe('Exec', () => {
	it('should return not null object', async () => {
		const sqlList = [
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
			
			'OPTIMIZE TABLE session_temp PARTITION 201807 FINAL'
		];
		
		for(const query of sqlList) {
			const r = await clickhouse.query(query).toPromise();
			
			expect(r).to.be.ok();
		}
	});
});

describe('Select', () => {
	it('use callback', callback => {
		clickhouse.query(sql).exec((err, rows) => {
			expect(err).to.not.be.ok();
			
			expect(rows).to.have.length(rowCount);
			expect(rows[0]).to.eql({ number: 0, str: '0', date: '1970-01-02' });
			
			callback();
		});
	});
	
	
	it('use callback #2', callback => {
		clickhouse.query(sql, (err, rows) => {
			expect(err).to.not.be.ok();
			
			expect(rows).to.have.length(rowCount);
			expect(rows[0]).to.eql({ number: 0, str: '0', date: '1970-01-02' });
			
			callback();
		});
	});

	it('use callback #3 with csv format', callback => {
		clickhouse.query(`${sql} format CSVWithNames`).exec((err, rows) => {
			expect(err).to.not.be.ok();

			expect(rows).to.have.length(rowCount);
			expect(rows[0]).to.eql({ number: 0, str: 0, date: '1970-01-02' });

			callback();
		});
	});


	it('use callback #4 with csv format', callback => {
		clickhouse.query(`${sql} format CSVWithNames`, (err, rows) => {
			expect(err).to.not.be.ok();

			expect(rows).to.have.length(rowCount);
			expect(rows[0]).to.eql({ number: 0, str: 0, date: '1970-01-02' });

			callback();
		});
	});

	it('use callback #5 with tsv format', callback => {
		clickhouse.query(`${sql} format TabSeparatedWithNames`).exec((err, rows) => {
			expect(err).to.not.be.ok();

			expect(rows).to.have.length(rowCount);
			expect(rows[0]).to.eql({ number: 0, str: '0', date: '1970-01-02' });

			callback();
		});
	});


	it('use callback #6 with tsv format', callback => {
		clickhouse.query(`${sql} format TabSeparatedWithNames`, (err, rows) => {
			expect(err).to.not.be.ok();

			expect(rows).to.have.length(rowCount);
			expect(rows[0]).to.eql({ number: 0, str: '0', date: '1970-01-02' });

			callback();
		});
	});


	it('use stream', function(callback) {
		this.timeout(10000);
		
		let i = 0;
		let error = null;
		
		clickhouse.query(sql).stream()
			.on('data', () => {
				++i
			})
			// TODO: on this case you should catch error
			.on('error', err => {
				callback(err)
			})
			.on('end', () => {
				expect(error).to.not.be.ok();
				expect(i).to.be(rowCount);
				
				callback();
			});
	});
	
	it('use stream with csv format', function(callback) {
		// this.timeout(10000);
		
		let i = 0;
		let error = null;
		
		clickhouse.query(`${sql} format CSVWithNames`).stream()
			.on('data', () => {
				++i
			})
			// TODO: on this case you should catch error
			.on('error', err => callback(err))
			.on('end', () => {
				expect(error).to.not.be.ok();
				expect(i).to.be(rowCount);
				
				callback();
			});
	});
	
	it('use stream with tsv format', function(callback) {
		// this.timeout(10000);
		
		let i = 0;
		let error = null;
		
		clickhouse.query(`${sql} format TabSeparatedWithNames`).stream()
			.on('data', () => {
				++i
			})
			// TODO: on this case you should catch error
			.on('error', err => error = err)
			.on('end', () => {
				expect(error).to.not.be.ok();
				expect(i).to.be(rowCount);
				
				callback();
			});
	});
	
	
	it('use stream with pause/resume', function(callback) {
		const
			count = 10,
			pause = 1000,
			ts    = Date.now();
		
		this.timeout(count * pause * 2);
		
		let i     = 0,
			error = null;
		
		const stream = clickhouse.query(`SELECT number FROM system.numbers LIMIT ${count}`).stream();
		
		stream
			.on('data', () => {
				++i;
				
				stream.pause();
				
				setTimeout(() => stream.resume(), pause);
			})
			.on('error', err => error = err)
			.on('end', () => {
				expect(error).to.not.be.ok();
				expect(i).to.be(count);
				expect(Date.now() - ts).to.be.greaterThan(count * pause);
				
				callback();
			})
	});
	
	
	const nodeVersion = process.version.split('.')[0].substr(1);
	if (parseInt(nodeVersion, 10) >= 10) {
		it('use async for', async function() {
			let i = 0;
			
			for await (const row of clickhouse.query(sql).stream()) {
				++i;
				expect(row).to.have.key('number');
				expect(row).to.have.key('str');
				expect(row).to.have.key('date');
			}
			
			expect(i).to.be(rowCount);
		});
		
		it('use async for with csv format', async function() {
			let i = 0;
			
			for await (const row of clickhouse.query(`${sql} format CSVWithNames`).stream()) {
				++i;
				expect(row).to.have.key('number');
				expect(row).to.have.key('str');
				expect(row).to.have.key('date');
			}
			
			expect(i).to.be(rowCount);
		});
		
		it('use async for with tsv format', async function() {
			let i = 0;
			
			for await (const row of clickhouse.query(`${sql} format TabSeparatedWithNames`).stream()) {
				++i;
				expect(row).to.have.key('number');
				expect(row).to.have.key('str');
				expect(row).to.have.key('date');
			}
			
			expect(i).to.be(rowCount);
		});
	}
	
	
	it('use promise and await/async', async () => {
		let rows = await clickhouse.query(sql).toPromise();
		
		expect(rows).to.have.length(rowCount);
		expect(rows[0]).to.eql({ number: 0, str: '0', date: '1970-01-02' });
	});
	
	
	it('use select with external', async () => {
		const result = await clickhouse.query('SELECT count(*) AS count FROM temp_table', {
			external: [
				{
					name: 'temp_table',
					data: _.range(0, rowCount).map(i => `str${i}`)
				},
			]
		}).toPromise();
		
		expect(result).to.be.ok();
		expect(result).to.have.length(1);
		expect(result[0]).to.have.key('count');
		expect(result[0].count).to.be(rowCount);
	});
});


describe('session', () => {
	it('use session', async () => {
		const sessionId = clickhouse.sessionId;
		clickhouse.sessionId = Date.now();
		
		const result = await clickhouse.query(
			`CREATE TEMPORARY TABLE test_table
			(_id String, str String)
			ENGINE=Memory`
		).toPromise();
		expect(result).to.be.ok();
		
		clickhouse.sessionId = Date.now();
		const result2 = await clickhouse.query(
			`CREATE TEMPORARY TABLE test_table
			(_id String, str String)
			ENGINE=Memory`
		).toPromise();
		expect(result2).to.be.ok();
		
		clickhouse.sessionId = sessionId;
	});
});


// You can use all settings from request library (https://github.com/request/request#tlsssl-protocol)
describe('TLS/SSL Protocol', () => {
	it('use TLS/SSL Protocol', async () => {
		const
			https = require('https'),
			fs    = require('fs');
		
		let server = null;
		
		try {
			server = https.createServer(
				{
					key  : fs.readFileSync('test/cert/server.key'),
					cert : fs.readFileSync('test/cert/server.crt')
				},
				(req, res) => {
					res.writeHead(200);
					res.end('{\n\t"meta":\n\t[\n\t\t{\n\t\t\t"name": "plus(1, 1)",\n\t\t\t"type": "UInt16"\n\t\t}\n\t],\n\n\t"data":\n\t[\n\t\t{\n\t\t\t"plus(1, 1)": 2\n\t\t}\n\t],\n\n\t"rows": 1,\n\n\t"statistics":\n\t{\n\t\t"elapsed": 0.000037755,\n\t\t"rows_read": 1,\n\t\t"bytes_read": 1\n\t}\n}\n');
				})
				.listen(8002);
			
			const temp = new ClickHouse({
				url       : 'https://localhost',
				port      : 8002,
				reqParams : {
					agentOptions: {
						ca: fs.readFileSync('test/cert/server.crt'),
						cert: fs.readFileSync('test/cert/server.crt'),
						key: fs.readFileSync('test/cert/server.key'),
					}
				}
			});
			
			
			const r = await temp.query('SELECT 1 + 1').toPromise();
			
			expect(r).to.be.ok();
			expect(r[0]).to.have.key('plus(1, 1)');
			expect(r[0]['plus(1, 1)']).to.be(2);
			
			if (server) {
				server.close();
			}
		} catch(err) {
			if (server) {
				server.close();
			}
			
			throw err;
		}
	});
});


describe('queries', () => {
	it('insert field as array', async () => {
		clickhouse.sessionId = Date.now();
		
		const r = await clickhouse.query(`
			CREATE TABLE IF NOT EXISTS test_array (
				date Date,
				str String,
				arr Array(String),
				arr2 Array(Date),
				arr3 Array(UInt8)
			) ENGINE=MergeTree(date, date, 8192)
		`).toPromise();
		expect(r).to.be.ok();
		
		const rows = [
			{
				date: '2018-02-01',
				str: 'имеющим ванную и теплый клозет!',
				arr: ['5670000000', 'asdas dasf'],
				arr2: ['1915-01-01'],
				arr3: [1, 2, 3, 4]
			},
			{
				date: '2018-02-01',
				str: 'имеющим ванную и теплый клозет!',
				arr: ['5670000000', 'asdas dasf'],
				arr2: ['1915-02-02'],
				arr3: [1, 2, 3, 4]
			}
		];
		
		
		const r2 = await clickhouse.insert('INSERT INTO test_array (date, str, arr, arr2, arr3)', rows).toPromise();
		expect(r2).to.be.ok();
		
		
		clickhouse.sessionId = null;
	});
	
	
	it('queries', async () => {
		const result = await clickhouse.query('DROP TABLE IF EXISTS session_temp').toPromise();
		expect(result).to.be.ok();
		
		const result2 = await clickhouse.query('DROP TABLE IF EXISTS session_temp2').toPromise();
		expect(result2).to.be.ok();
		
		const result3 = await clickhouse.query('CREATE TABLE session_temp (str String) ENGINE=MergeTree PARTITION BY tuple() ORDER BY tuple()').toPromise();
		expect(result3).to.be.ok();
		
		const result4 = await clickhouse.query('CREATE TABLE session_temp2 (str String) ENGINE=MergeTree PARTITION BY tuple() ORDER BY tuple()').toPromise();
		expect(result4).to.be.ok();
		
		const data = _.range(0, rowCount).map(r => [r]);
		const result5 = await clickhouse.insert(
			'INSERT INTO session_temp', data
		).toPromise();
		expect(result5).to.be.ok();
		
		const rows = await clickhouse.query('SELECT COUNT(*) AS count FROM session_temp').toPromise();
		expect(rows).to.be.ok();
		expect(rows).to.have.length(1);
		expect(rows[0]).to.have.key('count');
		expect(rows[0].count).to.be(data.length);
		expect(rows[0].count).to.be(rowCount);
		
		const result6 = await clickhouse.query('TRUNCATE TABLE session_temp').toPromise();
		expect(result6).to.be.ok();
		
		const ws = clickhouse.insert('INSERT INTO session_temp').stream();
		let count = 0;
		
		for(let i = 1; i <= rowCount; i++) {
			await ws.writeRow(
				[
					_.range(0, 50).map(
						j => `${i}:${i * 2}:${j}`
					).join('-')
				]
			);
			++count;
		}
		const result7 = await ws.exec();
		expect(result7).to.be.ok();
		expect(count).to.be(rowCount);
		
		clickhouse.isUseGzip = true;
		const rs = clickhouse.query(sql).stream();
		
		const tf = new stream.Transform({
			objectMode : true,
			transform  : function (chunk, enc, cb) {
				cb(null, JSON.stringify(chunk) + '\n');
			}
		});
		
		clickhouse.sessionId = Date.now();
		const ws2 = clickhouse.insert('INSERT INTO session_temp2').stream();
		
		const result8 = await rs.pipe(tf).pipe(ws2).exec();
		expect(result8).to.be.ok();
		clickhouse.isUseGzip = false;
		
		const result9 = await clickhouse.query('SELECT count(*) AS count FROM session_temp').toPromise();
		const result10 = await clickhouse.query('SELECT count(*) AS count FROM session_temp2').toPromise();
		expect(result9).to.eql(result10);
		
		const result11 = await clickhouse.query('SELECT date FROM test_array GROUP BY date WITH TOTALS').withTotals().toPromise();
		expect(result11).to.have.key('meta');
		expect(result11).to.have.key('data');
		expect(result11).to.have.key('totals');
		expect(result11).to.have.key('rows');
		expect(result11).to.have.key('statistics');
		
		const result111 = await clickhouse.query('DROP TABLE IF EXISTS test_int_temp').toPromise();
		expect(result111).to.be.ok();
		
		const result12 = await clickhouse.query('CREATE TABLE test_int_temp (int_value Int8 ) ENGINE=Memory').toPromise();
		expect(result12).to.be.ok();
		
		const int_value_data = [{int_value: 0}];
		const result13 = await clickhouse.insert('INSERT INTO test_int_temp (int_value)', int_value_data).toPromise();
		expect(result13).to.be.ok();
		
		const result14 = await clickhouse.query('SELECT int_value FROM test_int_temp').toPromise();
		expect(result14).to.eql(int_value_data);
	});
});


describe('response codes', () => {
	it('table is not exists', async () => {
		try {
			const result = await clickhouse.query('DROP TABLE session_temp').toPromise();
			expect(result).to.be.ok();
			
			await clickhouse.query('SELECT COUNT(*) AS count FROM session_temp').toPromise();
			expect().fail('You should not be here');
		} catch (err) {
			expect(err).to.be.ok();
			expect(err).to.have.key('code');
			expect(err.code).to.be(60);
		}
		
		try {
			let result = await clickhouse.query('DROP TABLE session_temp2').toPromise();
			expect(result).to.be.ok();
		
			await clickhouse.query('SELECT COUNT(*) AS count FROM session_temp2').toPromise();
			expect().fail('You should not be here2');
		} catch (err) {
			expect(err).to.be.ok();
			expect(err).to.have.key('code');
			expect(err.code).to.be(60);
		}
	});
});



describe('set database', () => {
	it('create instance with non-default database', async () => {
		const noDefaultDb = 'default_' + _.random(1000, 10000);
		const r = await clickhouse.query(`CREATE DATABASE ${noDefaultDb}`).toPromise();
		expect(r).to.be.ok();
		
		const temp = new ClickHouse({
			database: noDefaultDb
		});
		
		
		const result3 = await temp.query('CREATE TABLE session_temp (str String) ENGINE=MergeTree PARTITION BY tuple() ORDER BY tuple()').toPromise();
		expect(result3).to.be.ok();
		
		const r2 = await temp.query(`DROP DATABASE ${noDefaultDb}`).toPromise();
		expect(r2).to.be.ok();
	});
});


describe('compatibility with Sequelize ORM', () => {
	it('select with ;', async () => {
		const sqls = [
			'SELECT 1 + 1',
			'SELECT 1 + 1;',
			'SELECT 1 + 1 ;',
			'SELECT 1 + 1 ; '
		];

		for(const sql of sqls) {
			const r = await clickhouse.query(sql).toPromise();
			expect(r).to.be.ok();
		}
	});
});



describe('Constructor options', () => {
	it('url and host', async () => {
		const clickhouses = [
			new ClickHouse({
				url: 'localhost'
			}),
			
			new ClickHouse({
				url: 'http://localhost'
			}),
			
			new ClickHouse({
				url: 'http://localhost:8123',
				port: 8123
			}),
			
			new ClickHouse({
				host: 'localhost'
			}),
			
			new ClickHouse({
				host: 'http://localhost'
			}),
			
			new ClickHouse({
				host: 'http://localhost:8124',
				port: 8123
			}),

			new ClickHouse({
				host: 'http://localhost:8124',
				port: 8123,
				format: "json"
			}),

			new ClickHouse({
				host: 'http://localhost:8124',
				port: 8123,
				format: "tsv"
			}),

			new ClickHouse({
				host: 'http://localhost:8124',
				port: 8123,
				format: "csv"
			})
		];
		
		for(const clickhouse of clickhouses) {
			const r = await clickhouse.query('SELECT 1 + 1').toPromise();
			expect(r).to.be.ok();
		}
	});
	
	
	it('user && password ok', async () => {
		const clickhouses = [
			new ClickHouse({
				user: 'default',
				password: ''
			}),
			
			new ClickHouse({
				username: 'default',
				password: ''
			}),

			new ClickHouse({
				basicAuth: {
					username: 'default',
					password: ''
				}
			}),
		];
		
		for(const clickhouse of clickhouses) {
			const r = await clickhouse.query('SELECT 1 + 1').toPromise();
			expect(r).to.be.ok();
		}
	});
	
	
	it('user && password fail', async () => {
		const clickhouses = [
			new ClickHouse({
				user: 'default1',
				password: ''
			}),
			
			new ClickHouse({
				username: 'default1',
				password: ''
			}),
		];
		
		for(const clickhouse of clickhouses) {
			try {
				await clickhouse.query('SELECT 1 + 1').toPromise();
			} catch (err) {
				expect(err).to.be.ok();
			}
		}
	});
});


describe('Exec system queries', () => {
	it('select with ;', async () => {
		const sqls = [
			'EXISTS test_db.myTable'
		];

		for(const sql of sqls) {
			const [ row ] = await clickhouse.query(sql).toPromise();
			expect(row).to.be.ok();
			expect(row).to.have.key('result');
			expect(row.result).to.be(0);
		}
	});
});


describe('Abort query', () => {
	it('exec & abort', cb => {
		const $q = clickhouse.query(`SELECT number FROM system.numbers LIMIT ${rowCount}`);
		
		let i     = 0,
			error = null;
		
		const stream = $q.stream()
			.on('data', () => {
				++i;
				
				if (i > minRnd) {
					stream.pause();
				}
			})
			.on('error', err => error = err)
			.on('close', () => {
				expect(error).to.not.be.ok();
				expect(i).to.be.below(rowCount);
				
				cb();
			})
			.on('end', () => {
				cb(new Error('no way!'));
			});
		
		setTimeout(() => $q.destroy(), 10 * 1000);
	});
});


after(async () => {
	await clickhouse.query(`DROP DATABASE IF EXISTS ${database}`).toPromise();
});
