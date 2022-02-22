import {Client} from "pg";

type Of<T> = {[key: string]: T | undefined};

export type RemoteData = RemoteMap | JSON;
export type JSON = JSONObject | JSONArray | string | number | boolean | null;

interface RemoteMap extends Map<RemoteData> {}
interface JSONObject extends Of<RemoteData> {}
interface JSONArray extends Array<RemoteData> {}

function requiresInitialized<
	A extends any[],
	T extends {initializePromise: Promise<void>},
	R
>(
	_target: any,
	_name: string,
	descriptor: TypedPropertyDescriptor<(this: T, ...args: A) => Promise<R>>
) {
	const oldFunction = descriptor.value;
	async function newFunction(this: T, ...args: A): Promise<R> {
		await this.initializePromise;
		return await oldFunction.call(this, ...args);
	}
	descriptor.value = newFunction;
}

export class Database<T extends RemoteData> {
	// Implementation detail, do not use.
	client: Client;

	// Implementation detail, do not use.
	initializePromise: Promise<void>;

	public constructor(url: string);
	public constructor(client: Client);
	public constructor(url: Client | string) {
		this.client = typeof url == "string" ? new Client(url) : url;
		this.initializePromise = this.makeInitializePromise();
	}

	private async makeInitializePromise(): Promise<void> {
		try {
			await this.client.connect();
		} catch {}

		await this.client.query(`
			CREATE
				TABLE IF NOT EXISTS _ad_root 
				(
					name VARCHAR NOT NULL PRIMARY KEY,
					data JSON    NOT NULL
				);
		`);
	}

	@requiresInitialized
	public async set(value: T): Promise<void> {
		await this.client.query(
			`INSERT
				INTO _ad_root
				VALUES ('default', $1)
				ON
					CONFLICT (name)
					DO UPDATE SET data = EXCLUDED.data`,
			[JSON.stringify(value)]
		);
	}

	@requiresInitialized
	public async get(): Promise<T> {
		const result = (
			await this.client.query(
				`SELECT
					data :: TEXT
				FROM
					_ad_root
				WHERE
					name = 'default'
				`
			)
		).rows[0];

		const retriever = (key: string, value: any) => {
			if (typeof value == "object" && "?_table" in value) {
				return new Map(this, value["?_table"]);
			} else return value;
		};

		if (result == null) return null;
		else return JSON.parse(result.data, retriever);
	}

	@requiresInitialized
	public async end() {
		this.client.end();
	}
}

export class Map<T extends RemoteData> {
	private table: string;
	private database: Database<any>;

	// Implementation detail, do not use.
	private initializePromise: Promise<void>;

	constructor(database: Database<any>, table: string) {
		if (!/^[\w_]+$/.test(table)) throw new Error("bad table name");

		this.table = table;
		this.database = database;

		this.initializePromise = this.makeInitializePromise();
	}

	private async makeInitializePromise(): Promise<void> {
		await this.database.initializePromise;

		await this.database.client.query(
			`CREATE
				TABLE IF NOT EXISTS ${this.table}
				(
					name VARCHAR NOT NULL PRIMARY KEY,
					data JSON    NOT NULL
				);`
			);
	}

	@requiresInitialized
	public async set(key: string, value: T) {
		if (value == null) {
			await this.database.client.query(
				`DROP
					FROM ${this.table}
					WHERE name = $1
				`,
				[key]
			);
		} else {
			await this.database.client.query(
				`INSERT
					INTO ${this.table}
					VALUES ($1, $2)
					ON
						CONFLICT (name)
						DO UPDATE SET data = EXCLUDED.data`,
				[key, JSON.stringify(value)]
			);
		}
	}

	@requiresInitialized
	public async get(key: string): Promise<T> {
		const result = (
			await this.database.client.query(
				`SELECT
					data :: TEXT
				FROM
					${this.table}
				WHERE
					name = $1
				`,
				[key]
			)
		).rows[0];

		const retriever = (key: string, value: any) => {
			if (typeof value == "object" && "?_table" in value) {
				return new Map(this.database, value["?_table"]);
			} else return value;
		};

		if (result == null) return null;
		else return JSON.parse(result.data, retriever);
	}

	public toJSON(): any {
		return {"?_table": this.table};
	}
}
