import {Client, ClientConfig} from "pg";

const INITIALIZE = "CREATE TABLE IF NOT EXISTS _ad_objects (identifier BIGSERIAL NOT NULL PRIMARY KEY, parent_object_identifier BIGINT, data JSONB); CREATE TABLE IF NOT EXISTS _ad_tables (identifier BIGSERIAL NOT NULL, sub_tables JSONB NOT NULL); CREATE TABLE IF NOT EXISTS _ad_table_refs (identifier BIGINT NOT NULL, key VARCHAR NOT NULL, object_identifier BIGINT NOT NULL);";

const DATABASE_GET = "SELECT data :: TEXT FROM _ad_objects WHERE identifier = 0";
const DATABASE_RESET = "INSERT INTO _ad_objects VALUES (0, NULL, $1) ON CONFLICT (identifier) DO UPDATE SET data = EXCLUDED.data";

const OBJECT_INIT = "SELECT nextval('_ad_objects_identifier_seq') AS identifier";
const OBJECT_SET = "INSERT INTO _ad_objects VALUES ($1, $2, $3 :: JSONB)";

const REMOTE_MAP_INIT = "INSERT INTO _ad_tables (sub_tables) VALUES ($1 :: JSONB) RETURNING identifier";
const REMOTE_MAP_SCHEMA_GET = "SELECT sub_tables :: TEXT FROM _ad_tables WHERE identifier = $1"
const REMOTE_MAP_SCHEMA_SET = "UPDATE _ad_tables SET sub_tables = $2 :: JSONB WHERE identifier = $1"
const REMOTE_MAP_GET = "SELECT _ad_objects.identifier, _ad_objects.data :: TEXT FROM _ad_table_refs INNER JOIN _ad_objects ON _ad_objects.identifier = _ad_table_refs.object_identifier WHERE _ad_table_refs.identifier = $1 AND _ad_table_refs.key = $2 AND _ad_objects.parent_object_identifier = $3";
const REMOTE_MAP_RESET = "INSERT INTO _ad_table_refs VALUES ($1, $2, $3)"; // TODO: This should actually... reset.
const REMOTE_MAP_QUERY = "SELECT _ad_objects.identifier, _ad_objects.data :: TEXT FROM _ad_table_refs INNER JOIN _ad_objects ON _ad_objects.identifier = _ad_table_refs.object_identifier WHERE _ad_table_refs.identifier = $1";

type RemoteData = RemoteMap<RemoteData> | RemoteJSON | string | number;
type RemoteJSON = {[key: string]: RemoteData};

type Query<T extends RemoteData> =
	T extends RemoteMap<any> ? QueryState<T> :
	T extends {[key: string]: RemoteData} ? (QueryState<T> & {[K in keyof T]: Query<T[K]>}) :
	QueryState<T>;
type QueryDescription = (string | QueryState<any>)[];

type JSON = {[key: string]: JSON} | number;

async function prepareSet<T extends RemoteData>(
		value: T, schema: JSON, objectIdentifier: string): Promise<[string, JSON]> {
	async function prepare(value: any, schema: any): Promise<any> {
		if (value instanceof RemoteMap) {
			let tableIdentifier: string | null;
			if (typeof schema == "string") {
				tableIdentifier = schema;
			} else if (schema != null) {
				throw new Error("cannot assign a RemoteMap where data is");
			}

			return await value.onSet(objectIdentifier, tableIdentifier);
		} else if (typeof value == "object") {
			if (typeof schema == "string")
				throw new Error("cannot assign data where a RemoteMap is");
			if (typeof schema != "object" || schema == null)
				schema = {};

			const promises = Object.keys(value)
				.map(async key => {
					schema[key] = await prepare(value[key as any], schema[key]);
				});

			await Promise.all(promises);
			return schema;
		} else return schema;
	}

	function replace(key: string, value: any) {
		if (value instanceof RemoteMap) return undefined;
		else return value;
	}

	const newSchema = await prepare(value, schema);
	return [JSON.stringify(value), newSchema];
}

function fromJSON<T extends RemoteData>(
		database: Database<any>, value: string, identifier: string): Promise<T> {
	return JSON.parse(value, (_, value) => {
		if (typeof value == "object" && "?_table" in value)
			return new RemoteMap(database, value["?_table"], identifier);
		else
			return value;
	});
}

class Database<T extends RemoteData> {
	readonly client: Client;
	readonly initialize: Promise<void>;

	constructor(client: Client);
	constructor(config: string | ClientConfig);
	constructor(config: string | ClientConfig | Client) {
		this.client = config instanceof Client ? config : new Client(config);
		this.initialize = this.makeInitialize();
	}

	private async makeInitialize() {
		try {this.client.connect();} catch {}
		await this.client.query(INITIALIZE);
	}

	async get(): Promise<T> {
		await this.initialize;
		const data = (await this.client.query(DATABASE_GET)).rows[0].data;
		return fromJSON(this, data, "0");
	}

	async set(value: T) {
		await this.initialize;
		const [object,] = await prepareSet(value, null, "0");
		await this.client.query(DATABASE_RESET, [object]);
	}
}

/*
{
	"guilds": {
		x => {
			"entitlements": {
				y => {
					"name": "wow"
				}
			}
		}
	}
}
*/

export class RemoteMap<T extends RemoteData> {
	readonly database: Database<any>;
	identifier: string | null;
	private parentObjectIdentifier: string | [string, any][]; // any is T.
	private schema: JSON;

	constructor(database: Database<any>, identifier?: string,
			parentObjectIdentifier?: string) {
		this.database = database;
		this.schema = null;
		if (identifier != null && parentObjectIdentifier != null) {
			this.identifier = identifier;
			this.parentObjectIdentifier = parentObjectIdentifier;
		} else if (identifier == null && parentObjectIdentifier == null) {
			this.identifier = null;
			this.parentObjectIdentifier = [];
		} else throw new Error("bad constructor use");
	}

	async get(key: string): Promise<T> {
		await this.database.initialize;

		if (this.parentObjectIdentifier instanceof Array) {
			return this.parentObjectIdentifier.find(([index, _]) => index == key)[1];
		} else {
			const args = [this.identifier, key, this.parentObjectIdentifier];
			const data = await this.database.client.query(REMOTE_MAP_GET, args);
			if (data.rows[0] == null) return null;
			return await fromJSON(this.database,
				data.rows[0].data, data.rows[0].identifier);
		}
	}

	async set(key: string, value: T) {
		await this.database.initialize;

		if (this.parentObjectIdentifier instanceof Array) {
			this.parentObjectIdentifier.push([key, value]);
		} else {
			// TODO: Care to reuse or delete old objects?

			const identifierRow = await this.database.client.query(OBJECT_INIT);
			const identifier = identifierRow.rows[0].identifier;

			const [data, schema] = await prepareSet(value, this.schema, identifier);
			this.schema = schema;
			let args = [identifier, this.parentObjectIdentifier, data];
			await this.database.client.query(OBJECT_SET, args);

			args = [this.identifier, key, identifier];
			await this.database.client.query(REMOTE_MAP_RESET, args);

			args = [this.identifier, JSON.stringify(this.schema)];
			await this.database.client.query(REMOTE_MAP_SCHEMA_SET, args);
		}
	}

	async query<R extends RemoteData>(
			queryBuilder: (data: Query<T>) => QueryState<R>): Promise<R[]> {
		const processQuery = async ([indexes, table]: [QueryDescription, string]):
				Promise<[string[], string]> => {
			const lastIndex = indexes[indexes.length - 1];
			if (lastIndex instanceof QueryState) {
				let [lastIndexes, lastTable] = QueryState.getDescription(lastIndex);
				if (lastTable == null) {
					const subTablesRow =
						await this.database.client.query(REMOTE_MAP_SCHEMA_GET, [table]);
					const subTables = JSON.parse(subTablesRow.rows[0].sub_tables);
					lastTable = indexes.reduce((data: any, index) => {
						if (index instanceof QueryState) return data;
						return data[index];
					}, subTables);
				}
				return await processQuery([lastIndexes, lastTable]);
			} else return [indexes as string[], table];
		};

		await this.database.initialize;

		const facade = QueryState.create<T>(this.identifier);
		const query = QueryState.getDescription(queryBuilder(facade));
		const [indexes, table] = await processQuery(query);

		const data = await this.database.client.query(REMOTE_MAP_QUERY, [table])
		const promises = data.rows.map(async row => {
			const data = await fromJSON(this.database, row.data, row.identifier);
			return indexes.reduce((data, key) => data[key as any], data as any) as R;
		});
		return await Promise.all(promises);
	}

	async onSet(parentObjectIdentifier: string, identifier?: string | null):
			Promise<string> {
		if (identifier == null) {
			const identifier =
				await this.database.client.query(REMOTE_MAP_INIT, ["null"]);
			this.identifier = identifier.rows[0].identifier;
		} else this.identifier = identifier;

		const data = this.parentObjectIdentifier;
		this.parentObjectIdentifier = parentObjectIdentifier;
		if (data instanceof Array)
			await Promise.all(data.map(entry => this.set(...entry)));
		return this.identifier;
	}

	toJSON() {
		if (this.identifier == null) throw new TypeError("expected non null id");
		return {"?_table": this.identifier};
	}
}

class QueryState<T extends RemoteData> {
	static create<T extends RemoteData>(id: string | null): Query<T> {
		return new QueryState(id, []) as Query<T>;
	}

	static getDescription<T extends RemoteData>(
			query: QueryState<T>): [QueryDescription, string] {
		return [query.built, query.tableID];
	}

	static proxy: ProxyHandler<QueryState<any>> = {
		get<T extends RemoteJSON, P extends keyof T>(
				state: QueryState<T>, key: P, _: Query<T>): Query<T[P]> {
			// @ts-ignore in is a perfectly valid guard.
			if (key in state) return state[key];

			const newState = new QueryState(state.tableID, [...state.built, key.toString()]);
			return newState as Query<T[P]>;
		}
	};

	private tableID: string | null;
	private built: QueryDescription;
	
	private constructor(tableID: string | null, query: QueryDescription) {
		this.tableID = tableID;
		this.built = query;
		return new Proxy(this, QueryState.proxy);
	}

	query?: T extends RemoteMap<infer P> ?
		<R extends RemoteData>(
			this: QueryState<RemoteMap<P>>,
			queryBuilder: (data: Query<P>) => QueryState<R>
		) => Query<R> :
		void =
	function<P extends RemoteData, R extends RemoteData>(
		this: QueryState<RemoteMap<P>>,
		queryBuilder: (data: Query<P>) => QueryState<R>
	): Query<R> {
		const query = queryBuilder(QueryState.create(null));
		return new QueryState(this.tableID, [...this.built, query]) as Query<R>;
	} as any;
}

export async function test() {
	type DatabaseRoot = {
		guilds: RemoteMap<DatabaseGuild>
	};

	type DatabaseGuild = {
		entitlements: RemoteMap<DatabaseEntitlement>,
		other: number
	};

	type DatabaseEntitlement = {
		name: string
	};

	const database = new Database<DatabaseRoot>("postgres://danii@localhost/danii");

	await database.set({guilds: new RemoteMap(database)});
	const root = await database.get();

	await root.guilds.set("0", {entitlements: new RemoteMap(database), other: 4});
	const guild1 = await root.guilds.get("0");

	guild1.entitlements.set("0", {name: "Guild 0's entitlement."});

	const entitlements2 = new RemoteMap<DatabaseEntitlement>(database);
	await entitlements2.set("0", {name: "Guild 1's entitlement."});
	await root.guilds.set("1", {entitlements: entitlements2, other: 9});

	const allEntitlements = await root.guilds.query<string>(guild => guild.entitlements.query(entitlement => entitlement.name));
	console.log(allEntitlements);

	await database.client.end();
	console.log("ended");

	//const j = (await db.get());
	//const d = j.guild.query(guild => guild.entitlements.query(entitlement => entitlement.name));
}

test();
