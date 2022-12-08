import { Plugin, PluginMeta, PluginEvent } from '@posthog/plugin-scaffold'
import { Client } from 'pg'

type PostgresPlugin = Plugin<{
    global: {
        pgClient: Client
        eventsToIgnore: Set<string>
        sanitizedTableName: string
    }
    config: {
        databaseUrl: string
        host: string
        port: string
        dbName: string
        tableName: string
        dbUsername: string
        dbPassword: string
        eventsToIgnore: string
        hasSelfSignedCert: 'Yes' | 'No'
    }
}>

type PostgresMeta = PluginMeta<PostgresPlugin>

interface ParsedEvent {
    uuid?: string
    eventName: string
    properties: string
    elements: string
    set: string
    set_once: string
    distinct_id: string
    team_id: number
    ip: string | null
    site_url: string
    timestamp: string
}

interface UploadJobPayload {
    batch: ParsedEvent[]
    batchId: number
    retriesPerformedSoFar: number
}

const randomBytes = (): string => {
    return (((1 + Math.random()) * 0x10000) | 0).toString(16).substring(1)
}

const generateUuid = (): string => {
    return (
        randomBytes() +
        randomBytes() +
        '-' +
        randomBytes() +
        '-3' +
        randomBytes().substr(0, 2) +
        '-' +
        randomBytes() +
        '-' +
        randomBytes() +
        randomBytes() +
        randomBytes()
    ).toLowerCase()
}

export const jobs: PostgresPlugin['jobs'] = {
    uploadBatchToPostgres: async (payload: UploadJobPayload, meta: PostgresMeta) => {
        await insertBatchIntoPostgres(payload, meta)
    },
}

export const setupPlugin: PostgresPlugin['setupPlugin'] = async (meta) => {
    console.log('Setting up in progress 1')
    const { global, config } = meta

    console.log(meta)
    if (!config.databaseUrl) {
        const requiredConfigOptions = ['host', 'port', 'dbName', 'dbUsername', 'dbPassword']
        for (const option of requiredConfigOptions) {
            if (!(option in config)) {
                throw new Error(`Required config option ${option} is missing!`)
            }
        }
    }

    global.sanitizedTableName = sanitizeSqlIdentifier(config.tableName)
    console.log('Setting up in progress 2')
    const queryError = await executeQuery(
        `CREATE TABLE IF NOT EXISTS public.${global.sanitizedTableName} (
            uuid varchar(200),
            event varchar(200),
            properties jsonb,
            elements jsonb,
            set jsonb,
            set_once jsonb,
            timestamp timestamp with time zone,
            team_id int,
            distinct_id varchar(200),
            ip varchar(200),
            site_url varchar(200),
            os varchar(200),
            city varchar(200),
            timezone varchar(200),
            country_code varchar(200),
            country varchar(200),
            browser varchar(200),
            browser_version varchar(200),
            device varchar(200),
            device_id varchar(200),
            pathname varchar(200),
            referrer varchar(500),
            referring_domain varchar(500),
            initial_pathname varchar(200),
            initial_referrer varchar(500),
            initial_device varchar(200),
            initial_browser_version varchar(200),
            initial_city varchar(200),
            initial_timezone varchar(200),
            initial_country_code varchar(200),
            initial_country varchar(200),
            initial_referring_domain varchar(500),
            initial_os varchar(200),
            session_id varchar(200),
            url varchar(500),
            screen_height int,
            screen_width int,
            viewport_height int,
            viewport_width int,
            search_engine varchar(200),
            event varchar(200),
            selected_plan varchar(200),
            image_key varchar(200),
            position varchar(200),
            button varchar(200),
            screen varchar(200),
            billing_cycle varchar(200),
            position varchar(200)
        );`,
        [],
        config
    )
    console.log('Setting up in progress 3')
    if (queryError) {
        throw new Error(`Unable to connect to PostgreSQL instance and create table with error: ${queryError.message}`)
    }

    global.eventsToIgnore = new Set(
        config.eventsToIgnore ? config.eventsToIgnore.split(',').map((event) => event.trim()) : null
    )
    console.log('Setting up in progress 4')
}

export async function exportEvents(events: PluginEvent[], { global, jobs }: PostgresMeta) {
    const batch: ParsedEvent[] = []
    for (const event of events) {
        const {
            event: eventName,
            properties,
            $set,
            $set_once,
            distinct_id,
            team_id,
            site_url,
            now,
            sent_at,
            uuid,
            ..._discard
        } = event

        if (global.eventsToIgnore.has(eventName)) {
            continue
        }

        const ip = properties?.['$ip'] || event.ip
        const timestamp = event.timestamp || properties?.timestamp || now || sent_at
        let ingestedProperties = properties
        let elements = []

        // only move prop to elements for the $autocapture action
        if (eventName === '$autocapture' && properties && '$elements' in properties) {
            const { $elements, ...props } = properties
            ingestedProperties = props
            elements = $elements
        }

        const parsedEvent: ParsedEvent = {
            uuid,
            eventName,
            properties: JSON.stringify(ingestedProperties || {}),
            elements: JSON.stringify(elements || {}),
            set: JSON.stringify($set || {}),
            set_once: JSON.stringify($set_once || {}),
            distinct_id,
            team_id,
            ip,
            site_url,
            timestamp: new Date(timestamp).toISOString(),
        }

        batch.push(parsedEvent)
    }

    if (batch.length > 0) {
        await jobs
            .uploadBatchToPostgres({ batch, batchId: Math.floor(Math.random() * 1000000), retriesPerformedSoFar: 0 })
            .runNow()
    }
}

export const insertBatchIntoPostgres = async (payload: UploadJobPayload, { global, jobs, config }: PostgresMeta) => {
    let values: any[] = []
    let valuesString = ''

    for (let i = 0; i < payload.batch.length; ++i) {
        const { uuid, eventName, properties, elements, set, set_once, distinct_id, team_id, ip, site_url, timestamp } =
            payload.batch[i]


        // Creates format: ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11), ($12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
        valuesString += ' ('
        for (let j = 1; j <= 11; ++j) {
            valuesString += `$${11 * i + j}${j === 11 ? '' : ', '}`
        }
        valuesString += `)${i === payload.batch.length - 1 ? '' : ','}`
        
        values = values.concat([
            uuid || generateUuid(),
            eventName,
            properties,
            elements,
            set,
            set_once,
            distinct_id,
            team_id,
            ip,
            site_url,
            timestamp,
        ])
    }

    console.log(
        `(Batch Id: ${payload.batchId}) Flushing ${payload.batch.length} event${
            payload.batch.length > 1 ? 's' : ''
        } to Postgres instance`
    )

    const queryError = await executeQuery(
        `INSERT INTO ${global.sanitizedTableName} (uuid, event, properties, elements, set, set_once, distinct_id, team_id, ip, site_url, timestamp)
        VALUES ${valuesString}`,
        values,
        config
    )

    if (queryError) {
        console.error(`(Batch Id: ${payload.batchId}) Error uploading to Postgres: ${queryError.message}`)
        if (payload.retriesPerformedSoFar >= 15) {
            return
        }
        const nextRetryMs = 2 ** payload.retriesPerformedSoFar * 3000
        console.log(`Enqueued batch ${payload.batchId} for retry in ${nextRetryMs}ms`)
        await jobs
            .uploadBatchToPostgres({
                ...payload,
                retriesPerformedSoFar: payload.retriesPerformedSoFar + 1,
            })
            .runIn(nextRetryMs, 'milliseconds')
    }
}

const executeQuery = async (query: string, values: any[], config: PostgresMeta['config']): Promise<Error | null> => {
    const basicConnectionOptions = config.databaseUrl
        ? {
              connectionString: config.databaseUrl,
          }
        : {
              user: config.dbUsername,
              password: config.dbPassword,
              host: config.host,
              database: config.dbName,
              port: parseInt(config.port),
          }
    const pgClient = new Client({
        ...basicConnectionOptions,
        ssl: {
            rejectUnauthorized: config.hasSelfSignedCert === 'No',
        },
    })

    await pgClient.connect()

    let error: Error | null = null
    try {
        await pgClient.query(query, values)
    } catch (err) {
        error = err as Error
    }

    await pgClient.end()

    return error
}

const sanitizeSqlIdentifier = (unquotedIdentifier: string): string => {
    return unquotedIdentifier.replace(/[^\w\d_]+/g, '')
}
